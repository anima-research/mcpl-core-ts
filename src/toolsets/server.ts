/**
 * The wire side of an RFC-014 revision 2 server: initialize and policy, the full tool list with
 * hints, toolsets/open|close|list, calls routed to static tools or toolset instances, hint
 * changes (in results, on pushes, standalone), and events.
 *
 * Instances belong to a principal, not a connection: a reconnect finds them, and
 * toolsets/list reports them.
 */
import { AsyncLocalStorage } from 'node:async_hooks';
import { randomBytes } from 'node:crypto';
import type { McplConnection } from '../connection.js';
import { RpcError } from '../errors.js';
import { method } from '../methods.js';
import type { FeatureSetDeclaration, FeatureSetsUpdateAccepted, FeatureSetsUpdateParams, UnavailableFeature } from '../methods.js';
import type { CapabilityGrantState, CapabilityPath, McplInitializeParams } from '../capabilities.js';
import { deriveFeatureSets, emptyGrantState, featureSetSelected, grantFromUpdate } from '../capabilities.js';
import type { ContentBlock, JsonRpcId } from '../types.js';
import { digest, validateJson } from './schema.js';
import { AccessError } from './types.js';
import type {
  AccessCredential, AccessOptions, CallOutcome, HintChanges, InstanceContext, InstanceEvent, InstanceHandler,
  StaticTool, ToolDef, ToolsetServerSpec, ToolsetSpec,
} from './types.js';

type Log = (...a: unknown[]) => void;
const INSTANCE_NAME = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$/;

class RpcFail extends Error {
  constructor(readonly code: number, message: string, readonly data?: unknown) { super(message); }
}

/** FIFO lock; preempt runs now and makes later work wait for it too. */
class Lock {
  private tail: Promise<void> = Promise.resolve();
  run<T>(fn: () => Promise<T>): Promise<T> {
    const r = this.tail.then(fn, fn);
    this.tail = r.then(() => undefined, () => undefined);
    return r;
  }
  preempt<T>(fn: () => Promise<T>): Promise<T> {
    const r = fn();
    this.tail = Promise.all([this.tail, r.then(() => undefined, () => undefined)]).then(() => undefined);
    return r;
  }
}

/** The call an instance's emit belongs to (rule: a call reports its own consequences). */
interface CallScope { instance: Instance; buffer: InstanceEvent[]; closeRequested: string | null; done: boolean }
const callScope = new AsyncLocalStorage<CallScope>();

function txt(t: string): ContentBlock { return { type: 'text', text: t }; }

// ── Instances and owners ─────────────────────────────────────────────────────

class Instance {
  readonly key: string;
  handler!: InstanceHandler;
  open = true;
  readonly lock = new Lock();
  private changeTimer: NodeJS.Timeout | null = null;

  constructor(readonly owner: Owner, readonly toolset: string, readonly spec: ToolsetSpec, readonly name: string | undefined) {
    this.key = `${toolset}\u0000${name ?? ''}`;
  }

  get server(): ToolsetServer { return this.owner.server; }

  visibleNow(): Set<string> {
    if (!this.open || !this.handler) return new Set();
    const names = new Set(this.spec.tools.map((t) => t.name));
    try { return new Set(this.handler.visible().filter((n) => names.has(n))); } catch (e) { this.server.log(`[${this.label()}] visible() failed:`, (e as Error).message); return new Set(); }
  }

  definition(tool: string): ToolDef {
    const base = this.spec.tools.find((t) => t.name === tool)!;
    try { return this.handler?.definition?.(tool) ?? base; } catch { return base; }
  }

  label(): string { return this.name ? `${this.toolset}:${this.name}` : this.toolset; }

  private scope(): CallScope | undefined {
    const sc = callScope.getStore();
    return sc && sc.instance === this && !sc.done ? sc : undefined;
  }

  context(): InstanceContext {
    const self = this;
    return {
      toolset: this.toolset, instance: this.name, principal: this.owner.principal,
      log: (...a) => self.server.log(`[${self.owner.principal}/${self.label()}]`, ...a),
      emit(ev: InstanceEvent) {
        if (!self.open) return;
        const sc = self.scope();
        if (sc) { sc.buffer.push(ev); return; }
        self.owner.event(self, ev);
      },
      changed() {
        if (!self.open || self.scope()) return; // inside a call, the result reports it
        if (self.changeTimer) return;
        self.changeTimer = setTimeout(() => { self.changeTimer = null; self.owner.hintsChanged(); }, 25);
      },
      suggest(s) { self.owner.suggest(s); },
      access(audience, opts) { return self.owner.access(audience, { ...opts, purpose: opts?.purpose ?? `${self.server.spec.name} ${self.label()}` }); },
      close(reason: string) {
        const sc = self.scope();
        if (sc) { sc.closeRequested = reason; return; }
        void self.lock.run(async () => { await self.server.closeInstance(self, reason, 'self'); });
      },
    };
  }
}

/** Everything one principal has open, and the connections it is reachable on. */
class Owner {
  readonly instances = new Map<string, Instance>();
  readonly sessions = new Set<Session>();
  private counters = new Map<string, number>();
  private backlog: Array<{ inst: Instance; ev: InstanceEvent }> = [];
  /** Instances that ended while no connection could be told (RFC-014 §5.3 `ended`), kept an hour. */
  unseenEnds: Array<{ id: number; at: number; toolset: string; instance?: string; reason: string }> = [];
  private endSeq = 0;
  constructor(readonly server: ToolsetServer, readonly principal: string) {}

  /** An instance ended on its own: tell live connections, or keep it for the next toolsets/list. */
  ended(inst: Instance, reason: string): void {
    if ([...this.sessions].some((s) => s.canPush())) {
      this.event(inst, { kind: 'activity', text: `${inst.handler?.title ?? inst.label()} closed: ${reason}` });
    } else {
      this.unseenEnds.push({ id: ++this.endSeq, at: Date.now(), toolset: inst.toolset, ...(inst.name ? { instance: inst.name } : {}), reason });
      if (this.unseenEnds.length > 20) this.unseenEnds.shift();
    }
  }

  nextName(toolset: string): string {
    const n = (this.counters.get(toolset) ?? 0) + 1;
    this.counters.set(toolset, n);
    return `${toolset}-${n}`;
  }

  // ── Delegated access (RFC-015) ──
  private credentials = new Map<string, AccessCredential>();
  private inflight = new Map<string, Promise<AccessCredential>>();

  /** A credential for `audience` from the newest connection that can delegate; cached per audience. */
  async access(audience: string, opts: AccessOptions = {}): Promise<AccessCredential> {
    const sessions = [...this.sessions];
    const provider = sessions.reverse().find((s) => s.accessBlocked() === null);
    if (!provider) {
      this.credentials.clear(); // nothing may delegate any more: drop what was delegated (§4.5)
      throw new AccessError(audience, [...this.sessions].at(-1)?.accessBlocked() ?? 'no connection from the host is open');
    }
    const cached = this.credentials.get(audience);
    if (!opts.fresh && cached && cached.expiresAt - Date.now() > 60_000) return cached;
    const pending = opts.fresh ? undefined : this.inflight.get(audience);
    if (pending) return pending;
    const p = provider.requestAccess(audience, opts).then((c) => {
      if (c.expiresAt <= Date.now()) throw new AccessError(audience, 'the host returned an expired credential');
      this.credentials.set(audience, c);
      return c;
    }).finally(() => { if (this.inflight.get(audience) === p) this.inflight.delete(audience); });
    this.inflight.set(audience, p);
    return p;
  }

  attach(s: Session): void {
    this.sessions.add(s);
    for (const { inst, ev } of this.backlog.splice(0)) s.push(inst, ev);
  }

  detach(s: Session): void { this.sessions.delete(s); }

  event(inst: Instance, ev: InstanceEvent): void {
    const live = [...this.sessions].filter((s) => s.canPush());
    if (!live.length) {
      if (ev.kind === 'state') this.backlog = this.backlog.filter((b) => !(b.inst === inst && b.ev.kind === 'state' && b.ev.key === ev.key));
      this.backlog.push({ inst, ev });
      if (this.backlog.length > 100) this.backlog.splice(0, this.backlog.length - 100);
      return;
    }
    for (const s of live) s.push(inst, ev);
  }

  hintsChanged(except?: Session): void {
    for (const s of this.sessions) if (s !== except) s.notifyHints();
  }

  suggest(sg: { toolset: string; instance?: string; action: 'open' | 'close'; reason: string }): void {
    for (const s of this.sessions) s.notifyHints([sg]);
  }
}

// ── The server ───────────────────────────────────────────────────────────────

export class ToolsetServer {
  readonly spec: ToolsetServerSpec;
  readonly log: Log;
  readonly owners = new Map<string, Owner>();
  /** tool name → where it lives */
  private readonly index = new Map<string, { kind: 'static'; tool: StaticTool } | { kind: 'toolset'; toolset: string; spec: ToolsetSpec; tool: ToolDef }>();

  constructor(spec: ToolsetServerSpec) {
    this.spec = spec;
    this.log = spec.log ?? (() => {});
    for (const st of spec.staticTools ?? []) this.addIndex(st.tool.name, { kind: 'static', tool: st });
    for (const [name, ts] of Object.entries(spec.toolsets)) {
      if (!/^[a-z][a-z0-9_-]{0,31}$/.test(name)) throw new Error(`toolset name "${name}" is not allowed`);
      if (ts.default === 'open' && ts.instances) throw new Error(`toolset "${name}": default "open" cannot be combined with instances`);
      for (const t of ts.tools) this.addIndex(t.name, { kind: 'toolset', toolset: name, spec: ts, tool: t });
    }
  }

  private addIndex(name: string, v: { kind: 'static'; tool: StaticTool } | { kind: 'toolset'; toolset: string; spec: ToolsetSpec; tool: ToolDef }): void {
    if (this.index.has(name)) throw new Error(`tool "${name}" is defined twice; tool names are unique across a server`);
    this.index.set(name, v);
  }

  where(name: string) { return this.index.get(name); }

  /** Validate arguments with the spec's validator, or the built-in one. */
  validate(schema: Record<string, unknown>, value: unknown): string | null {
    return (this.spec.validate ?? validateJson)(schema, value);
  }

  owner(principal: string): Owner {
    let o = this.owners.get(principal);
    if (!o) { o = new Owner(this, principal); this.owners.set(principal, o); }
    return o;
  }

  /** Serve one connection for a principal until it closes. */
  async serve(conn: McplConnection, principal: string): Promise<void> {
    await new Session(this, conn, this.owner(principal)).serve();
  }

  /** The manifest's MCPL block (RFC-014 §5.1). */
  manifest(): Record<string, unknown> {
    const toolsets: Record<string, unknown> = {};
    for (const [name, ts] of Object.entries(this.spec.toolsets)) {
      toolsets[name] = { title: ts.title, summary: ts.summary, default: ts.default, ...(ts.instances ? { instances: true } : {}), ...(ts.options ? { options: ts.options } : {}) };
    }
    return { toolHints: true, toolsets, ...(this.spec.surface ? { surface: this.spec.surface } : {}) };
  }

  /** Every tool, hinted (RFC-014 §4.1). Visibility in the list is "visible in some open instance". */
  toolList(owner: Owner, compat: boolean): ToolDef[] {
    const out: ToolDef[] = [];
    for (const st of this.spec.staticTools ?? []) out.push({ ...st.tool, _meta: { ...st.tool._meta, 'mcpl/presence': 'static' } });
    for (const [name, ts] of Object.entries(this.spec.toolsets)) {
      const open = [...owner.instances.values()].filter((i) => i.toolset === name && i.open);
      for (const t of ts.tools) {
        const visible = open.some((i) => i.visibleNow().has(t.name));
        out.push({ ...t, _meta: { ...t._meta, 'mcpl/presence': 'dynamic', 'mcpl/toolset': name, 'mcpl/visible': visible } });
      }
    }
    if (compat) out.push(...compatTools(this.spec));
    return out;
  }

  async openInstance(owner: Owner, toolset: string, proposed: string | undefined, options: Record<string, unknown>): Promise<{ inst: Instance; content: ContentBlock[]; adopted: boolean }> {
    const ts = this.spec.toolsets[toolset];
    if (!ts) throw new RpcFail(-32602, `no toolset "${toolset}"`, { toolset });
    let name: string | undefined;
    if (ts.instances) {
      if (proposed !== undefined && !INSTANCE_NAME.test(proposed)) throw new RpcFail(-32602, `instance names are letters, digits, _ . - (up to 64)`, { field: 'instance' });
      name = proposed ?? owner.nextName(toolset);
    } else if (proposed !== undefined) {
      throw new RpcFail(-32602, `toolset "${toolset}" has no instances`, { field: 'instance' });
    }
    const key = `${toolset}\u0000${name ?? ''}`;
    const existing = owner.instances.get(key);
    if (existing?.open) return { inst: existing, content: await contentOf(existing), adopted: true };
    const opts = { ...options };
    if (ts.options) {
      const bad = this.validate(ts.options, opts);
      if (bad) throw new RpcFail(-32602, `invalid options: ${bad}`, { field: 'options' });
    }
    const inst = new Instance(owner, toolset, ts, name);
    const sc: CallScope = { instance: inst, buffer: [], closeRequested: null, done: false };
    try {
      inst.handler = await callScope.run(sc, () => Promise.resolve(ts.open(inst.context(), opts)));
    } catch (e) {
      sc.done = true;
      throw new RpcFail(-32000, `could not open ${toolset}: ${(e as Error).message}`, { toolset });
    }
    sc.done = true;
    owner.instances.set(key, inst);
    if (sc.closeRequested !== null) {
      await this.closeInstance(inst, sc.closeRequested, 'self');
      throw new RpcFail(-32000, `${toolset} opened and closed at once: ${sc.closeRequested}`, { toolset });
    }
    const content: ContentBlock[] = [];
    for (const ev of sc.buffer) content.push(txt(ev.text), ...(ev.content ?? []));
    content.push(...await contentOf(inst));
    return { inst, content, adopted: false };
  }

  /** Close an instance. `by`: who asked — the host (a toolsets/close), or the instance itself. */
  async closeInstance(inst: Instance, reason: string, by: 'host' | 'self'): Promise<string | void> {
    if (!inst.open) return;
    let said: string | void = undefined;
    try { said = await inst.handler?.close(reason); } catch (e) { this.log(`[${inst.label()}] close failed:`, (e as Error).message); }
    inst.open = false;
    inst.owner.instances.delete(inst.key);
    if (by === 'self') inst.owner.ended(inst, reason);
    return said;
  }

  async callTool(owner: Owner, name: string, args: Record<string, unknown>, instanceName: string | undefined, session: Session): Promise<{ result: CallOutcome; touched: Instance[] }> {
    const where = this.index.get(name);
    if (!where) return { result: fail(`No tool "${name}".`), touched: [] };
    if (where.kind === 'static') {
      const r = await where.tool.call(args, { principal: owner.principal });
      return { result: r, touched: [] };
    }
    const { toolset, spec } = where;
    const open = [...owner.instances.values()].filter((i) => i.toolset === toolset && i.open);
    let inst: Instance | undefined;
    if (instanceName !== undefined) inst = open.find((i) => i.name === instanceName);
    else if (open.length === 1) inst = open[0];
    if (!inst) {
      if (!open.length) return { result: fail(`"${name}" belongs to ${toolset}, which is not open.${session.compat ? ` Open it with toolset_open {toolset:"${toolset}"}.` : ''}`), touched: [] };
      if (instanceName !== undefined) return { result: fail(`No open instance "${instanceName}" of ${toolset}; open: ${open.map((i) => i.name).join(', ')}.`), touched: [] };
      return { result: fail(`Several ${toolset} instances are open (${open.map((i) => i.name).join(', ')}); say which with _meta["mcpl/instance"].`), touched: [] };
    }
    const target = inst;
    if (!target.visibleNow().has(name)) {
      const now = [...target.visibleNow()];
      return { result: fail(`"${name}" is not available right now${target.name ? ` in ${target.name}` : ''}. Available: ${now.join(', ') || '(nothing)'}.`), touched: [target] };
    }
    const def = target.definition(name);
    const a = { ...args };
    const bad = this.validate(def.inputSchema, a);
    if (bad) return { result: fail(`Invalid arguments for ${name}: ${bad}. Nothing was done.`), touched: [] };
    const run = async (): Promise<CallOutcome> => {
      if (!target.open) return fail(`${target.label()} closed.`);
      const sc: CallScope = { instance: target, buffer: [], closeRequested: null, done: false };
      let r: CallOutcome;
      try {
        r = await callScope.run(sc, () => target.handler.call(name, a));
      } catch (e) {
        r = fail(`${name} failed: ${(e as Error).message}`);
      } finally {
        sc.done = true;
      }
      const content = [...r.content];
      for (const ev of sc.buffer) {
        content.push(txt(ev.text), ...(ev.content ?? []));
        if (ev.includeContent) content.push(...await contentOf(target));
      }
      if (sc.closeRequested !== null) {
        const said = await this.closeInstance(target, sc.closeRequested, 'host');
        content.push(txt(said ? String(said) : `${target.handler.title} closed: ${sc.closeRequested}`));
      }
      return { content: mergeText(content), isError: r.isError };
    };
    const result = def._meta?.['mcpl/preempt'] === true ? await target.lock.preempt(run) : await target.lock.run(run);
    void spec;
    return { result, touched: [target] };
  }
}

async function contentOf(inst: Instance): Promise<ContentBlock[]> {
  try { return (await inst.handler.content?.()) ?? []; } catch (e) { return [txt(`(state unavailable: ${(e as Error).message})`)]; }
}

function fail(t: string): CallOutcome { return { content: [txt(t)], isError: true }; }

export function mergeText(blocks: ContentBlock[]): ContentBlock[] {
  const out: ContentBlock[] = [];
  for (const b of blocks) {
    if (b.type === 'text' && !b.text) continue;
    const prev = out[out.length - 1];
    if (b.type === 'text' && prev?.type === 'text') out[out.length - 1] = txt(`${prev.text}\n${b.text}`);
    else out.push(b);
  }
  return out;
}

/**
 * For a host that does not speak RFC-014: two plain tools that open and close toolsets, so the
 * server is still usable (its toolset tools are in the list; calls go to the only open instance).
 */
function compatTools(spec: ToolsetServerSpec): ToolDef[] {
  const names = Object.keys(spec.toolsets);
  if (!names.length) return [];
  return [
    {
      name: 'toolset_open',
      description: `Open a toolset: ${Object.entries(spec.toolsets).map(([n, t]) => `${n} (${t.summary})`).join('; ')}. Its tools then work.`,
      inputSchema: { type: 'object', properties: { toolset: { enum: names }, instance: { type: 'string' }, options: { type: 'object' } }, required: ['toolset'], additionalProperties: false },
      _meta: { 'mcpl/presence': 'static' },
    },
    {
      name: 'toolset_close',
      description: 'Close an open toolset.',
      inputSchema: { type: 'object', properties: { toolset: { enum: names }, instance: { type: 'string' } }, required: ['toolset'], additionalProperties: false },
      _meta: { 'mcpl/presence': 'static' },
    },
  ];
}

// ── One connection ───────────────────────────────────────────────────────────

interface Reported { visible: Set<string>; defs: Map<string, string> }
/** What a connection was told about a toolset instance being open, and under what title. */
interface ReportedOpen { title: string }

class Session {
  private mcplEnabled = false;
  /** The host advertised RFC-014 toolHints. */
  hints = false;
  private coalesce = false;
  private grant: CapabilityGrantState = emptyGrantState();
  /** The host advertised RFC-015 `access.token` (it can delegate credentials). */
  private hostAccess = false;
  private rev = 0;
  private readonly reported = new Map<string, Reported>();
  private readonly reportedOpen = new Map<string, ReportedOpen>();
  /** `ended` records this connection has already been told (each connection hears each once). */
  private readonly endsTold = new Set<number>();
  private readonly idPrefix = `ev_${randomBytes(4).toString('hex')}`;
  private seq = 0;
  private outbox: Promise<void> = Promise.resolve();
  private readonly fsTools: string;
  private readonly fsEvents: string;
  private readonly featureSets: Record<string, FeatureSetDeclaration>;

  constructor(readonly server: ToolsetServer, readonly conn: McplConnection, readonly owner: Owner) {
    const n = server.spec.name.replace(/[^a-z0-9]+/gi, '-').toLowerCase();
    this.fsTools = `${n}.tools`;
    this.fsEvents = `${n}.events`;
    this.featureSets = {
      [this.fsTools]: { description: 'Tools, toolsets and their hints', uses: ['tools'] },
      [this.fsEvents]: { description: 'Events from open toolsets', uses: ['pushEvents'] },
    };
    conn.on('close', () => owner.detach(this));
    conn.on('error', (e: Error) => server.log('connection error:', e.message));
  }

  /** A host that cannot use hints gets the compat open/close tools. */
  get compat(): boolean { return !this.hints; }

  async serve(): Promise<void> {
    const first = await this.conn.nextMessage().catch(() => null);
    if (!first || first.type !== 'request' || first.request.method !== 'initialize') { this.conn.close(); return; }
    const params = first.request.params as McplInitializeParams | undefined;
    const mcpl = (params?.capabilities?.experimental as Record<string, unknown> | undefined)?.mcpl as Record<string, unknown> | undefined;
    this.mcplEnabled = !!mcpl;
    this.hints = mcpl?.toolHints === true;
    this.hostAccess = (mcpl?.access as { token?: unknown } | undefined)?.token === true;
    const ec = mcpl?.eventCoalescing;
    this.coalesce = ec === true || (typeof ec === 'object' && ec !== null && (ec as Record<string, unknown>).pushEvents !== false);
    const spec = this.server.spec;
    this.conn.sendResponse(first.request.id, {
      protocolVersion: '2024-11-05',
      capabilities: {
        tools: { listChanged: true },
        ...(this.mcplEnabled ? { experimental: { mcpl: {
          version: '0.5', pushEvents: true,
          featureSets: this.featureSets,
          access: { token: true },
          ...this.server.manifest(),
        } } } : {}),
      },
      serverInfo: { name: spec.name, version: spec.version },
      ...(spec.instructions ? { instructions: spec.instructions } : {}),
    });
    this.server.log(`${this.owner.principal}: initialize (${this.mcplEnabled ? 'MCPL' : 'MCP'}, toolHints=${this.hints})`);
    // Default-open toolsets are open from the start of a principal's session (RFC-014 §5.1).
    for (const [name, ts] of Object.entries(spec.toolsets)) {
      if (ts.default !== 'open' || this.owner.instances.has(`${name}\u0000`)) continue;
      try { await this.server.openInstance(this.owner, name, undefined, {}); } catch (e) { this.server.log(`default-open ${name} failed:`, (e as Error).message); }
    }
    if (!this.mcplEnabled) this.owner.sessions.add(this); // reachable for hints-less results; never pushed to
    try {
      while (!this.conn.isClosed) {
        const msg = await this.conn.nextMessage();
        if (msg.type === 'request') void this.request(msg.request.id, msg.request.method, (msg.request.params ?? {}) as Record<string, unknown>);
        else if (msg.notification.method === method.FEATURE_SETS_UPDATE) {
          // §6.7: a Notification can only narrow.
          this.grant = grantFromUpdate(this.grant, (msg.notification.params ?? {}) as FeatureSetsUpdateParams, 'notification').state;
        }
      }
    } catch { /* closed */ } finally {
      this.owner.detach(this);
    }
  }

  canPush(): boolean {
    if (this.conn.isClosed || !this.mcplEnabled || !this.grant.ready) return false;
    return deriveFeatureSets(this.featureSets, this.grant.effectiveCapabilities).enabled.includes(this.fsEvents)
      && featureSetSelected(this.grant, this.fsEvents);
  }

  /** The degradation receipt for the current grant (SPEC §6.7). */
  private receipt(): FeatureSetsUpdateAccepted {
    const d = deriveFeatureSets(this.featureSets, this.grant.effectiveCapabilities);
    const unavailable: UnavailableFeature[] = Object.entries(d.disabled).map(([featureSet, r]) => ({
      featureSet, missingCapabilities: r.missingCapabilities as CapabilityPath[], effect: 'disabled',
    }));
    for (const name of d.enabled) {
      if (!featureSetSelected(this.grant, name)) unavailable.push({ featureSet: name, missingCapabilities: [], effect: 'disabled' });
    }
    return unavailable.length ? { accepted: true, mode: 'degraded', unavailableFeatures: unavailable } : { accepted: true };
  }

  // ── Delegated access (RFC-015) ──

  /** Why this connection cannot ask its host for a credential, or null if it can. */
  accessBlocked(): string | null {
    if (this.conn.isClosed) return 'the connection to the host closed';
    if (!this.mcplEnabled) return 'the host speaks plain MCP and cannot delegate access';
    if (!this.hostAccess) return 'the host does not delegate access to servers';
    if (!this.grant.ready) return 'the host has not sent its policy yet';
    // RFC-015 §4.1: only an explicit grant counts, never a wildcard.
    if (!this.grant.effectiveCapabilities.includes('access.token')) return 'the host has not granted this server access (access.token)';
    return null;
  }

  async requestAccess(audience: string, opts: AccessOptions): Promise<AccessCredential> {
    const blocked = this.accessBlocked();
    if (blocked) throw new AccessError(audience, blocked);
    if (!audience || [...audience].length > 128) throw new AccessError(audience, 'bad audience name');
    const params: Record<string, unknown> = { audience };
    if (opts.endpoint) params.endpoint = opts.endpoint;
    if (opts.purpose) params.purpose = [...opts.purpose].slice(0, 256).join('');
    if (opts.fresh) params.fresh = true;
    let r: unknown;
    try {
      r = await this.conn.sendRequest('access/token', params, 15_000);
    } catch (e) {
      const data = e instanceof RpcError ? (e.data as { reason?: unknown } | undefined) : undefined;
      const reason = typeof data?.reason === 'string' ? data.reason : e instanceof RpcError ? e.message : `the host did not answer (${(e as Error).message})`;
      this.server.log(`access ${audience}: refused — ${reason}`);
      throw new AccessError(audience, reason);
    }
    const res = (r ?? {}) as { scheme?: unknown; credential?: unknown; expiresAt?: unknown; principal?: unknown };
    const exp = typeof res.expiresAt === 'string' ? Date.parse(res.expiresAt) : NaN;
    if (res.scheme !== 'bearer' || typeof res.credential !== 'string' || !res.credential || !Number.isFinite(exp)) {
      throw new AccessError(audience, 'the host answered with something that is not a credential');
    }
    // Audit without the credential (RFC-015 §6).
    this.server.log(`access ${audience}${opts.endpoint ? ` at ${auditEndpoint(opts.endpoint)}` : ''}: granted until ${new Date(exp).toISOString()}`);
    return { scheme: 'bearer', credential: res.credential, expiresAt: exp, ...(typeof res.principal === 'string' ? { principal: res.principal } : {}) };
  }

  private async request(id: JsonRpcId, m: string, p: Record<string, unknown>): Promise<void> {
    try {
      switch (m) {
        case 'ping': this.conn.sendResponse(id, {}); return;
        case method.FEATURE_SETS_UPDATE: {
          const r = grantFromUpdate(this.grant, p as FeatureSetsUpdateParams, 'request');
          this.grant = r.state;
          if (r.malformed) { this.conn.sendError(id, -32602, `paths both granted and denied: ${r.conflicts.join(', ')}`); return; }
          this.conn.sendResponse(id, this.receipt());
          this.owner.attach(this);
          return;
        }
        case 'tools/list':
          this.conn.sendResponse(id, { tools: this.server.toolList(this.owner, this.compat), ...(this.hints ? { _meta: { 'mcpl/rev': this.rev } } : {}) });
          return;
        case 'tools/call': return this.respond(id, await this.call(p));
        case 'toolsets/open': return this.respond(id, await this.open(p));
        case 'toolsets/close': return this.respond(id, await this.close(p));
        case 'toolsets/list': return this.respond(id, this.list());
        default: this.conn.sendError(id, -32601, `Method not found: ${m}`);
      }
    } catch (e) {
      if (e instanceof RpcFail) this.conn.sendError(id, e.code, e.message, e.data);
      else { this.server.log(`${m} failed:`, e); this.conn.sendError(id, -32603, (e as Error).message); }
    }
  }

  // ── Hints ──

  /** Differences since this connection was last told, for the given instances. */
  private diff(insts: Iterable<Instance>): NonNullable<HintChanges['tools']> {
    const changes: NonNullable<HintChanges['tools']> = [];
    for (const inst of insts) {
      const before = this.reported.get(inst.key) ?? { visible: new Set<string>(), defs: new Map<string, string>() };
      if (!inst.open) {
        // Closed since this connection was told: everything it was told is visible goes.
        for (const name of before.visible) changes.push({ name, ...(inst.name ? { instance: inst.name } : {}), visible: false });
        this.reported.delete(inst.key);
        continue;
      }
      const now = inst.visibleNow();
      const defs = new Map<string, string>();
      for (const t of inst.spec.tools) {
        const visibleNow = now.has(t.name);
        const def = visibleNow ? inst.definition(t.name) : undefined;
        const d = def && def !== inst.spec.tools.find((x) => x.name === t.name) ? digest(def) : '';
        if (visibleNow) defs.set(t.name, d);
        const was = before.visible.has(t.name);
        if (visibleNow !== was || (visibleNow && before.defs.get(t.name) !== d)) {
          changes.push({
            name: t.name, ...(inst.name ? { instance: inst.name } : {}), visible: visibleNow,
            ...(visibleNow && d && before.defs.get(t.name) !== d ? { definition: hinted(def!, inst.toolset, t) } : {}),
          });
        }
      }
      this.reported.set(inst.key, { visible: now, defs });
    }
    return changes;
  }

  /**
   * Build a HintChanges for everything this connection has not been told — tool visibility and
   * definitions, and toolsets opened, closed or retitled (by this connection or another of the
   * same principal, RFC-014 §5.6) — or null if there is nothing.
   */
  private changes(insts: Instance[], suggest?: HintChanges['suggest']): HintChanges | null {
    const toolsets: NonNullable<HintChanges['toolsets']> = [];
    const live = new Map([...this.owner.instances.values()].filter((i) => i.open).map((i) => [i.key, i]));
    for (const [key, inst] of live) {
      const title = inst.handler.title;
      const was = this.reportedOpen.get(key);
      if (!was || was.title !== title) toolsets.push({ toolset: inst.toolset, ...(inst.name ? { instance: inst.name } : {}), state: 'open', title });
      this.reportedOpen.set(key, { title });
    }
    for (const key of [...this.reportedOpen.keys()]) {
      if (live.has(key)) continue;
      const [toolset, name] = key.split('\u0000');
      toolsets.push({ toolset: toolset!, ...(name ? { instance: name } : {}), state: 'closed' });
      this.reportedOpen.delete(key);
    }
    const tools = this.diff(insts);
    if (!tools.length && !toolsets.length && !suggest?.length) return null;
    return { rev: ++this.rev, ...(tools.length ? { tools } : {}), ...(toolsets.length ? { toolsets } : {}), ...(suggest?.length ? { suggest } : {}) };
  }

  /** Every instance this principal has, plus ones this connection still believes open. */
  private allInstances(): Instance[] {
    const live = [...this.owner.instances.values()];
    const keys = new Set(live.map((i) => i.key));
    // Instances closed since last told: diff() reports their tools invisible.
    const gone: Instance[] = [];
    for (const key of this.reported.keys()) if (!keys.has(key)) gone.push(ghost(key));
    return [...live, ...gone];
  }

  /** Standalone `tools/hints` for what changed outside any call. */
  notifyHints(suggest?: HintChanges['suggest']): void {
    if (!this.hints || this.conn.isClosed) return;
    const h = this.changes(this.allInstances(), suggest);
    if (h) this.conn.sendNotification('tools/hints', h);
  }

  // ── Requests ──

  /**
   * Send a result with the hint changes it carries. The rev is allocated and the message written
   * in the same synchronous step, so changes leave this connection in rev order (RFC-014 §6.1):
   * nothing can be written between taking a rev and sending it.
   */
  private respond(id: JsonRpcId, body: Record<string, unknown>): void {
    const h = this.hints ? this.changes(this.allInstances()) : null;
    this.conn.sendResponse(id, h ? { ...body, _meta: { ...(body._meta as object | undefined), 'mcpl/hints': h } } : body);
    this.owner.hintsChanged(this);
  }

  private async call(p: Record<string, unknown>): Promise<Record<string, unknown>> {
    const name = String(p.name ?? '');
    const args = (p.arguments && typeof p.arguments === 'object' ? p.arguments : {}) as Record<string, unknown>;
    const meta = (p._meta && typeof p._meta === 'object' ? p._meta : {}) as Record<string, unknown>;
    const instance = typeof meta['mcpl/instance'] === 'string' ? meta['mcpl/instance'] as string : undefined;

    if (this.compat && (name === 'toolset_open' || name === 'toolset_close')) return this.compatCall(name, args);

    const { result } = await this.server.callTool(this.owner, name, args, instance, this);
    // What this call caused, in its result (and to this principal's other connections, standalone).
    return { content: result.content, ...(result.isError ? { isError: true } : {}) };
  }

  private async open(p: Record<string, unknown>): Promise<Record<string, unknown>> {
    const toolset = String(p.toolset ?? '');
    const proposed = typeof p.instance === 'string' ? p.instance : undefined;
    const options = (p.options && typeof p.options === 'object' ? p.options : {}) as Record<string, unknown>;
    const { inst, content, adopted } = await this.server.openInstance(this.owner, toolset, proposed, options);
    if (adopted && Object.keys(options).length) content.unshift(txt(`${inst.handler.title} was already open; the options were not applied.`));
    return { toolset, ...(inst.name ? { instance: inst.name } : {}), title: inst.handler.title, content: mergeText(content), ...(adopted ? { adopted: true } : {}) };
  }

  private async close(p: Record<string, unknown>): Promise<Record<string, unknown>> {
    const toolset = String(p.toolset ?? '');
    const name = typeof p.instance === 'string' ? p.instance : undefined;
    const inst = this.owner.instances.get(`${toolset}\u0000${name ?? ''}`);
    if (!inst) throw new RpcFail(-32602, `${toolset}${name ? `:${name}` : ''} is not open`, { toolset, instance: name });
    const said = await inst.lock.run(async () => this.server.closeInstance(inst, 'closed', 'host'));
    return { ...(said ? { content: [txt(String(said))] } : {}) };
  }

  /** RFC-014 §5.3: what this session has open, with full hint state (this connection starts over). */
  private list(): Record<string, unknown> {
    this.reported.clear();
    this.reportedOpen.clear();
    const open = [...this.owner.instances.values()].filter((i) => i.open);
    const hourAgo = Date.now() - 3_600_000;
    this.owner.unseenEnds = this.owner.unseenEnds.filter((e) => e.at > hourAgo);
    const ended = this.owner.unseenEnds.filter((e) => !this.endsTold.has(e.id)).map((e) => {
      this.endsTold.add(e.id);
      return { toolset: e.toolset, ...(e.instance ? { instance: e.instance } : {}), reason: e.reason };
    });
    return {
      open: open.map((i) => ({ toolset: i.toolset, ...(i.name ? { instance: i.name } : {}), title: i.handler.title })),
      ...(ended.length ? { ended } : {}),
    };
  }

  private async compatCall(name: string, args: Record<string, unknown>): Promise<Record<string, unknown>> {
    try {
      if (name === 'toolset_open') {
        const r = await this.open({ toolset: args.toolset, instance: args.instance, options: args.options });
        const content = (r.content as ContentBlock[]) ?? [];
        const inst = [...this.owner.instances.values()].find((i) => i.toolset === args.toolset && (args.instance === undefined || i.name === args.instance));
        const now = inst ? [...inst.visibleNow()] : [];
        return { content: mergeText([txt(`Opened ${String(r.title ?? args.toolset)}${r.instance ? ` (${String(r.instance)})` : ''}. Available now: ${now.join(', ') || '(nothing)'}.`), ...content]) };
      }
      const r = await this.close({ toolset: args.toolset, instance: args.instance });
      return { content: [txt(`Closed ${String(args.toolset)}.`), ...((r.content as ContentBlock[]) ?? [])] };
    } catch (e) {
      return { content: [txt((e as Error).message)], isError: true };
    }
  }

  // ── Events ──

  push(inst: Instance, ev: InstanceEvent): void {
    this.outbox = this.outbox.then(() => this.pushNow(inst, ev)).catch((e) => this.server.log('push failed:', e));
  }

  private async pushNow(inst: Instance, ev: InstanceEvent): Promise<void> {
    if (!this.canPush()) return;
    const content: ContentBlock[] = [txt(ev.text), ...(ev.content ?? [])];
    if (ev.includeContent && inst.open) content.push(...await contentOf(inst));
    // A visibility change rides the event that explains it (RFC-014 §6.1), never a replaceable one.
    const h = this.hints && ev.kind === 'activity' ? this.changes(this.allInstances()) : null;
    const params: Record<string, unknown> = {
      featureSet: this.fsEvents, eventId: `${this.idPrefix}_${++this.seq}`, timestamp: new Date().toISOString(),
      tags: ev.tags ?? [], toolset: inst.toolset, ...(inst.name ? { instance: inst.name } : {}),
      payload: { content: mergeText(content) },
      ...(h ? { hints: h } : {}),
      ...(ev.kind === 'state' && this.coalesce ? { coalesce: { key: `${inst.toolset}/${inst.name ?? ''}/${ev.key ?? 'state'}` } } : {}),
    };
    let accepted = false;
    try {
      const r = await this.conn.sendRequest(method.PUSH_EVENT, params) as { accepted?: boolean } | null;
      accepted = r?.accepted !== false;
    } catch (e) {
      this.server.log('push not delivered:', (e as Error).message);
    }
    // A refused push's hints were not applied. Forget what it told this connection and restate the
    // current state of those instances under a new rev (RFC-014 §6.1): entries are absolute values,
    // so restating is safe, and a new rev never goes backwards.
    if (h && !accepted && !this.conn.isClosed) {
      for (const t of h.tools ?? []) this.reported.delete(`${inst.toolset}\u0000${t.instance ?? inst.name ?? ''}`);
      for (const ts of h.toolsets ?? []) this.reportedOpen.delete(`${ts.toolset}\u0000${ts.instance ?? ''}`);
      this.notifyHints();
    }
    // A state event never carries visibility; if it changed, say so standalone.
    if (ev.kind === 'state') this.notifyHints();
  }
}

/** A definition as it travels in a hint change: carrying its own hints. */
function hinted(def: ToolDef, toolset: string, declared?: ToolDef): ToolDef {
  // A definition sent in hints may change the description, schema and annotations — never the name
  // or the RFC-008 class, which change only through tools/list (RFC-014 §6.1).
  const meta: Record<string, unknown> = { ...def._meta, 'mcpl/presence': 'dynamic', 'mcpl/toolset': toolset };
  if (declared?._meta?.['mcpl/class'] !== undefined) meta['mcpl/class'] = declared._meta['mcpl/class']; else delete meta['mcpl/class'];
  return { ...def, name: declared?.name ?? def.name, _meta: meta };
}

/** Stand-in for an instance that has closed, so diff() reports its tools gone. */
function ghost(key: string): Instance {
  const [toolset, name] = key.split('\u0000');
  return { key, toolset, name: name || undefined, open: false, spec: { tools: [] }, visibleNow: () => new Set() } as unknown as Instance;
}

/** An endpoint as it may be logged: no userinfo, query or fragment (RFC-015 §8.2). */
function auditEndpoint(endpoint: string): string {
  try { const u = new URL(endpoint); return `${u.protocol}//${u.host}${u.pathname}`; } catch { return '(unparseable endpoint)'; }
}
