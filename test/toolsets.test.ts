import { test, type TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { PassThrough } from 'node:stream';
import { McplConnection } from '../src/index.js';
import { ToolsetServer, validateJson } from '../src/toolsets/index.js';
import type { HintChanges, InstanceContext, InstanceHandler, ToolsetServerSpec, ToolDef } from '../src/toolsets/index.js';

const settle = () => new Promise<void>((r) => setImmediate(r));
const clock = (t: TestContext) => {
  t.mock.timers.enable({ apis: ['setTimeout', 'setInterval', 'Date'], now: Date.now() });
  return async (ms: number) => {
    await settle();
    for (let i = 0; i < ms; i++) { t.mock.timers.tick(1); await settle(); }
  };
};
const obj = (p: Record<string, unknown> = {}, req: string[] = []) => ({ type: 'object', properties: p, required: req, additionalProperties: false });

class Host {
  conn!: McplConnection;
  serverSide?: McplConnection;
  pushes: Array<Record<string, unknown>> = [];
  hints: HintChanges[] = [];
  revs: number[] = [];
  init: Record<string, unknown> = {};
  private lastRev = 0;
  private state = new Map<string, Set<string>>();
  apply(h: HintChanges | undefined): void {
    if (!h || h.rev <= this.lastRev) return;
    this.lastRev = h.rev;
    for (const t of h.tools ?? []) {
      const k = t.instance ?? '';
      const set = this.state.get(k) ?? new Set<string>();
      if (t.visible === true) set.add(t.name); else if (t.visible === false) set.delete(t.name);
      this.state.set(k, set);
    }
  }
  visible(instance = ''): string[] { return [...(this.state.get(instance) ?? [])].sort(); }

  static async connect(server: ToolsetServer, principal: string, opts: { hints?: boolean } = {}): Promise<Host> {
    const a = new PassThrough(); const b = new PassThrough();
    const serverSide = McplConnection.fromStreams(a, b);
    void server.serve(serverSide, principal);
    const h = new Host();
    h.serverSide = serverSide;
    h.conn = McplConnection.fromStreams(b, a);
    h.conn.requestTimeout = 20_000;
    void h.pump();
    h.init = await h.conn.sendRequest('initialize', { protocolVersion: '2024-11-05', capabilities: { experimental: { mcpl: { version: '0.5', pushEvents: true, ...(opts.hints === false ? {} : { toolHints: true }) } } }, clientInfo: { name: 'test', version: '0' } }) as Record<string, unknown>;
    await h.conn.sendRequest('featureSets/update', { effectiveCapabilities: ['tools', 'pushEvents'] });
    return h;
  }

  private async pump(): Promise<void> {
    try {
      for (;;) {
        const m = await this.conn.nextMessage();
        if (m.type === 'notification' && m.notification.method === 'tools/hints') { const hc = m.notification.params as HintChanges; this.hints.push(hc); this.revs.push(hc.rev); this.apply(hc); }
        if (m.type === 'request' && m.request.method === 'push/event') {
          const p = m.request.params as Record<string, unknown>;
          this.pushes.push(p);
          if (p.hints) { this.revs.push((p.hints as HintChanges).rev); this.apply(p.hints as HintChanges); }
          this.conn.sendResponse(m.request.id, { accepted: true });
        }
      }
    } catch { }
  }

  async req(method: string, params: unknown): Promise<Record<string, unknown>> {
    const r = await this.conn.sendRequest(method, params) as Record<string, unknown>;
    const h = (r._meta as Record<string, unknown> | undefined)?.['mcpl/hints'] as HintChanges | undefined;
    if (h) { this.revs.push(h.rev); this.apply(h); }
    return r;
  }

  call(name: string, args: Record<string, unknown> = {}, instance?: string) {
    return this.req('tools/call', { name, arguments: args, ...(instance ? { _meta: { 'mcpl/instance': instance } } : {}) });
  }

  async waitPush(n: number, elapse: (ms: number) => Promise<void>): Promise<void> {
    for (let waited = 0; this.pushes.length < n; waited += 20) {
      if (waited >= 8000) throw new Error(`waited for ${n} pushes, have ${this.pushes.length}`);
      await elapse(20);
    }
  }

  close(): void { try { this.conn.close(); } catch { } try { this.serverSide?.close(); } catch { } }
}

const text = (r: Record<string, unknown>) => ((r.content as Array<{ type: string; text?: string }>) ?? []).filter((b) => b.type === 'text').map((b) => b.text).join('\n');
const hintsOf = (r: Record<string, unknown>) => (r._meta as Record<string, unknown> | undefined)?.['mcpl/hints'] as HintChanges | undefined;
const visibleIn = (h: HintChanges | undefined, instance?: string) => (h?.tools ?? []).filter((t) => t.visible && (instance === undefined || t.instance === instance)).map((t) => t.name).sort();


let lampCtx: InstanceContext | null = null;
let lampState = { on: false, levels: [1, 2, 3] };
function lampSpec(): ToolsetServerSpec {
  const tools: Record<string, ToolDef> = {
    on: { name: 'on', description: 'Switch on.', inputSchema: obj() },
    off: { name: 'off', description: 'Switch off.', inputSchema: obj() },
    dim: { name: 'dim', description: 'Dim.', inputSchema: obj({ level: { enum: [1, 2, 3] } }, ['level']) },
  };
  return {
    name: 'lamp-server', version: '0',
    staticTools: [{ tool: { name: 'status', description: 'Status.', inputSchema: obj() }, call: () => ({ content: [{ type: 'text', text: 'fine' }] }) }],
    toolsets: {
      lamp: {
        title: 'Lamp', summary: 'A lamp', default: 'closed', tools: Object.values(tools),
        open(ctx) {
          lampCtx = ctx; lampState = { on: false, levels: [1, 2, 3] };
          return {
            title: 'Lamp',
            visible: () => (lampState.on ? ['off', 'dim'] : ['on']),
            definition: (t) => (t === 'dim' ? { ...tools.dim!, inputSchema: obj({ level: { enum: lampState.levels } }, ['level']) } : undefined),
            content: () => [{ type: 'text', text: `lamp ${lampState.on ? 'on' : 'off'}` }],
            async call(t) { if (t === 'on') lampState.on = true; if (t === 'off') lampState.on = false; return { content: [{ type: 'text', text: `did ${t}` }] }; },
            close() {},
          };
        },
      },
    },
  };
}

test('1. tools/list lists every tool with hints, visible or not', async () => {
  const server = new ToolsetServer(lampSpec());
  const h = await Host.connect(server, 'p');
  const tools = (await h.req('tools/list', {})).tools as ToolDef[];
  const byName = Object.fromEntries(tools.map((t) => [t.name, t._meta]));
  assert.equal(byName.status!['mcpl/presence'], 'static');
  for (const n of ['on', 'off', 'dim']) {
    assert.equal(byName[n]!['mcpl/presence'], 'dynamic');
    assert.equal(byName[n]!['mcpl/toolset'], 'lamp');
    assert.equal(byName[n]!['mcpl/visible'], false);
  }
  const caps = ((h.init.capabilities as Record<string, unknown>).experimental as Record<string, Record<string, unknown>>).mcpl!;
  assert.equal(caps.toolHints, true);
  assert.deepEqual(Object.keys(caps.toolsets as object), ['lamp']);
  h.close();
});

test('2. open and calls carry their own hint changes; nothing is pushed for them', async (t) => {
  const elapse = clock(t);
  const server = new ToolsetServer(lampSpec());
  const h = await Host.connect(server, 'p');
  const o = await h.req('toolsets/open', { toolset: 'lamp' });
  assert.match(text(o), /lamp off/);
  assert.deepEqual(visibleIn(hintsOf(o)), ['on']);
  assert.equal(hintsOf(o)!.toolsets?.[0]?.state, 'open');
  const r = await h.call('on');
  const hc = hintsOf(r)!;
  assert.deepEqual(visibleIn(hc), ['dim', 'off']);
  assert.deepEqual(hc.tools!.filter((t) => !t.visible).map((t) => t.name), ['on']);
  await elapse(80);
  assert.equal(h.pushes.length, 0);
  assert.equal(h.hints.length, 0);
  h.close();
});

test('3+4. a change from elsewhere: definition by notification, visibility on an explaining push; revs increase', async (t) => {
  const elapse = clock(t);
  const server = new ToolsetServer(lampSpec());
  const h = await Host.connect(server, 'p');
  await h.req('toolsets/open', { toolset: 'lamp' });
  await h.call('on');
  lampState.levels = [1, 2, 3, 4, 5];
  lampCtx!.changed();
  await elapse(80);
  const defChange = h.hints.at(-1)!.tools!.find((t) => t.name === 'dim')!;
  assert.deepEqual((defChange.definition!.inputSchema.properties as Record<string, { enum: number[] }>).level.enum, [1, 2, 3, 4, 5]);
  lampState.on = false;
  lampCtx!.emit({ kind: 'activity', text: 'The fuse blew.', tags: ['surface:alert'] });
  await h.waitPush(1, elapse);
  const p = h.pushes[0]!;
  assert.equal(p.toolset, 'lamp');
  assert.deepEqual(visibleIn(p.hints as HintChanges), ['on']);
  const revs = h.revs;
  assert.ok(revs.every((r, i) => i === 0 || r > revs[i - 1]!), `revs strictly increase: ${revs}`);
  h.close();
});

test('8. a host without toolHints gets no hints, but compat open/close tools that work', async (t) => {
  const elapse = clock(t);
  const server = new ToolsetServer(lampSpec());
  const h = await Host.connect(server, 'p', { hints: false });
  const names = ((await h.req('tools/list', {})).tools as ToolDef[]).map((t) => t.name);
  assert.ok(names.includes('toolset_open') && names.includes('toolset_close'));
  const o = await h.call('toolset_open', { toolset: 'lamp' });
  assert.match(text(o), /Available now: on/);
  const r = await h.call('on');
  assert.equal(hintsOf(r), undefined);
  const bad = await h.call('on');
  assert.equal(bad.isError, true);
  assert.match(text(bad), /not available right now/);
  lampCtx!.changed();
  await elapse(80);
  assert.equal(h.hints.length, 0);
  h.close();
});



test('one principal, two connections: an open on one reaches the other as hint changes, not a suggestion', async (t) => {
  const elapse = clock(t);
  const server = new ToolsetServer(lampSpec());
  const a = await Host.connect(server, 'p');
  const b = await Host.connect(server, 'p');
  await a.req('toolsets/open', { toolset: 'lamp' });
  await elapse(80);
  const toB = b.hints.at(-1)!;
  assert.equal(toB.toolsets?.[0]?.state, 'open');
  assert.equal(toB.suggest, undefined);
  assert.deepEqual(b.visible(), ['on']);
  await a.call('on');
  await elapse(80);
  assert.deepEqual(b.visible(), ['dim', 'off'], 'the other connection follows');
  const list = await a.req('tools/list', {});
  assert.equal(typeof (list._meta as Record<string, unknown>)['mcpl/rev'], 'number');
  a.close(); b.close();
});

test('an instance that ends while nobody is connected is reported as ended by toolsets/list', async (t) => {
  const elapse = clock(t);
  const server = new ToolsetServer(lampSpec());
  const a = await Host.connect(server, 'p');
  await a.req('toolsets/open', { toolset: 'lamp' });
  a.close();
  await elapse(50);
  lampCtx!.close('the bulb burned out');
  await elapse(50);
  const b = await Host.connect(server, 'p');
  const list = await b.req('toolsets/list', {});
  assert.deepEqual(list.open, []);
  const ended = list.ended as Array<{ id: string; toolset: string; reason: string }>;
  assert.equal(ended.length, 1);
  assert.deepEqual({ toolset: ended[0]!.toolset, reason: ended[0]!.reason }, { toolset: 'lamp', reason: 'the bulb burned out' });
  assert.equal(typeof ended[0]!.id, 'string');
  const again = await b.req('toolsets/list', {});
  assert.deepEqual(again.ended, ended, 'reported on every list until it expires, with the same id');
  const c = await Host.connect(server, 'p');
  const forC = await c.req('toolsets/list', {});
  assert.deepEqual(forC.ended, ended, 'another connection hears it too, under the same id');
  b.close(); c.close();
});

test('a refused push is not restated: its hints apply on arrival, and the next change takes the next rev', async (t) => {
  const elapse = clock(t);
  const server = new ToolsetServer(lampSpec());
  const a = new PassThrough(); const b = new PassThrough();
  void server.serve(McplConnection.fromStreams(a, b), 'p');
  const conn = McplConnection.fromStreams(b, a);
  const notes: HintChanges[] = [];
  const pushedRevs: number[] = [];
  void (async () => {
    try {
      for (;;) {
        const m = await conn.nextMessage();
        if (m.type === 'notification' && m.notification.method === 'tools/hints') notes.push(m.notification.params as HintChanges);
        if (m.type === 'request' && m.request.method === 'push/event') {
          const h = (m.request.params as Record<string, unknown>).hints as HintChanges | undefined;
          if (h) pushedRevs.push(h.rev);
          conn.sendResponse(m.request.id, { accepted: false, reason: 'busy' });
        }
      }
    } catch { }
  })();
  await conn.sendRequest('initialize', { protocolVersion: '2024-11-05', capabilities: { experimental: { mcpl: { version: '0.5', pushEvents: true, toolHints: true } } }, clientInfo: { name: 't', version: '0' } });
  await conn.sendRequest('featureSets/update', { effectiveCapabilities: ['tools', 'pushEvents'] });
  const opened = await conn.sendRequest('toolsets/open', { toolset: 'lamp' }) as Record<string, unknown>;
  const openRev = ((opened._meta as Record<string, unknown>)['mcpl/hints'] as HintChanges).rev;
  lampState.on = true;
  lampCtx!.emit({ kind: 'activity', text: 'Someone switched it on.' });
  await elapse(150);
  assert.deepEqual(pushedRevs, [openRev + 1]);
  assert.equal(notes.length, 0, 'nothing is restated');
  lampState.on = false;
  lampCtx!.emit({ kind: 'activity', text: 'Someone switched it off.' });
  await elapse(150);
  assert.deepEqual(pushedRevs, [openRev + 1, openRev + 2], 'consecutive revs');
  conn.close();
});

test('a default-open toolset is open from the start, reported by toolsets/list', async () => {
  const spec = lampSpec();
  spec.toolsets.lamp!.default = 'open';
  const server = new ToolsetServer(spec);
  const h = await Host.connect(server, 'p');
  const list = await h.req('toolsets/list', {});
  assert.deepEqual((list.open as Array<{ toolset: string }>).map((o) => o.toolset), ['lamp']);
  assert.deepEqual(h.visible(), ['on']);
  h.close();
});
