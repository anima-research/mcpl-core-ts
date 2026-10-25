/**
 * More of the ToolsetServer: instances shared by two principals, the built-in validator, and
 * MCPL RFC-015 delegated access from the server side.
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { PassThrough } from 'node:stream';
import { McplConnection, ToolsetServer, validateJson } from '../src/index.js';
import type { HintChanges, InstanceContext, InstanceHandler, ToolDef, ToolsetServerSpec } from '../src/index.js';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const obj = (p: Record<string, unknown> = {}, req: string[] = []) => ({ type: 'object', properties: p, required: req, additionalProperties: false });
const text = (r: Record<string, unknown>) => ((r.content as Array<{ type: string; text?: string }>) ?? []).filter((b) => b.type === 'text').map((b) => b.text).join('\n');
const hintsOf = (r: Record<string, unknown>) => (r._meta as Record<string, unknown> | undefined)?.['mcpl/hints'] as HintChanges | undefined;

/** A scripted host that tracks visibility per instance from hint changes, in rev order. */
class Host {
  conn!: McplConnection;
  serverSide!: McplConnection;
  pushes: Array<Record<string, unknown>> = [];
  private lastRev = 0;
  private state = new Map<string, Set<string>>();
  static async connect(server: ToolsetServer, principal: string): Promise<Host> {
    const a = new PassThrough(); const b = new PassThrough();
    const h = new Host();
    h.serverSide = McplConnection.fromStreams(a, b);
    void server.serve(h.serverSide, principal);
    h.conn = McplConnection.fromStreams(b, a);
    void h.pump();
    await h.conn.sendRequest('initialize', { protocolVersion: '2024-11-05', capabilities: { experimental: { mcpl: { version: '0.5', pushEvents: true, toolHints: true } } }, clientInfo: { name: 't', version: '0' } });
    await h.conn.sendRequest('featureSets/update', { effectiveCapabilities: ['tools', 'pushEvents'] });
    return h;
  }
  private apply(hc: HintChanges | undefined): void {
    if (!hc || hc.rev <= this.lastRev) return;
    this.lastRev = hc.rev;
    for (const t of hc.tools ?? []) {
      const set = this.state.get(t.instance ?? '') ?? new Set<string>();
      if (t.visible === true) set.add(t.name); else if (t.visible === false) set.delete(t.name);
      this.state.set(t.instance ?? '', set);
    }
  }
  private async pump(): Promise<void> {
    try {
      for (;;) {
        const m = await this.conn.nextMessage();
        if (m.type === 'notification' && m.notification.method === 'tools/hints') this.apply(m.notification.params as HintChanges);
        if (m.type === 'request' && m.request.method === 'push/event') {
          const p = m.request.params as Record<string, unknown>;
          this.pushes.push(p);
          this.apply(p.hints as HintChanges | undefined);
          this.conn.sendResponse(m.request.id, { accepted: true });
        }
      }
    } catch { /* closed */ }
  }
  async req(method: string, params: unknown): Promise<Record<string, unknown>> {
    const r = await this.conn.sendRequest(method, params) as Record<string, unknown>;
    this.apply(hintsOf(r));
    return r;
  }
  call(name: string, args: Record<string, unknown> = {}, instance?: string) {
    return this.req('tools/call', { name, arguments: args, ...(instance ? { _meta: { 'mcpl/instance': instance } } : {}) });
  }
  visible(instance = ''): string[] { return [...(this.state.get(instance) ?? [])].sort(); }
  async waitPush(n: number): Promise<void> {
    const end = Date.now() + 8000;
    while (this.pushes.length < n) { if (Date.now() > end) throw new Error(`waited for ${n} pushes`); await sleep(20); }
  }
  close(): void { try { this.conn.close(); } catch { /* */ } try { this.serverSide.close(); } catch { /* */ } }
}

// ── Dice: a toolset with instances; a second principal joins by id ──

function diceSpec(): ToolsetServerSpec {
  const games = new Map<string, { players: Map<string, InstanceContext>; turn: string; score: Record<string, number> }>();
  const tools: ToolDef[] = [
    { name: 'roll', description: 'Roll on your turn.', inputSchema: obj() },
    { name: 'concede', description: 'Give up.', inputSchema: obj() },
  ];
  return {
    name: 'dice', version: '0', surface: { title: 'Dice', summary: 'Dice games' },
    toolsets: {
      game: {
        title: 'Dice', summary: 'A dice game', default: 'closed', instances: true,
        options: obj({ join: { type: 'string' } }), tools,
        open(ctx, o): InstanceHandler {
          const id = (o.join as string | undefined) ?? `${ctx.principal}/${ctx.instance}`;
          const g = games.get(id) ?? { players: new Map<string, InstanceContext>(), turn: ctx.principal, score: {} as Record<string, number> };
          games.set(id, g);
          g.players.set(ctx.principal, ctx);
          g.score[ctx.principal] = 0;
          for (const [p, other] of g.players) if (p !== ctx.principal) other.emit({ kind: 'activity', text: `${ctx.principal} joined.`, tags: ['surface:turn'] });
          return {
            title: `Dice ${id}`,
            visible: () => (g.turn === ctx.principal ? ['roll', 'concede'] : ['concede']),
            content: () => [{ type: 'text', text: `game ${id}: ${JSON.stringify(g.score)}` }],
            async call(t) {
              if (t === 'roll') {
                g.score[ctx.principal]! += 3;
                const next = [...g.players.keys()].find((p) => p !== ctx.principal) ?? ctx.principal;
                g.turn = next;
                if (next !== ctx.principal) g.players.get(next)?.emit({ kind: 'activity', text: `${ctx.principal} rolled 3. Your turn.`, tags: ['surface:turn'] });
                return { content: [{ type: 'text', text: 'You rolled 3.' }] };
              }
              return { content: [{ type: 'text', text: 'Conceded.' }] };
            },
            close() { g.players.delete(ctx.principal); },
          };
        },
      },
    },
  };
}

test('named instances; an instance-less call with two open fails; reconnect reports and adopts what is kept', async () => {
  const server = new ToolsetServer(diceSpec());
  const h = await Host.connect(server, 'ada');
  const a = await h.req('toolsets/open', { toolset: 'game', instance: 'first' });
  assert.equal(a.instance, 'first');
  assert.deepEqual(h.visible('first'), ['concede', 'roll']);
  const b = await h.req('toolsets/open', { toolset: 'game' });
  assert.equal(b.instance, 'game-1');
  const ambiguous = await h.call('roll');
  assert.equal(ambiguous.isError, true);
  assert.match(text(ambiguous), /first, game-1/);
  assert.match(text(await h.call('roll', {}, 'first')), /You rolled 3/);
  h.close();
  await sleep(30);
  const h2 = await Host.connect(server, 'ada');
  const list = await h2.req('toolsets/list', {});
  assert.deepEqual((list.open as Array<{ instance: string }>).map((o) => o.instance).sort(), ['first', 'game-1']);
  assert.deepEqual(h2.visible('first'), ['concede', 'roll']);
  const again = await h2.req('toolsets/open', { toolset: 'game', instance: 'first' });
  assert.match(text(again), /"ada":3/, 'opening a kept instance adopts it');
  h2.close();
});

test('two principals in one game: a turn reaches the other player as a push carrying its visibility change', async () => {
  const server = new ToolsetServer(diceSpec());
  const ada = await Host.connect(server, 'ada');
  const bo = await Host.connect(server, 'bo');
  await ada.req('toolsets/open', { toolset: 'game', instance: 'g' });
  await bo.req('toolsets/open', { toolset: 'game', instance: 'g', options: { join: 'ada/g' } });
  await ada.waitPush(1);
  assert.ok((ada.pushes[0]!.tags as string[]).includes('surface:turn'));
  assert.deepEqual(bo.visible('g'), ['concede']);
  await ada.call('roll', {}, 'g');
  assert.deepEqual(ada.visible('g'), ['concede'], 'her own turn: in the result');
  await bo.waitPush(1);
  assert.match(text(bo.pushes[0]!.payload as Record<string, unknown>), /ada rolled 3\. Your turn\./);
  assert.deepEqual(bo.visible('g'), ['concede', 'roll'], 'his turn: on the push');
  const closed = await bo.req('toolsets/close', { toolset: 'game', instance: 'g' });
  assert.equal(hintsOf(closed)!.toolsets![0]!.state, 'closed');
  assert.deepEqual(bo.visible('g'), []);
  ada.close(); bo.close();
});

// ── The built-in validator ──

test('validateJson: the keywords tool schemas use', () => {
  const s = obj({
    point: { type: 'string', pattern: '^[A-T](1[0-9]|[1-9])$' },
    n: { type: 'integer', minimum: 1, maximum: 3, default: 2 },
    tags: { type: 'array', items: { enum: ['a', 'b'] }, maxItems: 2 },
  }, ['point']);
  const ok: Record<string, unknown> = { point: 'D4' };
  assert.equal(validateJson(s, ok), null);
  assert.equal(ok.n, 2, 'defaults are filled');
  assert.match(validateJson(s, {})!, /missing required property "point"/);
  assert.match(validateJson(s, { point: 'Z99' })!, /must match/);
  assert.match(validateJson(s, { point: 'D4', n: 1.5 })!, /must be integer/);
  assert.match(validateJson(s, { point: 'D4', n: 9 })!, /≤ 3/);
  assert.match(validateJson(s, { point: 'D4', extra: 1 })!, /unexpected property "extra"/);
  assert.match(validateJson(s, { point: 'D4', tags: ['a', 'c'] })!, /one of "a", "b"/);
  assert.match(validateJson({ oneOf: [{ type: 'string' }, { type: 'integer' }] }, true)!, /exactly one/);
});

test('a server may plug in its own validator', async () => {
  const seen: unknown[] = [];
  const spec = diceSpec();
  spec.validate = (_schema, value) => { seen.push(value); return null; };
  const server = new ToolsetServer(spec);
  const h = await Host.connect(server, 'v');
  await h.req('toolsets/open', { toolset: 'game', instance: 'x' });
  await h.call('roll', {}, 'x');
  assert.ok(seen.length >= 1);
  h.close();
});

// ── RFC-015: delegated access ──

function accessSpec(seen: string[]): ToolsetServerSpec {
  return {
    name: 'needs-access', version: '0',
    toolsets: {
      remote: {
        title: 'Remote', summary: 'Something behind a login', default: 'closed',
        tools: [{ name: 'fetch', description: 'Fetch.', inputSchema: obj() }],
        async open(ctx) {
          const c = await ctx.access('games', { endpoint: 'wss://games.example/mcpl' });
          seen.push(c.credential);
          return {
            title: 'Remote', visible: () => ['fetch'],
            async call() { seen.push((await ctx.access('games')).credential); return { content: [{ type: 'text', text: 'fetched' }] }; },
            close() {},
          };
        },
      },
    },
  };
}

async function accessHost(server: ToolsetServer, opts: { advertise: boolean; grant: string[]; answer?: (p: Record<string, unknown>) => unknown }) {
  const a = new PassThrough(); const b = new PassThrough();
  void server.serve(McplConnection.fromStreams(a, b), 'p');
  const conn = McplConnection.fromStreams(b, a);
  const asked: Array<Record<string, unknown>> = [];
  void (async () => {
    try {
      for (;;) {
        const m = await conn.nextMessage();
        if (m.type === 'request' && m.request.method === 'access/token') {
          asked.push(m.request.params as Record<string, unknown>);
          const r = opts.answer?.(m.request.params as Record<string, unknown>) as { code?: number; message?: string; data?: unknown } | undefined;
          if (r && typeof r.code === 'number') conn.sendError(m.request.id, r.code, r.message ?? 'refused', r.data);
          else conn.sendResponse(m.request.id, r ?? { scheme: 'bearer', credential: `tok-${asked.length}`, expiresAt: new Date(Date.now() + 15 * 60_000).toISOString() });
        }
      }
    } catch { /* closed */ }
  })();
  await conn.sendRequest('initialize', { protocolVersion: '2024-11-05', capabilities: { experimental: { mcpl: { version: '0.5', toolHints: true, ...(opts.advertise ? { access: { token: true } } : {}) } } }, clientInfo: { name: 't', version: '0' } });
  await conn.sendRequest('featureSets/update', { effectiveCapabilities: opts.grant });
  return { conn, asked };
}

test('RFC-015: a granted server gets a credential from its host, cached across calls', async () => {
  const seen: string[] = [];
  const { conn, asked } = await accessHost(new ToolsetServer(accessSpec(seen)), { advertise: true, grant: ['tools', 'access.token'] });
  await conn.sendRequest('toolsets/open', { toolset: 'remote' });
  await conn.sendRequest('tools/call', { name: 'fetch', arguments: {} });
  assert.deepEqual(seen, ['tok-1', 'tok-1']);
  assert.equal(asked.length, 1);
  assert.equal(asked[0]!.audience, 'games');
  assert.equal(asked[0]!.endpoint, 'wss://games.example/mcpl');
  conn.close();
});

test('RFC-015: a wildcard grant is not enough, and nothing is asked of a host that did not advertise access', async () => {
  for (const [advertise, grant] of [[true, ['tools', 'access.*']], [false, ['tools', 'access.token']]] as const) {
    const { conn, asked } = await accessHost(new ToolsetServer(accessSpec([])), { advertise, grant: [...grant] });
    await assert.rejects(conn.sendRequest('toolsets/open', { toolset: 'remote' }), /no access to "games"/);
    assert.equal(asked.length, 0);
    conn.close();
  }
});

test('RFC-015: a refusal from the host fails the open with its reason', async () => {
  const { conn } = await accessHost(new ToolsetServer(accessSpec([])), {
    advertise: true, grant: ['tools', 'access.token'],
    answer: () => ({ code: -32040, message: 'Access refused', data: { audience: 'games', reason: 'audience not allowed' } }),
  });
  await assert.rejects(conn.sendRequest('toolsets/open', { toolset: 'remote' }), /audience not allowed/);
  conn.close();
});
