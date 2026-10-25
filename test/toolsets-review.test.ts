import { test } from 'node:test';
import assert from 'node:assert/strict';
import { PassThrough } from 'node:stream';
import { McplConnection } from '../src/index.js';
import { ToolsetServer, validateJson } from '../src/toolsets/index.js';
import type { HintChanges, InstanceContext, ToolDef, ToolsetServerSpec } from '../src/toolsets/index.js';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const obj = (p: Record<string, unknown> = {}, req: string[] = []) => ({ type: 'object', properties: p, required: req, additionalProperties: false });
const text = (r: Record<string, unknown>) => ((r.content as Array<{ type: string; text?: string }>) ?? []).filter((b) => b.type === 'text').map((b) => b.text).join('\n');

interface HostOpts { hints?: boolean; caps?: string[]; refusePushes?: boolean; disabled?: string[] }

class Host {
  conn!: McplConnection;
  serverSide!: McplConnection;
  notes: HintChanges[] = [];
  pushes: Array<Record<string, unknown>> = [];
  lastRev = 0;
  skips: number[] = [];
  private state = new Map<string, Set<string>>();
  readonly open = new Set<string>();
  readonly defs = new Map<string, ToolDef>();

  static async connect(server: ToolsetServer, principal: string, o: HostOpts = {}): Promise<Host> {
    const a = new PassThrough(); const b = new PassThrough();
    const h = new Host();
    h.serverSide = McplConnection.fromStreams(a, b);
    void server.serve(h.serverSide, principal);
    h.conn = McplConnection.fromStreams(b, a);
    void h.pump(o.refusePushes === true);
    await h.conn.sendRequest('initialize', { protocolVersion: '2024-11-05', capabilities: { experimental: { mcpl: { version: '0.5', pushEvents: true, ...(o.hints === false ? {} : { toolHints: true }) } } }, clientInfo: { name: 't', version: '0' } });
    await h.conn.sendRequest('featureSets/update', { effectiveCapabilities: o.caps ?? ['tools', 'pushEvents'], ...(o.disabled ? { disabled: o.disabled } : {}) });
    return h;
  }

  apply(hc: HintChanges | undefined, full = false): void {
    if (!hc) return;
    if (hc.rev !== this.lastRev + 1) this.skips.push(hc.rev);
    this.lastRev = hc.rev;
    if (full) this.state.clear();
    for (const t of hc.tools ?? []) {
      const set = this.state.get(t.instance ?? '') ?? new Set<string>();
      if (t.visible === true) set.add(t.name); else if (t.visible === false) set.delete(t.name);
      this.state.set(t.instance ?? '', set);
      if (t.definition) this.defs.set(`${t.instance ?? ''}/${t.name}`, t.definition);
    }
    for (const ts of hc.toolsets ?? []) {
      const k = `${ts.toolset}/${ts.instance ?? ''}`;
      if (ts.state === 'open') this.open.add(k); else this.open.delete(k);
    }
  }

  private async pump(refuse: boolean): Promise<void> {
    try {
      for (;;) {
        const m = await this.conn.nextMessage();
        if (m.type === 'notification' && m.notification.method === 'tools/hints') { const hc = m.notification.params as HintChanges; this.notes.push(hc); this.apply(hc); }
        if (m.type === 'request' && m.request.method === 'push/event') {
          const p = m.request.params as Record<string, unknown>;
          this.pushes.push(p);
          this.apply(p.hints as HintChanges | undefined);
          this.conn.sendResponse(m.request.id, refuse ? { accepted: false, reason: 'busy' } : { accepted: true });
        }
      }
    } catch { }
  }

  async req(method: string, params: unknown): Promise<Record<string, unknown>> {
    const r = await this.conn.sendRequest(method, params) as Record<string, unknown>;
    if (method === 'toolsets/list') this.open.clear();
    this.apply((r._meta as Record<string, unknown> | undefined)?.['mcpl/hints'] as HintChanges | undefined, method === 'toolsets/list');
    if (method === 'toolsets/list') for (const o of (r.open as Array<{ toolset: string; instance?: string }>)) this.open.add(`${o.toolset}/${o.instance ?? ''}`);
    return r;
  }

  call(name: string, args: Record<string, unknown> = {}, instance?: string) {
    return this.req('tools/call', { name, arguments: args, ...(instance ? { _meta: { 'mcpl/instance': instance } } : {}) });
  }

  visible(instance = ''): string[] { return [...(this.state.get(instance) ?? [])].sort(); }
  close(): void { try { this.conn.close(); } catch { } try { this.serverSide.close(); } catch { } }
}

let lampCtx: InstanceContext | null = null;
let lamp = { on: false, levels: [1, 2, 3], closed: 0 };
const lampTools: Record<string, ToolDef> = {
  on: { name: 'on', description: 'Switch on.', inputSchema: obj() },
  off: { name: 'off', description: 'Switch off.', inputSchema: obj() },
  dim: { name: 'dim', description: 'Dim.', inputSchema: obj({ level: { enum: [1, 2, 3] } }, ['level']) },
  quit: { name: 'quit', description: 'Put the lamp away.', inputSchema: obj() },
};
function lampSpec(extra: Partial<ToolsetServerSpec> = {}): ToolsetServerSpec {
  return {
    name: 'lamp-server', version: '0',
    staticTools: [
      { tool: { name: 'count', description: 'Count.', inputSchema: obj({ n: { type: 'integer' } }, ['n']) }, call: (a) => ({ content: [{ type: 'text', text: `n=${String(a.n)}` }] }) },
      { tool: { name: 'explode', description: 'Throws.', inputSchema: obj() }, call: () => { throw new Error('kaboom'); } },
    ],
    toolsets: {
      lamp: {
        title: 'Lamp', summary: 'A lamp', default: 'closed', tools: Object.values(lampTools),
        open(ctx) {
          lampCtx = ctx; lamp = { on: false, levels: [1, 2, 3], closed: 0 };
          return {
            title: 'Lamp',
            visible: () => (lamp.on ? ['off', 'dim', 'quit'] : ['on', 'quit']),
            definition: (t) => (t === 'dim' ? { ...lampTools.dim!, inputSchema: obj({ level: { enum: lamp.levels } }, ['level']) } : undefined),
            async call(t) {
              if (t === 'on') lamp.on = true;
              if (t === 'off') lamp.on = false;
              if (t === 'quit') ctx.close('put away');
              return { content: [{ type: 'text', text: `did ${t}` }] };
            },
            close() { lamp.closed++; },
          };
        },
      },
    },
    ...extra,
  };
}

test('a refused push is not restated: the host applied its hints on arrival, including a close', async () => {
  const server = new ToolsetServer(lampSpec());
  const h = await Host.connect(server, 'p', { refusePushes: true });
  await h.req('toolsets/open', { toolset: 'lamp' });
  assert.deepEqual(h.visible(), ['on', 'quit']);

  lamp.on = true;
  lampCtx!.emit({ kind: 'activity', text: 'Someone switched it on.' });
  await sleep(150);
  assert.equal(h.pushes.length, 1);
  assert.equal(h.notes.length, 0, 'nothing restated');
  assert.deepEqual(h.visible(), ['dim', 'off', 'quit']);

  lampCtx!.close('the bulb burned out');
  await sleep(150);
  assert.equal(h.pushes.length, 2);
  assert.equal(h.notes.length, 0);
  assert.deepEqual(h.visible(), []);
  assert.equal(h.open.size, 0);
  assert.deepEqual(h.skips, [], 'revs consecutive');
  h.close();
});

test('a definition that goes back to the declared one is sent again', async () => {
  const server = new ToolsetServer(lampSpec());
  const h = await Host.connect(server, 'p');
  await h.req('toolsets/open', { toolset: 'lamp' });
  const on = await h.call('on');
  const first = ((on._meta as Record<string, unknown>)['mcpl/hints'] as HintChanges).tools!.find((t) => t.name === 'dim')!;
  assert.equal(first.definition, undefined, 'an equal definition is the declared one: not sent');
  lamp.levels = [1, 2];
  lampCtx!.changed();
  await sleep(100);
  assert.deepEqual((h.defs.get('/dim')!.inputSchema as { properties: { level: { enum: number[] } } }).properties.level.enum, [1, 2]);
  lamp.levels = [1, 2, 3];
  lampCtx!.changed();
  await sleep(100);
  assert.deepEqual((h.defs.get('/dim')!.inputSchema as { properties: { level: { enum: number[] } } }).properties.level.enum, [1, 2, 3], 'the revert reaches the host');
  h.close();
});

test('a call that closes its own instance reports it in its result', async () => {
  const server = new ToolsetServer(lampSpec());
  const h = await Host.connect(server, 'p');
  await h.req('toolsets/open', { toolset: 'lamp' });
  const r = await h.call('quit');
  assert.match(text(r), /did quit/);
  assert.match(text(r), /put away/);
  assert.equal(h.open.size, 0);
  assert.deepEqual(h.visible(), []);
  assert.equal(lamp.closed, 1);
  h.close();
});

test('static tools: arguments are validated, and a throw is a tool error, not an RPC error', async () => {
  const server = new ToolsetServer(lampSpec());
  const h = await Host.connect(server, 'p');
  const bad = await h.call('count', { n: 'seven' });
  assert.equal(bad.isError, true);
  assert.match(text(bad), /Nothing was done/);
  assert.match(text(await h.call('count', { n: 7 })), /n=7/);
  const boom = await h.call('explode');
  assert.equal(boom.isError, true);
  assert.match(text(boom), /kaboom/);
  const toolsetBad = await (async () => { await h.req('toolsets/open', { toolset: 'lamp' }); return h.call('on', { extra: 1 }); })();
  assert.equal(toolsetBad.isError, true);
  assert.match(text(toolsetBad), /Nothing was done/);
  assert.equal(lamp.on, false);
  h.close();
});

test('a host that takes hints but no pushes still hears visibility changes and closes', async () => {
  const server = new ToolsetServer(lampSpec());
  const h = await Host.connect(server, 'p', { caps: ['tools'] });
  await h.req('toolsets/open', { toolset: 'lamp' });
  lamp.on = true;
  lampCtx!.emit({ kind: 'activity', text: 'Someone switched it on.' });
  await sleep(100);
  assert.equal(h.pushes.length, 0);
  assert.deepEqual(h.visible(), ['dim', 'off', 'quit']);
  lampCtx!.close('the bulb burned out');
  await sleep(100);
  assert.equal(h.open.size, 0);
  assert.deepEqual(h.visible(), []);
  h.close();
});

test('an MCPL host that did not grant tools, or disabled the tools feature set, gets no calls', async () => {
  const server = new ToolsetServer(lampSpec());
  const noTools = await Host.connect(server, 'p', { caps: ['pushEvents'] });
  await assert.rejects(noTools.call('count', { n: 1 }), (e: Error & { code?: number }) => e.code === -32002);
  await assert.rejects(noTools.req('toolsets/open', { toolset: 'lamp' }), (e: Error & { code?: number }) => e.code === -32002);
  noTools.close();
  const disabled = await Host.connect(server, 'q', { disabled: ['lamp-server.tools'] });
  await assert.rejects(disabled.call('count', { n: 1 }), (e: Error & { code?: number }) => e.code === -32001);
  disabled.close();
});

function boxSpec(opened: string[]): ToolsetServerSpec {
  return {
    name: 'box-server', version: '0',
    toolsets: {
      box: {
        title: 'Box', summary: 'A box', default: 'closed', instances: true,
        options: obj({ mode: { enum: ['x', 'y'], default: 'x' } }),
        tools: [
          { name: 'x_tool', description: 'X.', inputSchema: obj() },
          { name: 'y_tool', description: 'Y.', inputSchema: obj() },
        ],
        async open(ctx, options) {
          await sleep(40);
          opened.push(ctx.instance!);
          return { title: `Box ${ctx.instance}`, visible: () => [`${String(options.mode)}_tool`], async call(t) { return { content: [{ type: 'text', text: t }] }; }, close() {} };
        },
      },
    },
  };
}

test('two opens of the same instance at once open it once; the second adopts', async () => {
  const opened: string[] = [];
  const server = new ToolsetServer(boxSpec(opened));
  const h = await Host.connect(server, 'p');
  const [a, b] = await Promise.all([h.req('toolsets/open', { toolset: 'box', instance: 'k' }), h.req('toolsets/open', { toolset: 'box', instance: 'k' })]);
  assert.deepEqual(opened, ['k']);
  assert.equal([a, b].filter((r) => r.adopted === true).length, 1);
  h.close();
});

test('a server-chosen name never lands on a name in use', async () => {
  const opened: string[] = [];
  const server = new ToolsetServer(boxSpec(opened));
  const h = await Host.connect(server, 'p');
  await h.req('toolsets/open', { toolset: 'box', instance: 'box-1' });
  const r = await h.req('toolsets/open', { toolset: 'box' });
  assert.equal(r.instance, 'box-2');
  assert.equal(r.adopted, undefined);
  h.close();
});

test('compat toolset_open reports the tools of the instance it opened', async () => {
  const opened: string[] = [];
  const server = new ToolsetServer(boxSpec(opened));
  const h = await Host.connect(server, 'p', { hints: false });
  await h.call('toolset_open', { toolset: 'box', instance: 'a', options: { mode: 'x' } });
  const r = await h.call('toolset_open', { toolset: 'box', options: { mode: 'y' } });
  assert.match(text(r), /Available now: y_tool/);
  h.close();
});

test('a principal away longer than idleCloseMs has its instances closed and is forgotten', async () => {
  const server = new ToolsetServer(lampSpec({ idleCloseMs: 60 }));
  const h = await Host.connect(server, 'p');
  await h.req('toolsets/open', { toolset: 'lamp' });
  h.close();
  await sleep(250);
  assert.equal(lamp.closed, 1);
  assert.equal(server.owners.has('p'), false);
});

test('validateJson: inherited names are not properties, and anyOf holds next to oneOf', () => {
  const s = obj({ a: { type: 'string' } });
  assert.match(validateJson(s, { toString: 1 }) ?? '', /unexpected property "toString"/);
  assert.match(validateJson(s, { constructor: 1 }) ?? '', /unexpected property "constructor"/);
  const both = { oneOf: [{ type: 'string' }, { type: 'number' }], anyOf: [{ const: 'a' }, { type: 'number' }] };
  assert.equal(validateJson(both, 'a'), null);
  assert.match(validateJson(both, 'b') ?? '', /one of the allowed shapes/);
});

test('revs are consecutive across results, pushes and tools/hints, and toolsets/list is a full statement', async () => {
  const server = new ToolsetServer(lampSpec());
  const h = await Host.connect(server, 'p');
  const other = await Host.connect(server, 'p');
  await h.req('toolsets/open', { toolset: 'lamp' });
  await other.call('on');
  await sleep(80);
  lamp.on = false;
  lampCtx!.emit({ kind: 'activity', text: 'It flickered off.' });
  await sleep(80);
  lamp.levels = [1];
  lamp.on = true;
  lampCtx!.changed();
  await sleep(80);
  assert.ok(h.lastRev >= 4);
  assert.deepEqual(h.skips, []);
  const before = h.lastRev;
  const list = await h.req('toolsets/list', {});
  const lh = (list._meta as Record<string, unknown>)['mcpl/hints'] as HintChanges;
  assert.equal(lh.rev, before + 1);
  assert.deepEqual((lh.tools ?? []).filter((t) => t.visible).map((t) => t.name).sort(), ['dim', 'off', 'quit'], 'every visible tool is stated');
  assert.ok((lh.tools ?? []).find((t) => t.name === 'dim')?.definition, 'with its current definition');
  assert.deepEqual(h.visible(), ['dim', 'off', 'quit']);
  h.close(); other.close();
});

test('events held while nobody could take them arrive after toolsets/list, without hints', async () => {
  const server = new ToolsetServer(lampSpec());
  const a = await Host.connect(server, 'p');
  await a.req('toolsets/open', { toolset: 'lamp' });
  a.close();
  await sleep(50);
  lamp.on = true;
  lampCtx!.emit({ kind: 'activity', text: 'Someone switched it on while you were away.' });
  const b = await Host.connect(server, 'p');
  await sleep(80);
  assert.equal(b.pushes.length, 0, 'held until the list');
  await b.req('toolsets/list', {});
  await sleep(80);
  assert.equal(b.pushes.length, 1);
  assert.equal(b.pushes[0]!.hints, undefined);
  assert.match(JSON.stringify(b.pushes[0]!.payload), /while you were away/);
  assert.deepEqual(b.visible(), ['dim', 'off', 'quit'], 'the list stated the state');
  b.close();
});

test('a host without toolHints chooses among instances with toolset_use', async () => {
  const opened: string[] = [];
  const server = new ToolsetServer(boxSpec(opened));
  const h = await Host.connect(server, 'p', { hints: false });
  const names = ((await h.req('tools/list', {})).tools as ToolDef[]).map((t) => t.name);
  assert.ok(names.includes('toolset_use'));
  await h.call('toolset_open', { toolset: 'box', instance: 'a', options: { mode: 'x' } });
  await h.call('toolset_open', { toolset: 'box', instance: 'b', options: { mode: 'y' } });
  assert.match(text(await h.call('y_tool')), /^y_tool$/, 'calls go to the instance last opened');
  await h.call('toolset_use', { toolset: 'box', instance: 'a' });
  assert.match(text(await h.call('x_tool')), /^x_tool$/);
  await h.call('toolset_close', { toolset: 'box', instance: 'a' });
  const r = await h.call('x_tool');
  assert.equal(r.isError, true, 'its current instance closed: the only open one (b) has no x_tool now');
  assert.match(text(await h.call('y_tool')), /^y_tool$/);
  h.close();
});

test('an end is kept for toolsets/list even when a connection was online to hear it', async () => {
  const server = new ToolsetServer(lampSpec());
  const a = await Host.connect(server, 'p');
  await a.req('toolsets/open', { toolset: 'lamp' });
  lampCtx!.close('the bulb burned out');
  await sleep(100);
  assert.equal(a.pushes.length, 1, 'the online connection heard it as an event');
  const b = await Host.connect(server, 'p');
  const list = await b.req('toolsets/list', {});
  assert.deepEqual((list.ended as Array<{ reason: string }>).map((e) => e.reason), ['the bulb burned out'], 'a later connection still learns it');
  const quit = await Host.connect(server, 'q');
  await quit.req('toolsets/open', { toolset: 'lamp' });
  await quit.call('quit');
  const again = await quit.req('toolsets/list', {});
  assert.deepEqual((again.ended as Array<{ reason: string }>).map((e) => e.reason), ['put away'], 'an end a call caused is kept too');
  assert.equal(quit.pushes.length, 0, 'but not pushed: the result reported it');
  a.close(); b.close(); quit.close();
});

test('checks run under the lock: of two concurrent "on" calls, the second finds "on" gone', async () => {
  const server = new ToolsetServer(lampSpec());
  const h = await Host.connect(server, 'p');
  await h.req('toolsets/open', { toolset: 'lamp' });
  const [a, b] = await Promise.all([h.call('on'), h.call('on')]);
  assert.deepEqual([a.isError === true, b.isError === true], [false, true]);
  assert.match(text(b), /not available right now/);
  h.close();
});

test('a preempting tool runs while another call of the instance is in flight', async () => {
  let release!: () => void;
  const order: string[] = [];
  const server = new ToolsetServer({
    name: 'dog', version: '0',
    toolsets: {
      body: {
        title: 'Body', summary: 'A body', default: 'closed',
        tools: [
          { name: 'walk', description: 'Walk.', inputSchema: obj() },
          { name: 'stop', description: 'Stop now.', inputSchema: obj(), _meta: { 'mcpl/preempt': true } },
        ],
        open() {
          return {
            title: 'Body', visible: () => ['walk', 'stop'],
            async call(t) {
              if (t === 'walk') { await new Promise<void>((r) => { release = r; }); order.push('walk done'); }
              else { order.push('stopped'); release(); }
              return { content: [{ type: 'text', text: t }] };
            },
            close() {},
          };
        },
      },
    },
  });
  const h = await Host.connect(server, 'p');
  await h.req('toolsets/open', { toolset: 'body' });
  const walking = h.call('walk');
  await sleep(30);
  await h.call('stop');
  await walking;
  assert.deepEqual(order, ['stopped', 'walk done']);
  h.close();
});

test('opens waiting on a failed open: one opens afresh, the rest adopt it', async () => {
  let attempts = 0;
  const server = new ToolsetServer({
    name: 'flaky', version: '0',
    toolsets: {
      box: {
        title: 'Box', summary: 'A box', default: 'closed', instances: true,
        tools: [{ name: 'x_tool', description: 'X.', inputSchema: obj() }],
        async open() {
          await sleep(30);
          if (++attempts === 1) throw new Error('first open fails');
          return { title: 'Box', visible: () => ['x_tool'], async call() { return { content: [] }; }, close() {} };
        },
      },
    },
  });
  const h = await Host.connect(server, 'p');
  const results = await Promise.allSettled([1, 2, 3].map(() => h.req('toolsets/open', { toolset: 'box', instance: 'k' })));
  assert.equal(attempts, 2, 'one failure, then exactly one fresh open');
  const ok = results.filter((r) => r.status === 'fulfilled').map((r) => (r as PromiseFulfilledResult<Record<string, unknown>>).value);
  assert.equal(ok.length, 2);
  assert.equal(ok.filter((r) => r.adopted === true).length, 1);
  h.close();
});

test('compat: toolset_close finds the instance itself, and compat arguments are validated', async () => {
  const opened: string[] = [];
  const server = new ToolsetServer(boxSpec(opened));
  const h = await Host.connect(server, 'p', { hints: false });
  const bad = await h.call('toolset_open', { toolset: 'box', extra: 1 });
  assert.equal(bad.isError, true);
  assert.match(text(bad), /Nothing was done/);
  await h.call('toolset_open', { toolset: 'box', instance: 'a' });
  const closed = await h.call('toolset_close', { toolset: 'box' });
  assert.equal(closed.isError, undefined);
  assert.match(text(closed), /Closed box \(a\)/);
  h.close();
});

test('an empty toolsets/list still carries its statement and takes a rev', async () => {
  const server = new ToolsetServer(lampSpec());
  const h = await Host.connect(server, 'p');
  const list = await h.req('toolsets/list', {});
  const hc = (list._meta as Record<string, unknown>)['mcpl/hints'] as HintChanges;
  assert.equal(hc.rev, 1);
  assert.deepEqual(hc.tools, []);
  h.close();
});
