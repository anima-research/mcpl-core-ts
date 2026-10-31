import * as vm from 'node:vm';
import { describe, it, type TestContext } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import * as net from 'node:net';
import { PassThrough, Readable, Writable } from 'node:stream';

import {
  McplConnection,
  textContent,
  method,
  advertisedCapabilitiesFromInitialize,
  ERR_CHECKPOINT_NOT_FOUND,
  ERR_CAPABILITY_DENIED,
  ConnectionClosedError,
  ConnectionTimeoutError,
  RpcError,
} from '../src/index.js';

import type {
  McplInitializeParams,
  McplInitializeResult,
  McplCapabilities,
  FeatureSetDeclaration,
  FeatureSetsUpdateParams,
  PushEventParams,
  PushEventResult,
  ChannelsRegisterParams,
  ChannelsOpenParams,
  ChannelsOpenResult,
  ChannelDescriptor,
} from '../src/index.js';

async function connectedPair(): Promise<[McplConnection, McplConnection]> {
  const server = net.createServer();
  server.listen(0, '127.0.0.1');

  await new Promise<void>((resolve) => server.once('listening', resolve));
  const addr = server.address() as net.AddressInfo;

  const [serverConn, clientSocket] = await Promise.all([
    McplConnection.acceptTcp(server),
    new Promise<net.Socket>((resolve, reject) => {
      const socket = net.createConnection({ host: '127.0.0.1', port: addr.port }, () => {
        resolve(socket);
      });
      socket.once('error', reject);
    }),
  ]);

  const clientConn = McplConnection.fromTcp(clientSocket);

  server.close();

  return [clientConn, serverConn];
}

const turn = () => new Promise((resolve) => setImmediate(resolve));

function fakeWebSocket(readyState = 1) {
  const ws = Object.assign(new EventEmitter(), { readyState, sent: [] as string[], closeCalls: 0 });
  return Object.assign(ws, {
    send(data: string) { ws.sent.push(data); },
    close() { ws.closeCalls++; },
  });
}
const overWebSocket = (ws: ReturnType<typeof fakeWebSocket>) =>
  McplConnection.fromWebSocket(ws as unknown as Parameters<typeof McplConnection.fromWebSocket>[0]);

function heard(conn: McplConnection): { methods: string[]; params: unknown[]; errors: string[] } {
  const seen = { methods: [] as string[], params: [] as unknown[], errors: [] as string[] };
  conn.on('notification', (n) => {
    seen.methods.push(n.method);
    seen.params.push(n.params);
  });
  conn.on('error', (e) => seen.errors.push(e.message));
  return seen;
}

describe('McplConnection', () => {
  it('capability negotiation', async () => {
    const [client, server] = await connectedPair();

    const clientCaps: McplCapabilities = {
      version: '0.5',
      pushEvents: true,
      channels: { register: true, publish: true, incoming: true },
      rollback: true,
    };

    const initParams: McplInitializeParams = {
      protocolVersion: '2024-11-05',
      capabilities: {
        experimental: { mcpl: clientCaps },
      },
      clientInfo: { name: 'test-client', version: '0.1.0' },
    };

    const clientPromise = client.sendRequest('initialize', initParams);

    const msg = await server.nextMessage();
    assert.equal(msg.type, 'request');
    if (msg.type !== 'request') throw new Error('unreachable');
    assert.equal(msg.request.method, 'initialize');

    const params = msg.request.params as McplInitializeParams;
    assert.equal(params.clientInfo.name, 'test-client');

    const serverCaps: McplCapabilities = {
      version: '0.5',
      pushEvents: true,
      channels: { register: true, publish: true, incoming: true },
      rollback: true,
      featureSets: {
        lobby: {
          description: 'Lobby operations',
          uses: ['channels.register', 'channels.publish'],
          rollback: false,
        },
        game: {
          description: 'Game operations',
          uses: ['pushEvents', 'tools'],
          rollback: true,
        },
      },
    };

    const result: McplInitializeResult = {
      protocolVersion: '2024-11-05',
      capabilities: {
        experimental: { mcpl: serverCaps },
      },
      serverInfo: { name: 'test-server', version: '0.1.0' },
    };

    server.sendResponse(msg.request.id, result);

    const initResult = (await clientPromise) as McplInitializeResult;
    assert.equal(initResult.serverInfo.name, 'test-server');

    const mcpl = initResult.capabilities.experimental?.mcpl;
    assert.ok(mcpl);
    assert.equal(mcpl!.pushEvents, true);
    assert.deepEqual(mcpl!.channels, { register: true, publish: true, incoming: true });
    assert.equal(mcpl!.rollback, true);

    const featureSets = mcpl!.featureSets;
    assert.ok(featureSets && typeof featureSets === 'object');
    const declared = featureSets as Record<string, FeatureSetDeclaration>;
    assert.deepEqual(Object.keys(declared).sort(), ['game', 'lobby']);
    assert.equal(declared.lobby!.rollback, false);
    assert.equal(declared.game!.rollback, true);

    assert.ok(advertisedCapabilitiesFromInitialize(initResult.capabilities).has('channels.publish'));
    assert.equal(
      advertisedCapabilitiesFromInitialize(initResult.capabilities).has('channels.streaming'),
      false,
    );

    client.close();
    server.close();
  });

  it('notification roundtrip', async () => {
    const [client, server] = await connectedPair();

    const params: FeatureSetsUpdateParams = {
      enabled: ['lobby', 'game'],
    };

    client.sendNotification(method.FEATURE_SETS_UPDATE, params);

    const msg = await server.nextMessage();
    assert.equal(msg.type, 'notification');
    if (msg.type !== 'notification') throw new Error('unreachable');
    assert.equal(msg.notification.method, 'featureSets/update');

    const p = msg.notification.params as FeatureSetsUpdateParams;
    assert.deepEqual(p.enabled, ['lobby', 'game']);

    client.close();
    server.close();
  });

  it('push event request', async () => {
    const [client, server] = await connectedPair();

    const eventParams: PushEventParams = {
      featureSet: 'lobby',
      eventId: 'evt_001',
      timestamp: '2026-02-12T00:00:00Z',
      payload: {
        content: [textContent('User joined lobby')],
      },
    };

    const serverPromise = server.sendRequest(method.PUSH_EVENT, eventParams);

    const msg = await client.nextMessage();
    assert.equal(msg.type, 'request');
    if (msg.type !== 'request') throw new Error('unreachable');
    assert.equal(msg.request.method, 'push/event');

    const p = msg.request.params as PushEventParams;
    assert.equal(p.featureSet, 'lobby');
    assert.equal(p.eventId, 'evt_001');

    const pushResult: PushEventResult = {
      accepted: true,
      inferenceId: 'inf_001',
    };
    client.sendResponse(msg.request.id, pushResult);

    const result = (await serverPromise) as PushEventResult;
    assert.equal(result.accepted, true);
    assert.equal(result.inferenceId, 'inf_001');

    client.close();
    server.close();
  });

  it('channel lifecycle', async () => {
    const [client, server] = await connectedPair();

    const regParams: ChannelsRegisterParams = {
      channels: [
        {
          id: 'game',
          type: 'game_instance',
          label: 'Game Instances',
          direction: 'bidirectional',
        },
      ],
    };

    const serverRegPromise = server.sendRequest(method.CHANNELS_REGISTER, regParams);

    const regMsg = await client.nextMessage();
    assert.equal(regMsg.type, 'request');
    if (regMsg.type !== 'request') throw new Error('unreachable');
    assert.equal(regMsg.request.method, 'channels/register');

    const regP = regMsg.request.params as ChannelsRegisterParams;
    assert.equal(regP.channels.length, 1);
    assert.equal(regP.channels[0].id, 'game');

    client.sendResponse(regMsg.request.id, {});
    await serverRegPromise;

    const openParams: ChannelsOpenParams = {
      channelId: 'game',
      type: 'game_instance',
      address: { map: 'DeltaSiegeDry', mod: 'Zero-K v1.12' },
      history: { limit: 5, beforeMessageId: 'invite-1' },
    };

    const clientOpenPromise = client.sendRequest(method.CHANNELS_OPEN, openParams);

    const openMsg = await server.nextMessage();
    assert.equal(openMsg.type, 'request');
    if (openMsg.type !== 'request') throw new Error('unreachable');
    assert.equal(openMsg.request.method, 'channels/open');
    assert.deepEqual(
      (openMsg.request.params as unknown as ChannelsOpenParams).history,
      { limit: 5, beforeMessageId: 'invite-1' },
    );

    const openResult: ChannelsOpenResult = {
      channel: {
        id: 'game:live-1',
        type: 'game_instance',
        label: 'Live Game 1',
        direction: 'bidirectional',
        address: { map: 'DeltaSiegeDry' },
      },
      history: [{
        channelId: 'game:live-1',
        messageId: 'prior-1',
        author: { id: 'u1', name: 'Alice' },
        timestamp: new Date(0).toISOString(),
        content: [textContent('prior context')],
      }],
    };

    server.sendResponse(openMsg.request.id, openResult);

    const result = (await clientOpenPromise) as ChannelsOpenResult;
    assert.equal(result.channel.id, 'game:live-1');
    assert.equal(result.channel.label, 'Live Game 1');
    assert.equal(result.history?.[0].messageId, 'prior-1');

    const ackPromise = client.sendRequest(method.CHANNELS_ACKNOWLEDGE, {
      channelId: 'game:live-1', messageId: 'invite-1', intent: 'seen-not-opening', value: '👀',
    });
    const ackMsg = await server.nextMessage();
    assert.equal(ackMsg.type, 'request');
    if (ackMsg.type !== 'request') throw new Error('unreachable');
    assert.equal(ackMsg.request.method, 'channels/acknowledge');
    server.sendResponse(ackMsg.request.id, { acknowledged: true, representation: '👀' });
    assert.deepEqual(await ackPromise, { acknowledged: true, representation: '👀' });

    client.close();
    server.close();
  });

  it('error response', async () => {
    const [client, server] = await connectedPair();

    const clientPromise = client.sendRequest(method.STATE_ROLLBACK, {
      featureSet: 'game',
      checkpoint: 'nonexistent',
    });

    const msg = await server.nextMessage();
    assert.equal(msg.type, 'request');
    if (msg.type !== 'request') throw new Error('unreachable');

    server.sendError(msg.request.id, ERR_CHECKPOINT_NOT_FOUND, 'Checkpoint not found');

    try {
      await assert.rejects(clientPromise, (err: Error) => {
        assert.ok(err instanceof RpcError, 'an RpcError');
        assert.equal(err.code, ERR_CHECKPOINT_NOT_FOUND);
        assert.equal(err.message, 'Checkpoint not found');
        assert.equal((err as unknown as { data?: unknown }).data, undefined);
        return true;
      });
    } finally {
      client.close();
      server.close();
    }
  });

  it('error response with the SPEC §6.6 data member', async () => {
    const [client, server] = await connectedPair();

    const clientPromise = client.sendRequest(method.CHANNELS_PUBLISH, {
      conversationId: 'c1', channelId: 'game:live-1', content: [],
    });

    const msg = await server.nextMessage();
    assert.equal(msg.type, 'request');
    if (msg.type !== 'request') throw new Error('unreachable');

    server.sendError(msg.request.id, ERR_CAPABILITY_DENIED, 'Capability denied', { capability: 'channels.publish' });

    await assert.rejects(clientPromise, (err: Error) => {
      assert.ok(err instanceof RpcError, 'an RpcError');
      assert.deepEqual(err.data, { capability: 'channels.publish' });
      return true;
    });

    client.close();
    server.close();
  });

  it('content block serialization', async () => {
    const [client, server] = await connectedPair();

    const blocks = [
      { type: 'text' as const, text: 'Hello' },
      { type: 'image' as const, data: 'base64data', mimeType: 'image/png' },
      { type: 'resource' as const, uri: 'memory://facts/12345' },
    ];

    client.sendNotification('test/content', { content: blocks });

    const msg = await server.nextMessage();
    assert.equal(msg.type, 'notification');
    if (msg.type !== 'notification') throw new Error('unreachable');

    const p = msg.notification.params as { content: typeof blocks };
    assert.equal(p.content.length, 3);
    assert.equal(p.content[0].type, 'text');
    assert.equal((p.content[0] as { text: string }).text, 'Hello');
    assert.equal(p.content[1].type, 'image');
    assert.equal((p.content[1] as { data: string }).data, 'base64data');
    assert.equal((p.content[1] as { mimeType: string }).mimeType, 'image/png');
    assert.equal(p.content[2].type, 'resource');
    assert.equal((p.content[2] as { uri: string }).uri, 'memory://facts/12345');

    client.close();
    server.close();
  });

  it('request timeout: a ConnectionTimeoutError, still an RpcError with code -32000, that says the outcome is unknown', { timeout: 5000 }, async () => {
    const [client, server] = await connectedPair();

    const promise = client.sendRequest('test/slow', {}, 50);

    try {
      await assert.rejects(promise, (err: Error) => {
        assert.ok(err instanceof ConnectionTimeoutError, 'a timeout is told apart by its class');
        assert.ok(err instanceof RpcError, 'a caller that catches RpcError still catches it');
        assert.equal(err.code, -32000);
        assert.equal((err as ConnectionTimeoutError).outcome, 'no-response', 'it may have run, as a close before the answer says');
        assert.ok(err.message.includes('test/slow') && err.message.includes('50'), 'the method and the timeout are in its message');
        return true;
      });
    } finally {
      client.close();
      server.close();
    }
  });

  describe('what a failure says', () => {
    it('a request on a closed connection says nothing was sent, so the call did not run', async () => {
      const toClient = new PassThrough();
      const fromClient = new PassThrough();
      const client = McplConnection.fromStreams(toClient, fromClient);
      const written: string[] = [];
      const write = fromClient.write.bind(fromClient);
      fromClient.write = ((chunk: unknown, ...rest: unknown[]) => {
        written.push(String(chunk));
        return (write as (...a: unknown[]) => boolean)(chunk, ...rest);
      }) as typeof fromClient.write;
      client.close();

      await assert.rejects(client.sendRequest('test/after-close', {}), (err: Error) => {
        assert.ok(err instanceof ConnectionClosedError);
        assert.equal(err.outcome, 'not-sent');
        assert.match(err.message, /nothing was sent/i);
        assert.match(err.message, /closed/, "mcpl-harness's fallback reads 'closed'");
        return true;
      });
      await turn();
      assert.deepEqual(written, [], 'nothing reached the output');
    });

    it('at a close, a pending request learns its outcome is unknown, and a waiting reader that no more messages will come', { timeout: 5000 }, async () => {
      const [client, server] = await connectedPair();
      const pending = client.sendRequest('test/slow', {}, 0);
      const waiting = client.nextMessage();
      await server.nextMessage();
      client.close();

      try {
        await assert.rejects(pending, (err: Error) => {
          assert.ok(err instanceof ConnectionClosedError);
          assert.equal(err.outcome, 'no-response');
          assert.match(err.message, /answer/, 'it says the call got no answer');
          return true;
        });
        await assert.rejects(waiting, (err: Error) => {
          assert.ok(err instanceof ConnectionClosedError);
          assert.equal(err.outcome, undefined, 'a reader made no call');
          assert.match(err.message, /no more messages will come/);
          return true;
        });
      } finally {
        server.close();
      }
    });

    it('a malformed error response settles its request as an RpcError with a valid code, and the other end\'s message or a sentence with no code in front', { timeout: 5000 }, async () => {
      const toClient = new PassThrough();
      const fromClient = new PassThrough();
      const client = McplConnection.fromStreams(toClient, fromClient);
      // JSON.parse reads this nesting. Whether JSON.stringify can write it back
      // depends on the runtime's stack.
      const deep = '['.repeat(100_000) + ']'.repeat(100_000);
      const rows: Array<{ error: string; code: number; own?: string }> = [
        { error: '{"code":-32601,"message":"Method not found"}', code: -32601, own: 'Method not found' },
        { error: '{"code":-32603,"message":""}', code: -32603 },
        { error: '{"code":-32001}', code: -32001 },
        { error: '{"code":-32002,"message":null}', code: -32002 },
        { error: '{"code":-32603,"message":[]}', code: -32603 },
        { error: '{"code":-32000,"message":{"detail":"quota"}}', code: -32000 },
        // A message String() can't convert.
        { error: '{"code":-32000,"message":{"toString":1,"valueOf":1}}', code: -32000 },
        { error: '{"message":""}', code: -32603 },
        { error: '{"message":[1]}', code: -32603 },
        { error: '{"code":"abc"}', code: -32603 },
        { error: '"boom"', code: -32603 },
        { error: '[]', code: -32603 },
        { error: '0', code: -32603 },
        // JSON.parse reads 1e999 as Infinity, which JSON writes back as null.
        { error: '{"code":1e999,"message":"boom"}', code: -32603, own: 'boom' },
        { error: '{"code":1e999}', code: -32603 },
        { error: '{"code":1.5}', code: -32603 },
        { error: '{"code":1.5,"message":{"x":1}}', code: -32603 },
        { error: '{"code":-32000,"message":1e999}', code: -32000 },
        { error: '-1e999', code: -32603 },
        { error: `{"code":-32000,"message":${deep}}`, code: -32000 },
        { error: deep, code: -32603 },
        { error: `{"code":${deep}}`, code: -32603 },
      ];
      const requests = rows.map((_, i) => client.sendRequest(`test/odd-${i}`, {}, 2000));
      rows.forEach(({ error }, i) => toClient.write(`{"jsonrpc":"2.0","id":${i + 1},"error":${error}}\n`));
      try {
        for (const [i, { error, code, own }] of rows.entries()) {
          await assert.rejects(requests[i], (err: Error) => {
            const row = error.slice(0, 60);
            assert.ok(err instanceof RpcError, row);
            assert.ok(!(err instanceof ConnectionTimeoutError), `settled by the answer, not the timeout: ${row}`);
            assert.equal(err.code, code, row);
            if (own !== undefined) assert.equal(err.message, own, row);
            else {
              assert.ok(typeof err.message === 'string' && err.message.length > 0, `a message that isn't empty: ${row}`);
              assert.ok(!err.message.startsWith(String(code)), `no code in front of it: ${row}`);
            }
            return true;
          });
        }
      } finally {
        client.close();
      }
    });

    it('a request whose params can\'t be written as JSON is refused with a TypeError, and nothing is written', async () => {
      const toClient = new PassThrough();
      const fromClient = new PassThrough();
      const client = McplConnection.fromStreams(toClient, fromClient);
      const written: string[] = [];
      fromClient.on('data', (chunk) => written.push(String(chunk)));
      const OtherError = vm.runInNewContext('Error') as ErrorConstructor;
      const unwritable: Array<[string, unknown]> = [
        ['a BigInt', { n: 1n }],
        ["a toJSON that throws another realm's Error", { value: { toJSON() { throw new OtherError('from another realm'); } } }],
        ["a toJSON that throws something that isn't an Error", { value: { toJSON() { throw 'no'; } } }],
      ];
      try {
        for (const [what, params] of unwritable) {
          await assert.rejects(client.sendRequest('test/unwritable', params, 0), (err: Error) => {
            assert.ok(err instanceof TypeError, what);
            return true;
          });
        }
        await turn();
        assert.deepEqual(written, [], 'nothing was written');
      } finally {
        client.close();
      }
    });

    it('a response whose id is neither a number nor a string answers no request', { timeout: 5000 }, async () => {
      const toClient = new PassThrough();
      const fromClient = new PassThrough();
      const client = McplConnection.fromStreams(toClient, fromClient);
      const request = client.sendRequest('test/ids', {}, 2000);
      // An object String() can't convert, and an array String() would turn into "1".
      toClient.write(JSON.stringify({ jsonrpc: '2.0', id: { toString: 1, valueOf: 1 }, error: { code: -32000, message: 'x' } }) + '\n');
      toClient.write(JSON.stringify({ jsonrpc: '2.0', id: [1], error: { code: -32000, message: 'array id' } }) + '\n');
      toClient.write(JSON.stringify({ jsonrpc: '2.0', id: 1, result: 'answered' }) + '\n');
      try {
        assert.equal(await request, 'answered', 'only the answer with the id that was sent settles it');
      } finally {
        client.close();
      }
    });

    it('a request whose output throws at once rejects with that error', async () => {
      const toClient = new PassThrough();
      const fromClient = new PassThrough();
      const client = McplConnection.fromStreams(toClient, fromClient);
      const failure = new Error('the output refused the write');
      fromClient.write = () => { throw failure; };
      try {
        await assert.rejects(client.sendRequest('test/throwing-output', {}, 60_000), (err: Error) => err === failure);
      } finally {
        client.close();
      }
    });

    it('a timeout past setTimeout\'s limit runs as that limit, and its error names the wait that ran', async (t) => {
      t.mock.timers.enable({ apis: ['setTimeout'] });
      const toClient = new PassThrough();
      const fromClient = new PassThrough();
      const client = McplConnection.fromStreams(toClient, fromClient);
      try {
        const request = client.sendRequest('test/forever', {}, Number.MAX_SAFE_INTEGER);
        t.mock.timers.tick(2 ** 31 - 1);
        const settled = Promise.race([request, turn().then((): never => { throw new Error('still pending after the limit'); })]);
        await assert.rejects(settled, (err: Error) => {
          assert.ok(err instanceof ConnectionTimeoutError);
          assert.ok(err.message.includes('test/forever') && err.message.includes('2147483647'), 'the method and the wait that ran are in its message');
          return true;
        });
      } finally {
        client.close();
      }
    });

    it('an RpcError keeps the message it is given, so passing one on adds nothing to it', () => {
      const err = new RpcError(-32602, 'bad params', { field: 'x' });
      assert.equal(err.message, 'bad params');
      assert.equal(err.code, -32602);
      assert.deepEqual(err.data, { field: 'x' });
      const passedOn = new RpcError(err.code, err.message, err.data);
      assert.equal(passedOn.message, 'bad params');
    });
  });

  it('emits error on malformed JSON, and stays open', async () => {
    const { PassThrough } = await import('node:stream');

    const aToB = new PassThrough();
    const bToA = new PassThrough();

    const conn = McplConnection.fromStreams(aToB, bToA);

    const errors: Error[] = [];
    conn.on('error', (err) => errors.push(err));
    const reported = new Promise((resolve) => conn.once('error', resolve));

    aToB.write('this is not json\n');

    await reported;
    await new Promise((resolve) => setImmediate(resolve));

    assert.equal(errors.length, 1);
    assert.equal(conn.isClosed, false, 'a malformed line leaves the connection open');

    aToB.write(JSON.stringify({ jsonrpc: '2.0', method: 'test/after', params: {} }) + '\n');
    const next = await conn.nextMessage();
    assert.equal(next.type === 'notification' && next.notification.method, 'test/after');

    conn.close();
  });

  describe("a listener's exception", () => {
    const requestLine = JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'test/request', params: {} }) + '\n';
    const notificationLine = JSON.stringify({ jsonrpc: '2.0', method: 'test/after', params: {} }) + '\n';

    it('is reported as the error it threw; a nextMessage() call already waiting gets the message, and the connection stays open', { timeout: 5000 }, async () => {
      const input = new PassThrough();
      const conn = McplConnection.fromStreams(input, new PassThrough());
      const thrown = new Error('the listener failed');
      const errors: unknown[] = [];
      conn.on('error', (e) => errors.push(e));
      const later: string[] = [];
      conn.on('request', () => { throw thrown; });
      conn.on('request', (r) => later.push(r.method));

      const waiting = conn.nextMessage();
      input.write(requestLine);
      const msg = await waiting;
      assert.equal(msg.type === 'request' && msg.request.method, 'test/request', 'the waiting call gets the message');
      assert.equal(errors.length, 1);
      assert.strictEqual(errors[0], thrown, 'reported as itself, not as a malformed message');
      assert.deepEqual(later, [], "the listeners after the one that threw don't hear it");
      assert.equal(conn.isClosed, false);

      input.write(notificationLine);
      const next = await conn.nextMessage();
      assert.equal(next.type === 'notification' && next.notification.method, 'test/after');
      conn.close();
    });

    it('over a WebSocket, is reported as itself, and a later nextMessage() returns the message', { timeout: 5000 }, async () => {
      const ws = fakeWebSocket();
      const conn = overWebSocket(ws);
      const thrown = new TypeError('a handler bug');
      const errors: unknown[] = [];
      conn.on('error', (e) => errors.push(e));
      conn.on('notification', () => { throw thrown; });

      ws.emit('message', notificationLine.trim());
      await turn();
      assert.equal(errors.length, 1);
      assert.strictEqual(errors[0], thrown);
      const msg = await conn.nextMessage();
      assert.equal(msg.type === 'notification' && msg.notification.method, 'test/after');
      conn.close();
    });

    it('with no error listener, throws from the input as it was thrown, and the message is still queued', { timeout: 5000 }, async () => {
      const input = new PassThrough();
      const conn = McplConnection.fromStreams(input, new PassThrough());
      const thrown = new Error('nobody listens for errors');
      conn.on('notification', () => { throw thrown; });
      assert.throws(() => input.emit('data', Buffer.from(notificationLine)), (e: unknown) => e === thrown);
      const msg = await conn.nextMessage();
      assert.equal(msg.type === 'notification' && msg.notification.method, 'test/after');
      conn.close();
    });

    it('thrown after the listener closed the connection is still reported, after the close', { timeout: 5000 }, async () => {
      const input = new PassThrough();
      const conn = McplConnection.fromStreams(input, new PassThrough());
      const thrown = new Error('closed, then failed');
      const events: string[] = [];
      conn.on('close', () => events.push('close'));
      conn.on('error', (e) => events.push(e === thrown ? 'error: thrown' : `error: ${(e as Error).message}`));
      conn.on('notification', () => { conn.close(); throw thrown; });

      const waiting = conn.nextMessage();
      input.write(notificationLine);
      const msg = await waiting;
      assert.equal(msg.type === 'notification' && msg.notification.method, 'test/after', 'the waiting call gets the message');
      await turn();
      assert.deepEqual(events, ['close', 'error: thrown']);
      await assert.rejects(conn.nextMessage(), { name: 'ConnectionClosedError' });
    });

    it("that isn't an Error is the cause of the Error emitted; an Error from another realm is emitted as itself", { timeout: 5000 }, async () => {
      const foreign = vm.runInNewContext("new Error('from another realm')") as Error;
      const bare = Object.create(null) as object;
      for (const thrown of ['a string', bare, foreign] as unknown[]) {
        const input = new PassThrough();
        const conn = McplConnection.fromStreams(input, new PassThrough());
        const errors: unknown[] = [];
        conn.on('error', (e) => errors.push(e));
        conn.on('notification', () => { throw thrown; });
        input.write(notificationLine);
        const msg = await conn.nextMessage();
        assert.equal(msg.type === 'notification' && msg.notification.method, 'test/after');
        assert.equal(errors.length, 1);
        if (thrown === foreign) {
          assert.strictEqual(errors[0], foreign, 'an Error from another realm is emitted as itself');
        } else {
          assert.ok(errors[0] instanceof Error, 'an Error is emitted');
          assert.strictEqual((errors[0] as Error).cause, thrown, 'holding what was thrown as its cause');
        }
        conn.close();
      }
    });

    it('with no error listener, leaves the messages behind it to a later turn, in order', { timeout: 5000 }, async () => {
      const input = new PassThrough();
      const conn = McplConnection.fromStreams(input, new PassThrough());
      const thrown = new Error('the first one fails');
      const methods: string[] = [];
      conn.on('notification', (n) => {
        methods.push(n.method);
        if (n.method === 'test/first') throw thrown;
      });
      const line = (m: string) => JSON.stringify({ jsonrpc: '2.0', method: m, params: {} }) + '\n';
      assert.throws(() => input.emit('data', Buffer.from(line('test/first') + line('test/second'))), (e: unknown) => e === thrown);
      assert.deepEqual(methods, ['test/first'], 'the rest waits for a later turn');
      await turn();
      assert.deepEqual(methods, ['test/first', 'test/second']);
      const pulled = [await conn.nextMessage(), await conn.nextMessage()]
        .map((m) => m.type === 'notification' ? m.notification.method : '');
      assert.deepEqual(pulled, ['test/first', 'test/second'], 'both reach nextMessage(), in order');
      conn.close();
    });

    it("whose 'error' listener throws in turn surfaces that throw from the input, with the message still queued", { timeout: 5000 }, async () => {
      const input = new PassThrough();
      const conn = McplConnection.fromStreams(input, new PassThrough());
      const reportFailed = new Error('the error listener failed');
      conn.on('error', () => { throw reportFailed; });
      conn.on('notification', () => { throw new Error('the listener failed'); });
      assert.throws(() => input.emit('data', Buffer.from(notificationLine)), (e: unknown) => e === reportFailed);
      const msg = await conn.nextMessage();
      assert.equal(msg.type === 'notification' && msg.notification.method, 'test/after');
      conn.close();
    });
  });

  it('a listener that calls nextMessage() itself gets the message being dispatched', { timeout: 5000 }, async () => {
    const input = new PassThrough();
    const conn = McplConnection.fromStreams(input, new PassThrough());
    let inner: Promise<unknown> | undefined;
    conn.on('notification', () => { inner ??= conn.nextMessage(); });
    input.write(JSON.stringify({ jsonrpc: '2.0', method: 'test/dispatched', params: {} }) + '\n');
    await turn();
    assert.ok(inner, 'the listener ran');
    const msg = await inner as Awaited<ReturnType<McplConnection['nextMessage']>>;
    assert.equal(msg.type === 'notification' && msg.notification.method, 'test/dispatched');
    conn.close();
  });

  it('a JSON value that is not an object, null included, is not a message: nothing is reported, and the next line is taken', { timeout: 5000 }, async () => {
    const input = new PassThrough();
    const conn = McplConnection.fromStreams(input, new PassThrough());
    const errors: unknown[] = [];
    conn.on('error', (e) => errors.push(e));
    input.write('null\n5\n"text"\n[]\n' + JSON.stringify({ jsonrpc: '2.0', method: 'test/after', params: {} }) + '\n');
    const msg = await conn.nextMessage();
    assert.equal(msg.type === 'notification' && msg.notification.method, 'test/after');
    assert.deepEqual(errors, []);
    conn.close();
  });

  describe('a transport that ends or fails closes the connection', () => {
    async function connectionAndSocket(): Promise<[McplConnection, net.Socket]> {
      const listener = net.createServer();
      listener.listen(0, '127.0.0.1');
      await new Promise<void>((resolve) => listener.once('listening', resolve));
      const { port } = listener.address() as net.AddressInfo;
      const [conn, socket] = await Promise.all([
        McplConnection.acceptTcp(listener),
        new Promise<net.Socket>((resolve, reject) => {
          const s = net.createConnection({ host: '127.0.0.1', port }, () => resolve(s));
          s.once('error', reject);
        }),
      ]);
      listener.close();
      return [conn, socket];
    }

    function watch(conn: McplConnection): { errors: Error[]; closes: () => number; closed: Promise<void>; order: string[] } {
      const errors: Error[] = [];
      const order: string[] = [];
      let closes = 0;
      conn.on('error', (err) => {
        errors.push(err);
        order.push('error');
      });
      const closed = new Promise<void>((resolve) => {
        conn.on('close', () => {
          closes++;
          order.push('close');
          resolve();
        });
      });
      return { errors, closes: () => closes, closed, order };
    }

    const withinATurn = <T>(p: Promise<T>): Promise<T> =>
      Promise.race([p, turn().then((): never => { throw new Error('still pending after a turn'); })]);

    const line = (m: string) => JSON.stringify({ jsonrpc: '2.0', method: m, params: {} }) + '\n';

    function capture(output: PassThrough): () => string[] {
      const chunks: string[] = [];
      output.on('data', (chunk) => chunks.push(String(chunk)));
      return () => chunks.join('').split('\n').filter(Boolean).map((text) => {
        const msg = JSON.parse(text) as { method?: string; id?: unknown };
        return msg.method ?? `response ${String(msg.id)}`;
      });
    }

    function sendAndRead(conn: McplConnection): { request: Promise<unknown>; read: Promise<unknown> } {
      const request = conn.sendRequest('test/side-effect', {}, 0);
      conn.sendNotification('test/cleanup', {});
      conn.sendResponse(1, { answered: 'after the close' });
      conn.sendError(2, -32000, 'after the close');
      return { request, read: conn.nextMessage() };
    }

    it('a reset from the peer: one error, then closed, with both waiters and both pending requests rejected', { timeout: 5000 }, async () => {
      const [conn, socket] = await connectionAndSocket();
      const seen = watch(conn);
      const waiting = [conn.nextMessage(), conn.nextMessage()];
      const pending = [conn.sendRequest('test/slow', {}, 0), conn.sendRequest('test/slower', {}, 0)];
      socket.resetAndDestroy();

      await seen.closed;
      assert.equal(conn.isClosed, true);
      for (const p of [...waiting, ...pending]) await assert.rejects(p, { name: 'ConnectionClosedError' });
      await turn();
      assert.equal(seen.errors.length, 1, 'the reset is reported once, though the socket is both streams');
      assert.equal((seen.errors[0] as NodeJS.ErrnoException).code, 'ECONNRESET');
      assert.deepEqual(seen.order, ['error', 'close'], 'the error is reported, then the close');
    });

    it('an input destroyed with an error: one error, then closed', { timeout: 5000 }, async () => {
      const input = new PassThrough();
      const conn = McplConnection.fromStreams(input, new PassThrough());
      const seen = watch(conn);
      const waiting = conn.nextMessage();
      input.destroy(new Error('boom'));

      await seen.closed;
      await assert.rejects(waiting, { name: 'ConnectionClosedError' });
      await turn();
      assert.deepEqual(seen.errors.map((e) => e.message), ['boom']);
      assert.deepEqual(seen.order, ['error', 'close'], 'the error is reported, then the close');
    });

    it('an input destroyed without an error: closed, and no error', { timeout: 5000 }, async () => {
      const input = new PassThrough();
      const conn = McplConnection.fromStreams(input, new PassThrough());
      const seen = watch(conn);
      const waiting = conn.nextMessage();
      input.destroy();

      await seen.closed;
      await assert.rejects(waiting, { name: 'ConnectionClosedError' });
      await turn();
      assert.equal(seen.errors.length, 0);
      assert.equal(seen.closes(), 1);
    });

    it('an input that ends: closed once, and no error, when its close follows', { timeout: 5000 }, async () => {
      const input = new PassThrough();
      const conn = McplConnection.fromStreams(input, new PassThrough());
      const seen = watch(conn);
      const waiting = conn.nextMessage();
      const inputClosed = new Promise((resolve) => input.once('close', resolve));
      input.end();

      await seen.closed;
      await assert.rejects(waiting, { name: 'ConnectionClosedError' });
      await inputClosed;
      await turn();
      assert.equal(seen.errors.length, 0);
      assert.equal(seen.closes(), 1, "the input's close after its end doesn't close the connection again");
    });

    it('an input that emits an error and never closes, with no error listener: the error is thrown, and the connection still closes', { timeout: 5000 }, async () => {
      // A stream that reports an error without destroying itself sends no
      // 'close'; and with no 'error' listener, reporting the error throws.
      const input = new PassThrough({ autoDestroy: false, emitClose: false });
      const conn = McplConnection.fromStreams(input, new PassThrough());
      let closes = 0;
      const closed = new Promise((resolve) => conn.on('close', () => { closes++; resolve(undefined); }));
      const waiting = conn.nextMessage();

      assert.throws(() => input.emit('error', new Error('unheard')), { message: 'unheard' });
      assert.equal(conn.isClosed, true);
      await assert.rejects(waiting, { name: 'ConnectionClosedError' });
      await closed;
      await turn();
      assert.equal(closes, 1);
    });

    it('an output that fails: one error, then closed, and the input is no longer read', { timeout: 5000 }, async () => {
      const input = new PassThrough();
      const output = new PassThrough();
      const conn = McplConnection.fromStreams(input, output);
      const seen = watch(conn);
      const waiting = conn.nextMessage();
      const pending = conn.sendRequest('test/slow', {}, 0);
      output.destroy(new Error('EPIPE'));

      await seen.closed;
      await assert.rejects(waiting, { name: 'ConnectionClosedError' });
      await assert.rejects(pending, { name: 'ConnectionClosedError' });
      await turn();
      assert.deepEqual(seen.errors.map((e) => e.message), ['EPIPE']);
      assert.deepEqual(seen.order, ['error', 'close'], 'the error is reported, then the close');
      const heardAfter: string[] = [];
      conn.on('notification', (n) => heardAfter.push(n.method));
      input.write(line('test/after-close'));
      await turn();
      assert.deepEqual(heardAfter, [], 'a closed connection reads nothing more from its input');
    });

    it('an output destroyed without an error: closed, and no error', { timeout: 5000 }, async () => {
      const output = new PassThrough();
      const conn = McplConnection.fromStreams(new PassThrough(), output);
      const seen = watch(conn);
      const waiting = conn.nextMessage();
      output.destroy();

      await seen.closed;
      await assert.rejects(waiting, { name: 'ConnectionClosedError' });
      await turn();
      assert.equal(seen.errors.length, 0);
      assert.equal(seen.closes(), 1);
    });

    it("a request sent between the output's destroy() and its 'close' writes nothing and rejects", { timeout: 5000 }, async () => {
      const output = new PassThrough();
      const conn = McplConnection.fromStreams(new PassThrough(), output);
      const writes: unknown[] = [];
      const write = output.write.bind(output);
      output.write = ((chunk: unknown, ...rest: unknown[]) => {
        writes.push(chunk);
        return (write as (...args: unknown[]) => boolean)(chunk, ...rest);
      }) as typeof output.write;
      output.destroy();
      const request = conn.sendRequest('test/x', {});
      await assert.rejects(request, { name: 'ConnectionClosedError' });
      assert.deepEqual(writes, []);
    });

    it('an output ended on purpose leaves the input to finish, and sends nothing more', { timeout: 5000 }, async () => {
      const input = new PassThrough();
      const output = new PassThrough();
      const sent = capture(output); // the peer reads what was sent, so the output can finish
      const conn = McplConnection.fromStreams(input, output);
      const seen = watch(conn);
      const outputClosed = new Promise((resolve) => output.once('close', resolve));
      output.end();

      const beforeFinish = conn.sendRequest('test/before-finish', {}, 0);
      conn.sendNotification('test/before-finish', {});
      await assert.rejects(withinATurn(beforeFinish), (err: Error) => {
        assert.ok(err instanceof ConnectionClosedError);
        assert.equal(err.outcome, 'not-sent');
        assert.match(err.message, /nothing was sent/i);
        return true;
      });
      await outputClosed;
      await turn();
      assert.equal(conn.isClosed, false, 'still open: the peer may still be answering');
      await assert.rejects(withinATurn(conn.sendRequest('test/after-finish', {}, 0)), (err: Error) => {
        assert.ok(err instanceof ConnectionClosedError);
        assert.equal(err.outcome, 'not-sent');
        assert.match(err.message, /nothing was sent/i);
        return true;
      });
      conn.sendNotification('test/after-finish', {});
      conn.sendResponse(1, {});
      conn.sendError(2, -32000, 'after the end');
      await turn();
      assert.deepEqual(sent(), [], 'nothing is written once the output has ended');
      assert.equal(conn.isClosed, false, 'and the connection is still open');

      input.write(line('test/last'));
      const msg = await conn.nextMessage();
      assert.equal(msg.type === 'notification' && msg.notification.method, 'test/last');
      input.end();
      await seen.closed;
      assert.equal(seen.errors.length, 0);
    });

    it('an output that had already finished when the connection was built leaves the input to finish', { timeout: 5000 }, async () => {
      const finished = new PassThrough();
      finished.resume();
      const finishedClosed = new Promise((resolve) => finished.once('close', resolve));
      finished.end();
      await finishedClosed; // finished, and then destroyed (autoDestroy)
      assert.equal(finished.destroyed && finished.writableFinished, true);

      const input = new PassThrough();
      const conn = McplConnection.fromStreams(input, finished);
      const seen = watch(conn);
      await turn();
      await turn();
      assert.equal(conn.isClosed, false, 'still open: the input may still bring answers');
      await assert.rejects(withinATurn(conn.sendRequest('test/late', {}, 0)), { name: 'ConnectionClosedError' });

      input.write(line('test/still-read'));
      const msg = await conn.nextMessage();
      assert.equal(msg.type === 'notification' && msg.notification.method, 'test/still-read');
      input.end();
      await seen.closed;
      assert.equal(seen.errors.length, 0);
    });

    it('two streams failing from one cause: the first error is reported, and nothing after the close', { timeout: 5000 }, async () => {
      const input = new PassThrough();
      const output = new PassThrough();
      const conn = McplConnection.fromStreams(input, output);
      const seen = watch(conn);
      input.destroy(new Error('hangup (input)'));
      output.destroy(new Error('hangup (output)'));

      await seen.closed;
      await turn();
      assert.deepEqual(seen.errors.map((e) => e.message), ['hangup (input)']);
      assert.deepEqual(seen.order, ['error', 'close'], 'the first error is reported, then the close');
    });

    it("a listener of a stream's error finds the connection closed: it sends nothing, and a request or read it starts rejects", { timeout: 5000 }, async () => {
      const input = new PassThrough();
      const output = new PassThrough();
      const sent = capture(output);
      const conn = McplConnection.fromStreams(input, output);
      const seen = watch(conn);
      let closedWhenReported: boolean | undefined;
      let attempts: ReturnType<typeof sendAndRead> | undefined;
      conn.on('error', () => {
        closedWhenReported = conn.isClosed;
        attempts = sendAndRead(conn);
      });
      conn.sendNotification('test/before', {});
      await turn();
      assert.deepEqual(sent(), ['test/before'], 'a send before the failure reaches the output');
      input.destroy(new Error('input failure'));

      await seen.closed;
      assert.equal(closedWhenReported, true);
      await assert.rejects(attempts!.request, { name: 'ConnectionClosedError' });
      await assert.rejects(attempts!.read, { name: 'ConnectionClosedError' });
      await turn();
      assert.deepEqual(sent(), ['test/before'], 'nothing the listener sends reaches the output');
      assert.deepEqual(seen.order, ['error', 'close']);
    });

    it("a listener of 'close' when the input ends finds the connection closed: it sends nothing, and a request or read it starts rejects", { timeout: 5000 }, async () => {
      const input = new PassThrough();
      const output = new PassThrough();
      const sent = capture(output);
      const conn = McplConnection.fromStreams(input, output);
      let closedWhenHeard: boolean | undefined;
      let attempts: ReturnType<typeof sendAndRead> | undefined;
      conn.on('close', () => {
        closedWhenHeard = conn.isClosed;
        attempts = sendAndRead(conn);
      });
      const seen = watch(conn);
      conn.sendNotification('test/before', {});
      await turn();
      assert.deepEqual(sent(), ['test/before'], 'a send before the close reaches the output');
      input.end();

      await seen.closed;
      assert.equal(closedWhenHeard, true);
      await assert.rejects(attempts!.request, { name: 'ConnectionClosedError' });
      await assert.rejects(attempts!.read, { name: 'ConnectionClosedError' });
      await turn();
      assert.deepEqual(sent(), ['test/before'], 'nothing the listener sends reaches the output');
      assert.deepEqual(seen.order, ['close']);
    });

    it('a stream error raised while the first is being reported is not reported', { timeout: 5000 }, async () => {
      const input = new PassThrough();
      const output = new PassThrough();
      const conn = McplConnection.fromStreams(input, output);
      const seen = watch(conn);
      conn.on('error', (err) => {
        if (err.message === 'first') output.emit('error', new Error('second'));
      });
      input.destroy(new Error('first'));

      await seen.closed;
      await turn();
      assert.deepEqual(seen.errors.map((e) => e.message), ['first']);
      assert.deepEqual(seen.order, ['error', 'close']);
    });

    it('a stream error after close() is not reported', { timeout: 5000 }, async () => {
      const input = new PassThrough();
      const conn = McplConnection.fromStreams(input, new PassThrough());
      const seen = watch(conn);
      conn.close();
      input.destroy(new Error('late'));

      await turn();
      assert.equal(seen.errors.length, 0);
      assert.equal(seen.closes(), 1);
    });

    it('close() after the input closed the connection still destroys the output', { timeout: 5000 }, async () => {
      const input = new PassThrough();
      const output = new PassThrough();
      const conn = McplConnection.fromStreams(input, output);
      const seen = watch(conn);
      input.destroy();
      await seen.closed;
      assert.equal(output.destroyed, false, 'closing on its own releases nothing');

      conn.close();
      assert.equal(output.destroyed, true);
      assert.equal(seen.closes(), 1, 'close() releases without closing again');
    });

    it("close() releases once: called twice, or again from its own 'close' listener", { timeout: 5000 }, async () => {
      const ws = fakeWebSocket();
      const overWs = overWebSocket(ws);
      overWs.on('close', () => overWs.close());
      overWs.close();
      assert.equal(ws.closeCalls, 1, "a 'close' listener's close() inside close() releases once");
      overWs.close();
      assert.equal(ws.closeCalls, 1, 'a second close() releases nothing more');

      const output = new PassThrough();
      let destroys = 0;
      const destroy = output.destroy.bind(output);
      output.destroy = ((error?: Error) => {
        destroys++;
        return destroy(error);
      }) as typeof output.destroy;
      const overStreams = McplConnection.fromStreams(new PassThrough(), output);
      overStreams.on('close', () => overStreams.close());
      overStreams.close();
      overStreams.close();
      assert.equal(destroys, 1);
    });

    it("close() releases the streams even when a 'close' listener throws", { timeout: 5000 }, async () => {
      const output = new PassThrough();
      const overStreams = McplConnection.fromStreams(new PassThrough(), output);
      let threw = 0;
      overStreams.on('close', () => { threw++; throw new Error('listener'); });
      try { overStreams.close(); } catch { }
      assert.equal(threw, 1);
      assert.equal(output.destroyed, true);

      const ws = fakeWebSocket();
      const overWs = overWebSocket(ws);
      overWs.on('close', () => { threw++; throw new Error('listener'); });
      try { overWs.close(); } catch { }
      assert.equal(threw, 2);
      assert.equal(ws.closeCalls, 1);
    });

    it('lines already read when the connection closes are dropped; nextMessage() returns what was queued, then rejects', { timeout: 5000 }, async () => {
      const input = new PassThrough();
      const conn = McplConnection.fromStreams(input, new PassThrough());
      const heard: string[] = [];
      conn.on('notification', (n) => {
        heard.push(n.method);
        if (n.method === 'test/first') conn.close();
      });
      input.write(line('test/first') + line('test/second'));
      await turn();

      assert.deepEqual(heard, ['test/first']);
      const queued = await conn.nextMessage();
      assert.equal(queued.type === 'notification' && queued.notification.method, 'test/first');
      await assert.rejects(conn.nextMessage(), (err: Error) => {
        assert.ok(err instanceof ConnectionClosedError, 'a ConnectionClosedError');
        assert.equal(err.outcome, undefined);
        assert.match(err.message, /no more messages will come/);
        return true;
      });
    });

    it('streams that were already done close the connection at once, and report it on a later turn: the error one failed with, then close', { timeout: 5000 }, async () => {
      const destroyedInput = new PassThrough();
      destroyedInput.destroy();
      const endedInput = new PassThrough();
      endedInput.resume();
      endedInput.end();
      await new Promise((resolve) => endedInput.once('end', resolve));
      const destroyedOutput = new PassThrough();
      destroyedOutput.destroy();
      const failedInput = new PassThrough();
      failedInput.on('error', () => {});
      failedInput.destroy(new Error('input failed earlier'));
      const failedOutput = new PassThrough();
      failedOutput.on('error', () => {});
      failedOutput.destroy(new Error('output failed earlier'));
      await Promise.all([failedInput, failedOutput].map((s) => new Promise((resolve) => s.once('close', resolve))));
      const endedKeptInput = new PassThrough({ autoDestroy: false });
      endedKeptInput.resume();
      endedKeptInput.end();
      await new Promise((resolve) => endedKeptInput.once('end', resolve));
      assert.equal(endedKeptInput.readableEnded && !endedKeptInput.destroyed, true);
      const finishedThenFailedOutput = new PassThrough({ autoDestroy: false });
      finishedThenFailedOutput.resume();
      finishedThenFailedOutput.on('error', () => {});
      finishedThenFailedOutput.end();
      await new Promise((resolve) => finishedThenFailedOutput.once('finish', resolve));
      finishedThenFailedOutput.destroy(new Error('output failed after it finished'));
      await new Promise((resolve) => finishedThenFailedOutput.once('close', resolve));
      assert.equal(finishedThenFailedOutput.writableFinished && finishedThenFailedOutput.errored !== null, true);
      const failedKeptOutput = new Writable({ autoDestroy: false, write(_chunk, _encoding, callback) { callback(new Error('a write failed earlier')); } });
      const failedKeptOutputError = new Promise((resolve) => failedKeptOutput.once('error', resolve));
      failedKeptOutput.write('seed');
      await failedKeptOutputError;
      const failedKeptInput = new Readable({ autoDestroy: false, read() {} });
      const failedKeptInputError = new Promise((resolve) => failedKeptInput.once('error', resolve));
      failedKeptInput.push(123 as unknown as string);
      await failedKeptInputError;
      for (const stream of [failedKeptOutput, failedKeptInput]) {
        assert.equal(stream.errored !== null && !stream.destroyed, true);
      }

      const cases: Array<[Readable, Writable, string[]]> = [
        [destroyedInput, new PassThrough(), []],
        [endedInput, new PassThrough(), []],
        [endedKeptInput, new PassThrough(), []],
        [new PassThrough(), destroyedOutput, []],
        [failedInput, new PassThrough(), ['input failed earlier']],
        [new PassThrough(), failedOutput, ['output failed earlier']],
        [new PassThrough(), finishedThenFailedOutput, ['output failed after it finished']],
        [new PassThrough(), failedKeptOutput, ['a write failed earlier']],
        [failedKeptInput, new PassThrough(), [failedKeptInput.errored!.message]],
      ];
      for (const [input, output, errors] of cases) {
        const conn = McplConnection.fromStreams(input, output);
        const seen = watch(conn);
        assert.equal(conn.isClosed, true, 'closed from the start');
        assert.equal(seen.closes(), 0, 'and reported once the caller has had it to listen on');
        await assert.rejects(conn.nextMessage(), { name: 'ConnectionClosedError' });
        await seen.closed;
        assert.deepEqual(seen.errors.map((e) => e.message), errors);
        assert.deepEqual(seen.order, [...errors.map(() => 'error'), 'close']);
      }
    });

    it('a request sent at once on a connection built on an input already destroyed rejects, and nothing reaches the output', { timeout: 5000 }, async () => {
      const input = new PassThrough();
      const inputClosed = new Promise((resolve) => input.once('close', resolve));
      input.destroy();
      await inputClosed;
      const output = new PassThrough();
      const sent = capture(output);
      const conn = McplConnection.fromStreams(input, output);
      const seen = watch(conn);
      await assert.rejects(withinATurn(conn.sendRequest('test/side-effect', {}, 0)), { name: 'ConnectionClosedError' });
      conn.sendNotification('test/cleanup', {});
      await seen.closed;
      await turn();
      assert.deepEqual(sent(), [], 'nothing reaches the output');
    });

    it("a socket destroyed with an error before acceptTcp wraps it: the caller that awaited it hears the error, then 'close'", { timeout: 5000 }, async () => {
      const server = net.createServer();
      // A listener before acceptTcp's rejects the socket at once, so its
      // 'error' and 'close' are on their way when the connection is built.
      server.on('connection', (socket) => socket.destroy(new Error('rejected before wrapper')));
      await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
      const accepted = McplConnection.acceptTcp(server);
      const peer = net.connect((server.address() as net.AddressInfo).port, '127.0.0.1');
      peer.on('error', () => {});

      const conn = await accepted;
      server.close();
      const seen = watch(conn);
      try {
        await seen.closed;
        assert.deepEqual(seen.errors.map((e) => e.message), ['rejected before wrapper']);
        assert.deepEqual(seen.order, ['error', 'close']);
      } finally {
        conn.close();
        peer.destroy();
      }
    });

    it('a connection handed over through a promise that an event settles closes after the caller listens', { timeout: 5000 }, async () => {
      const destroyedInput = new PassThrough();
      destroyedInput.destroy();
      const server = new EventEmitter();
      const handedOver = new Promise<McplConnection>((resolve) => {
        server.once('connection', () => resolve(McplConnection.fromStreams(destroyedInput, new PassThrough())));
      });
      setImmediate(() => server.emit('connection'));

      const conn = await handedOver;
      const seen = watch(conn);
      await seen.closed;
      assert.equal(conn.isClosed, true);
    });

    it('a WebSocket error: one error, then closed; close() then closes the WebSocket', { timeout: 5000 }, async () => {
      const ws = fakeWebSocket();
      const conn = overWebSocket(ws);
      const seen = watch(conn);
      const waiting = conn.nextMessage();
      ws.emit('error', new Error('ws boom'));

      await seen.closed;
      await assert.rejects(waiting, { name: 'ConnectionClosedError' });
      await turn();
      assert.deepEqual(seen.errors.map((e) => e.message), ['ws boom']);
      assert.deepEqual(seen.order, ['error', 'close'], 'the error is reported, then the close');
      conn.close();
      assert.equal(ws.closeCalls, 1);
    });

    it('a WebSocket that closes cleanly: closed once, and no error', { timeout: 5000 }, async () => {
      const ws = fakeWebSocket();
      const conn = overWebSocket(ws);
      const seen = watch(conn);
      const waiting = conn.nextMessage();
      ws.emit('close');

      await seen.closed;
      await assert.rejects(waiting, { name: 'ConnectionClosedError' });
      await turn();
      assert.equal(seen.errors.length, 0);
      assert.equal(seen.closes(), 1);
    });

    it('a WebSocket that is connecting or closing sends nothing', { timeout: 5000 }, async () => {
      for (const readyState of [0, 2]) {
        const ws = fakeWebSocket(readyState);
        const conn = overWebSocket(ws);
        conn.sendNotification('test/out', {});
        await turn();
        assert.deepEqual(ws.sent, [], `readyState ${readyState}`);
        conn.close();
      }
    });

    it('a WebSocket without a readyState sends', { timeout: 5000 }, async () => {
      const ws = fakeWebSocket();
      delete (ws as { readyState?: number }).readyState;
      const conn = overWebSocket(ws);
      conn.sendNotification('test/out', {});
      await turn();
      assert.deepEqual(ws.sent.map((text) => (JSON.parse(text) as { method: string }).method), ['test/out']);
      conn.close();
    });

    it('a WebSocket already closed when it is wrapped closes the connection from the start, and reports it after a promise hand-over', { timeout: 5000 }, async () => {
      // Handed over as acceptTcp hands a connection over: built, and the
      // promise settled, in a callback of its own turn, so the caller resumes
      // only after whatever that turn queued for its next tick.
      const [conn, closedAtOnce] = await new Promise<[McplConnection, boolean]>((resolve) => setImmediate(() => {
        const built = overWebSocket(fakeWebSocket(3));
        resolve([built, built.isClosed]);
      }));
      const seen = watch(conn);
      assert.equal(closedAtOnce, true, 'closed before anything can be sent');
      await assert.rejects(conn.sendRequest('test/never', {}, 0), { name: 'ConnectionClosedError' });
      await seen.closed;
      await assert.rejects(conn.nextMessage(), { name: 'ConnectionClosedError' });
      assert.equal(seen.errors.length, 0);
    });
  });

  describe('framing: a message is a line ended by \\n over a stream, and a whole text message over a WebSocket', () => {
    const separators = 'one\u2028two\u2029three';
    const notification = (method: string, params: unknown = {}) => JSON.stringify({ jsonrpc: '2.0', method, params });

    it('over a stream, U+2028 and U+2029 in a string arrive in their message, and go out escaped', { timeout: 5000 }, async () => {
      const aToB = new PassThrough();
      const bToA = new PassThrough();
      const wire: string[] = [];
      aToB.on('data', (chunk) => wire.push(String(chunk)));
      const a = McplConnection.fromStreams(bToA, aToB);
      const b = McplConnection.fromStreams(aToB, bToA);
      const seen = heard(b);
      a.sendNotification('test/separators', { name: separators });
      await turn();
      assert.deepEqual(seen.params, [{ name: separators }]);
      assert.deepEqual(seen.errors, []);
      const out = wire.join('');
      assert.equal(out.indexOf('\n'), out.length - 1, 'one line, ended by \\n');
      assert.equal(/[\u2028\u2029]/.test(out), false, 'neither goes out raw');
      assert.deepEqual(JSON.parse(out), { jsonrpc: '2.0', method: 'test/separators', params: { name: separators } });
      a.close();
      b.close();
    });

    it("over a stream, a peer's raw U+2028 and U+2029 stay in their message", { timeout: 5000 }, async () => {
      const input = new PassThrough();
      const conn = McplConnection.fromStreams(input, new PassThrough());
      const seen = heard(conn);
      input.write(notification('test/raw', { name: separators }) + '\n');
      await turn();
      assert.deepEqual(seen.params, [{ name: separators }]);
      assert.deepEqual(seen.errors, []);
      conn.close();
    });

    it('over a stream, only \\n ends a line: a \\r\\n ending, a lone \\r between tokens, and an escaped CR and LF in a string', { timeout: 5000 }, async () => {
      const input = new PassThrough();
      const conn = McplConnection.fromStreams(input, new PassThrough());
      const seen = heard(conn);
      input.write(notification('test/crlf') + '\r\n');
      input.write('{"jsonrpc":"2.0",\r"method":"test/lone-cr","params":{}}\n');
      input.write(notification('test/escaped', { text: 'line one\r\nline two\n' }) + '\n');
      await turn();
      assert.deepEqual(seen.methods, ['test/crlf', 'test/lone-cr', 'test/escaped']);
      assert.deepEqual(seen.params[2], { text: 'line one\r\nline two\n' });
      assert.deepEqual(seen.errors, []);
      conn.close();
    });

    it('over a stream, an input paused before it was handed in is read', { timeout: 5000 }, async () => {
      const input = new PassThrough();
      input.pause();
      const conn = McplConnection.fromStreams(input, new PassThrough());
      const seen = heard(conn);
      input.write(notification('test/paused') + '\n');
      await turn();
      assert.deepEqual(seen.methods, ['test/paused']);
      conn.close();
    });

    it('over a WebSocket, each text message is parsed whole: raw U+2028 and U+2029, and a message written over several lines', { timeout: 5000 }, async () => {
      const ws = fakeWebSocket();
      const conn = overWebSocket(ws);
      const seen = heard(conn);
      ws.emit('message', notification('test/ws', { name: separators }));
      ws.emit('message', JSON.stringify({ jsonrpc: '2.0', method: 'test/pretty', params: { a: 1 } }, null, 2));
      await turn();
      assert.deepEqual(seen.methods, ['test/ws', 'test/pretty']);
      assert.deepEqual(seen.params, [{ name: separators }, { a: 1 }]);
      assert.deepEqual(seen.errors, []);
      conn.close();
    });

    it('over a WebSocket, each message sent is one text message, with U+2028 and U+2029 escaped and no newline', { timeout: 5000 }, async () => {
      const ws = fakeWebSocket();
      const conn = overWebSocket(ws);
      conn.sendNotification('test/out', { name: separators });
      await turn();
      assert.equal(ws.sent.length, 1);
      assert.equal(ws.sent[0]!.includes('\n'), false);
      assert.equal(/[\u2028\u2029]/.test(ws.sent[0]!), false, 'neither goes out raw');
      assert.deepEqual(JSON.parse(ws.sent[0]!).params, { name: separators });
      conn.close();
    });

    it('over a stream, three lines split into two chunks at every point are read whole, from bytes and from strings', { timeout: 20000 }, async () => {
      const text = notification('test/one', { name: separators }) + '\r\n' +
        notification('test/two', { name: 'é…' }) + '\n' + notification('test/three');
      const bytes = Buffer.from(text);
      for (const strings of [false, true]) {
        const whole = strings ? text : bytes;
        for (let cut = 1; cut < whole.length; cut++) {
          const input = new PassThrough();
          // With an encoding, 'data' gives strings, as fromTcp's setEncoding does.
          if (strings) input.setEncoding('utf8');
          const conn = McplConnection.fromStreams(input, new PassThrough());
          const seen = heard(conn);
          input.write(whole.slice(0, cut));
          input.end(whole.slice(cut));
          await new Promise((resolve) => conn.once('close', resolve));
          const where = `${strings ? 'strings' : 'bytes'} cut at ${cut}`;
          assert.deepEqual(seen.methods, ['test/one', 'test/two', 'test/three'], where);
          assert.deepEqual(seen.params.slice(0, 2), [{ name: separators }, { name: 'é…' }], where);
          assert.deepEqual(seen.errors, [], where);
        }
      }
    });

    it('over an input that delivers strings, as a TCP socket does, every line is read whole however the chunks fall', { timeout: 5000 }, async () => {
      const input = new PassThrough();
      // 'data' then gives strings, as fromTcp's setEncoding does.
      input.setEncoding('utf8');
      const conn = McplConnection.fromStreams(input, new PassThrough());
      const seen = heard(conn);
      const third = notification('test/three');
      input.write(notification('test/one', { name: separators }) + '\n' + notification('test/two') + '\r\n' + third.slice(0, 10));
      await turn();
      input.write(third.slice(10) + '\r');
      await turn();
      assert.deepEqual(seen.methods, ['test/one', 'test/two'], 'the third waits for its \\n');
      input.write('\n');
      await turn();
      conn.close();
      assert.deepEqual(seen.methods, ['test/one', 'test/two', 'test/three']);
      assert.deepEqual(seen.params[0], { name: separators });
      assert.deepEqual(seen.errors, []);
    });

    it('over a stream, a 16 MiB line in 8 KiB chunks arrives whole', { timeout: 30000 }, async () => {
      const input = new PassThrough();
      const conn = McplConnection.fromStreams(input, new PassThrough());
      const big = 'x'.repeat(16 * 1024 * 1024);
      const line = Buffer.from(notification('test/big', { big }) + '\n');
      const got = conn.nextMessage();
      for (let at = 0; at < line.length; at += 8 * 1024) input.write(line.subarray(at, at + 8 * 1024));
      const message = (await got) as { notification?: { method?: string; params?: { big?: string } } };
      conn.close();
      assert.equal(message.notification?.method, 'test/big');
      assert.equal(message.notification?.params?.big?.length, big.length);
    });

    it('after a close, nothing more is taken in, even if something resumes the input', { timeout: 5000 }, async () => {
      const input = new PassThrough();
      const conn = McplConnection.fromStreams(input, new PassThrough());
      const seen = heard(conn);
      conn.close();
      const callerRead: string[] = [];
      input.on('data', (chunk) => callerRead.push(String(chunk)));
      input.resume();
      input.write(notification('test/after') + '\n');
      await turn();
      assert.deepEqual(seen.methods, []);
      assert.equal(callerRead.join(''), notification('test/after') + '\n', 'the input stays the caller\'s');
    });

    it('over a stream, a malformed last line with no listener for its error still closes the connection', { timeout: 5000 }, async () => {
      const input = new PassThrough();
      const conn = McplConnection.fromStreams(input, new PassThrough());
      let closes = 0;
      conn.on('close', () => closes++);
      const pending = conn.sendRequest('test/waiting', {}, 0);
      input.write('{"jsonrpc": "2.0", "method": ');
      await turn();
      // The input ends: its last line is malformed, and reporting it throws,
      // as an 'error' with no listener does.
      assert.throws(() => input.emit('end'));
      assert.equal(conn.isClosed, true);
      assert.equal(closes, 1, "'close' was emitted");
      await assert.rejects(pending, { name: 'ConnectionClosedError' });
    });

    it('over a stream, a malformed line whose report throws strands nothing: what is behind it is taken on a later turn, or at the end of the input', { timeout: 5000 }, async () => {
      for (const ending of [false, true]) {
        const input = new PassThrough();
        const conn = McplConnection.fromStreams(input, new PassThrough());
        const methods: string[] = [];
        conn.on('notification', (n) => methods.push(n.method));
        // No 'error' listener: reporting the malformed line throws.
        assert.throws(() => input.emit('data', Buffer.from('{oops\n' + notification('test/behind') + '\n')));
        if (ending) {
          input.emit('end');
          assert.deepEqual(methods, ['test/behind'], 'taken at the end');
          assert.equal(conn.isClosed, true);
          continue;
        }
        await turn();
        assert.deepEqual(methods, ['test/behind'], 'taken on a later turn');
        input.emit('data', Buffer.from(notification('test/later') + '\n'));
        assert.deepEqual(methods, ['test/behind', 'test/later']);
        conn.close();
      }
    });

    it('over a stream, a listener that closes the connection on the last line, at the end of the input, closes it once', { timeout: 5000 }, async () => {
      const input = new PassThrough();
      const conn = McplConnection.fromStreams(input, new PassThrough());
      const seen = heard(conn);
      let closes = 0;
      conn.on('close', () => closes++);
      conn.on('notification', () => conn.close());
      input.end(notification('test/last'));
      await new Promise((resolve) => setImmediate(resolve));
      await turn();
      assert.deepEqual(seen.methods, ['test/last']);
      assert.equal(closes, 1);
      assert.deepEqual(seen.errors, []);
    });

    it('over a WebSocket, a text message holding two JSON objects on two lines is one malformed message', { timeout: 5000 }, async () => {
      const ws = fakeWebSocket();
      const conn = overWebSocket(ws);
      const seen = heard(conn);
      ws.emit('message', notification('test/one') + '\n' + notification('test/two'));
      await turn();
      conn.close();
      assert.deepEqual(seen.methods, []);
      assert.equal(seen.errors.length, 1);
    });

    it('an input already done when it is wrapped delivers nothing it still held', { timeout: 5000 }, async () => {
      const line = notification('test/held') + '\n';
      const destroyed = new Readable({ read() {} });
      destroyed.push(line);
      destroyed.destroy();
      const failed = new Readable({ autoDestroy: false, read() {} });
      failed.push(line);
      const failedError = new Promise((resolve) => failed.once('error', resolve));
      failed.push(123 as unknown as string);
      await failedError;
      assert.equal(failed.readableLength > 0, true, 'the failed input still holds the line');
      for (const input of [destroyed, failed]) {
        const conn = McplConnection.fromStreams(input, new PassThrough());
        const seen = heard(conn);
        const next = conn.nextMessage().then(() => 'a message', (err: Error) => err.name);
        await turn();
        await turn();
        assert.deepEqual(seen.methods, []);
        assert.equal(await next, 'ConnectionClosedError');
      }
    });

    it('a connection closed from the start reads nothing from its input', { timeout: 5000 }, async () => {
      let reads = 0;
      const input = new Readable({ read() { reads++; this.push(Buffer.alloc(1024)); } });
      const output = new PassThrough();
      output.destroy();
      await new Promise((resolve) => output.once('close', resolve));
      const conn = McplConnection.fromStreams(input, output);
      conn.on('error', () => {});
      assert.equal(conn.isClosed, true);
      await turn();
      await turn();
      assert.equal(reads, 0, 'the source is never read');
      assert.equal(input.readableLength, 0);
    });

    describe('a listener that pushes more input while a message is being taken', () => {
      const json = (method: string) => JSON.stringify({ jsonrpc: '2.0', method, params: {} });
      async function reading(onNotification: (method: string, input: Readable) => void) {
        const input = new Readable({ read() {} });
        const conn = McplConnection.fromStreams(input, new PassThrough());
        const seen = heard(conn);
        conn.on('notification', (n) => onNotification(n.method, input));
        await turn();
        return { input, conn, seen };
      }

      it('supplies the next message after a first one split across chunks', { timeout: 5000 }, async () => {
        const { input, conn, seen } = await reading((method, i) => { if (method === 'first') i.push(json('second') + '\n'); });
        const first = json('first');
        const cut = first.indexOf('method');
        input.push(first.slice(0, cut));
        await turn();
        input.push(first.slice(cut) + '\n');
        await turn();
        conn.close();
        assert.deepEqual({ methods: seen.methods, errors: seen.errors }, { methods: ['first', 'second'], errors: [] });
      });

      it('completes a partial message its own chunk ended with', { timeout: 5000 }, async () => {
        const second = json('second');
        const cut = second.indexOf('method');
        const { input, conn, seen } = await reading((method, i) => { if (method === 'first') i.push(second.slice(cut) + '\n'); });
        input.push(json('first') + '\n' + second.slice(0, cut));
        await turn();
        conn.close();
        assert.deepEqual({ methods: seen.methods, errors: seen.errors }, { methods: ['first', 'second'], errors: [] });
      });

      it('starts a partial message that a later chunk completes', { timeout: 5000 }, async () => {
        const second = json('second');
        const cut = second.indexOf('method');
        const { input, conn, seen } = await reading((method, i) => { if (method === 'first') i.push(second.slice(0, cut)); });
        input.push(json('first') + '\n');
        await turn();
        input.push(second.slice(cut) + '\n');
        await turn();
        conn.close();
        assert.deepEqual({ methods: seen.methods, errors: seen.errors }, { methods: ['first', 'second'], errors: [] });
      });

      it('waits behind the rest of the chunk it arrived during, so messages keep the order of the bytes', { timeout: 5000 }, async () => {
        const c = json('c');
        const cut = c.indexOf('method');
        const { input, conn, seen } = await reading((method, i) => { if (method === 'a') i.push(c.slice(cut) + '\n' + json('d') + '\n'); });
        input.push(json('a') + '\n' + json('b') + '\n' + c.slice(0, cut));
        await turn();
        conn.close();
        assert.deepEqual({ methods: seen.methods, errors: seen.errors }, { methods: ['a', 'b', 'c', 'd'], errors: [] });
      });

      it('over a stream, takes a line pushed from a listener only after the current one has reached every listener', { timeout: 5000 }, async () => {
        const input = new Readable({ read() {} });
        const conn = McplConnection.fromStreams(input, new PassThrough());
        const order: string[] = [];
        conn.on('notification', (n) => {
          order.push(`one:${n.method}`);
          if (n.method === 'first') input.push(json('second') + '\n');
        });
        conn.on('notification', (n) => order.push(`two:${n.method}`));
        await turn();
        input.push(json('first') + '\n');
        await turn();
        conn.close();
        assert.deepEqual(order, ['one:first', 'two:first', 'one:second', 'two:second']);
      });

      it('that ends the input mid-line takes the lines behind it, then the last line the end completes, then closes', { timeout: 5000 }, async () => {
        const c = json('c');
        const cut = c.indexOf('method');
        const input = new PassThrough();
        const conn = McplConnection.fromStreams(input, new PassThrough());
        const seen = heard(conn);
        let closedAfter: string[] | undefined;
        conn.on('close', () => { closedAfter = [...seen.methods]; });
        conn.on('notification', (n) => { if (n.method === 'a') input.end(c.slice(cut)); });
        input.write(json('a') + '\n' + json('b') + '\n' + c.slice(0, cut));
        await new Promise((resolve) => setImmediate(resolve));
        await turn();
        assert.deepEqual(seen.methods, ['a', 'b', 'c']);
        assert.deepEqual(closedAfter, ['a', 'b', 'c'], 'closed after the last line');
        assert.deepEqual(seen.errors, []);
      });

      it("over a WebSocket, the socket's close takes what is queued behind a message whose report threw", { timeout: 5000 }, async () => {
        const ws = fakeWebSocket();
        const conn = overWebSocket(ws);
        const methods: string[] = [];
        conn.on('notification', (n) => {
          methods.push(n.method);
          if (n.method === 'first') {
            ws.emit('message', '{oops');
            ws.emit('message', json('behind'));
          }
        });
        // No 'error' listener: reporting the malformed message throws, with
        // 'behind' queued after it.
        assert.throws(() => ws.emit('message', json('first')));
        ws.emit('close');
        await new Promise((resolve) => setImmediate(resolve));
        assert.deepEqual(methods, ['first', 'behind']);
        assert.equal(conn.isClosed, true);
      });

      it("over a WebSocket, a message after the socket's close is ignored", { timeout: 5000 }, async () => {
        const ws = fakeWebSocket();
        const conn = overWebSocket(ws);
        const seen = heard(conn);
        let closes = 0;
        conn.on('close', () => closes++);
        ws.emit('close');
        ws.emit('message', json('late'));
        await turn();
        await turn();
        assert.deepEqual(seen.methods, []);
        assert.deepEqual(seen.errors, []);
        assert.equal(closes, 1);
      });

      it('over a WebSocket, takes messages sent from a listener only after the current one has reached every listener, in the order sent', { timeout: 5000 }, async () => {
        const ws = fakeWebSocket();
        const conn = overWebSocket(ws);
        const order: string[] = [];
        conn.on('notification', (n) => {
          order.push(`one:${n.method}`);
          if (n.method === 'first') {
            ws.emit('message', json('second'));
            ws.emit('message', json('third'));
          }
        });
        conn.on('notification', (n) => order.push(`two:${n.method}`));
        ws.emit('message', json('first'));
        await turn();
        conn.close();
        assert.deepEqual(order, ['one:first', 'two:first', 'one:second', 'two:second', 'one:third', 'two:third']);
      });
    });
  });

  it('a request without jsonrpc 2.0 does not reach nextMessage()', async () => {
    const { PassThrough } = await import('node:stream');

    const aToB = new PassThrough();
    const bToA = new PassThrough();

    const conn = McplConnection.fromStreams(aToB, bToA);

    aToB.write(JSON.stringify({ id: 1, method: 'test', params: {} }) + '\n');

    aToB.write(JSON.stringify({ jsonrpc: '2.0', method: 'test/real', params: {} }) + '\n');

    const msg = await conn.nextMessage();
    assert.equal(msg.type, 'notification');
    if (msg.type === 'notification') {
      assert.equal(msg.notification.method, 'test/real');
    }

    conn.close();
  });

  it('event emitter API', async () => {
    const [client, server] = await connectedPair();

    const received: string[] = [];

    server.on('request', (req) => {
      received.push(`request:${req.method}`);
    });
    server.on('notification', (notif) => {
      received.push(`notification:${notif.method}`);
    });

    client.sendNotification('test/ping', { n: 1 });
    client.sendNotification('test/ping', { n: 2 });

    const msg1 = await server.nextMessage();
    assert.equal(msg1.type, 'notification');

    const msg2 = await server.nextMessage();
    assert.equal(msg2.type, 'notification');

    assert.equal(received.length, 2);
    assert.equal(received[0], 'notification:test/ping');
    assert.equal(received[1], 'notification:test/ping');

    client.close();
    server.close();
  });
});

// t.after runs when a test ends or times out, where a `finally` after an
// accept that never settles would never run. A suite's timeout cancels the
// suite without ending a test still waiting, so the bound is per test.
const BOUNDED = { timeout: 5_000 };

describe('McplConnection.acceptTcp', () => {
  const failedAccept = (): NodeJS.ErrnoException =>
    Object.assign(new Error('accept EMFILE'), { code: 'EMFILE', syscall: 'accept' });

  /** Clients are reset last: a reset also ends a server-side socket nothing
   *  took, such as one a stalled accept dropped, which would otherwise keep
   *  the process alive. */
  async function listening(t: TestContext) {
    const server = net.createServer();
    const clients: net.Socket[] = [];
    const held: net.Socket[] = [];
    const connections: McplConnection[] = [];
    t.after(() => {
      for (const conn of connections) conn.close();
      for (const socket of held) socket.destroy();
      for (const client of clients) client.resetAndDestroy();
      server.close();
    });
    server.listen(0, '127.0.0.1');
    await new Promise<void>((resolve) => server.once('listening', resolve));
    const port = (server.address() as net.AddressInfo).port;
    return {
      server,
      connect: () => new Promise<net.Socket>((resolve, reject) => {
        const socket = net.createConnection({ host: '127.0.0.1', port }, () => resolve(socket));
        clients.push(socket);
        socket.once('error', reject);
      }),
      accept: async (): Promise<McplConnection> => {
        const conn = await McplConnection.acceptTcp(server);
        connections.push(conn);
        return conn;
      },
      track: (socket: net.Socket): void => { held.push(socket); },
    };
  }

  it('leaves no listener on the server per accepted connection, so accepting more than ten draws no warning', BOUNDED, async (t) => {
    const { server, connect, accept } = await listening(t);
    const baseline = { connection: server.listenerCount('connection'), error: server.listenerCount('error') };
    const warnings: Error[] = [];
    const onWarning = (warning: Error): void => { warnings.push(warning); };
    process.on('warning', onWarning);
    t.after(() => { process.off('warning', onWarning); });
    for (let i = 0; i < 12; i++) await Promise.all([accept(), connect()]);
    // A warning is emitted on a later tick.
    await new Promise((r) => setImmediate(r));
    assert.equal(server.listenerCount('connection'), baseline.connection);
    assert.equal(server.listenerCount('error'), baseline.error);
    assert.deepEqual(warnings.filter((w) => w.name === 'MaxListenersExceededWarning'), []);
  });

  it('rejects when a listening server errors with an accept pending, and leaves no listener behind', BOUNDED, async (t) => {
    const { server, accept } = await listening(t);
    const baseline = { connection: server.listenerCount('connection'), error: server.listenerCount('error') };
    const accepting = accept();
    const failure = failedAccept();
    server.emit('error', failure);
    await assert.rejects(accepting, (err) => err === failure);
    assert.equal(server.listenerCount('connection'), baseline.connection);
    assert.equal(server.listenerCount('error'), baseline.error);
    assert.ok(server.listening, 'the server is still listening');
  });

  it('after a failed accept, the listening server accepts again on the next connection', BOUNDED, async (t) => {
    const { server, connect, accept } = await listening(t);
    const failing = accept();
    const failure = failedAccept();
    server.emit('error', failure);
    await assert.rejects(failing, (err) => err === failure);
    const [conn] = await Promise.all([accept(), connect()]);
    assert.ok(conn instanceof McplConnection, 'the next accept takes the next connection');
  });

  it('a failure to listen rejects a pending accept, and leaves the server not listening', BOUNDED, async (t) => {
    const holder = net.createServer();
    const server = net.createServer();
    t.after(() => { server.close(); holder.close(); });
    holder.listen(0, '127.0.0.1');
    await new Promise<void>((resolve) => holder.once('listening', resolve));
    const port = (holder.address() as net.AddressInfo).port;
    const accepting = McplConnection.acceptTcp(server);
    server.listen(port, '127.0.0.1');
    await assert.rejects(accepting, (err: NodeJS.ErrnoException) => err.code === 'EADDRINUSE');
    assert.equal(server.listening, false, 'not listening: listen again before accepting');
    assert.equal(server.listenerCount('error'), 0);
    assert.equal(server.listenerCount('connection'), 0);
  });

  it("between accepts, an 'error' with no listener of the caller's throws from the server's emit: after an accept that took a connection, and after one that failed", BOUNDED, async (t) => {
    const { server, connect, accept } = await listening(t);
    await Promise.all([accept(), connect()]);
    const afterSuccess = failedAccept();
    assert.throws(() => server.emit('error', afterSuccess), (err) => err === afterSuccess);
    const failing = accept();
    const first = failedAccept();
    server.emit('error', first);
    await assert.rejects(failing, (err) => err === first);
    const afterFailure = failedAccept();
    assert.throws(() => server.emit('error', afterFailure), (err) => err === afterFailure);
  });

  it("keeps the caller's own listeners, and an error after an accept settles reaches only them", BOUNDED, async (t) => {
    const { server, connect, accept, track } = await listening(t);
    const callerErrors: Error[] = [];
    const callerSockets: net.Socket[] = [];
    const onCallerError = (err: Error): void => { callerErrors.push(err); };
    const onCallerConnection = (socket: net.Socket): void => { callerSockets.push(socket); track(socket); };
    server.on('error', onCallerError);
    server.on('connection', onCallerConnection);
    await Promise.all([accept(), connect()]);
    assert.deepEqual(server.listeners('error'), [onCallerError]);
    assert.deepEqual(server.listeners('connection'), [onCallerConnection]);
    const failure = failedAccept();
    assert.doesNotThrow(() => server.emit('error', failure));
    assert.deepEqual(callerErrors, [failure]);
    assert.equal(callerSockets.length, 1, "the caller's connection listener saw the accepted socket too");
  });
});
