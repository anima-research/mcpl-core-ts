/**
 * Integration tests for McplConnection.
 * Mirrors mcpl-core/tests/connection_test.rs
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import * as net from 'node:net';
import { PassThrough } from 'node:stream';

import {
  McplConnection,
  textContent,
  method,
  advertisedCapabilitiesFromInitialize,
  ERR_CHECKPOINT_NOT_FOUND,
  ERR_CAPABILITY_DENIED,
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

/** Create a pair of McplConnections connected via TCP loopback. */
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

  // Close the listener — we only need the one connection
  server.close();

  return [clientConn, serverConn];
}

describe('McplConnection', () => {
  it('capability negotiation', async () => {
    const [client, server] = await connectedPair();

    const clientCaps: McplCapabilities = {
      version: '0.5',
      pushEvents: true,
      // SPEC §14.1: `channels` is an object. `true` remains legal as the §5.1
      // shorthand for every leaf beneath it.
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

    // Client sends initialize, server receives and responds
    const clientPromise = client.sendRequest('initialize', initParams);

    const msg = await server.nextMessage();
    assert.equal(msg.type, 'request');
    if (msg.type !== 'request') throw new Error('unreachable');
    assert.equal(msg.request.method, 'initialize');

    const params = msg.request.params as McplInitializeParams;
    assert.equal(params.clientInfo.name, 'test-client');

    // SPEC §6.1: `featureSets` is an object keyed by name — the name is the key,
    // not a member. §6.2: `uses` draws on the closed capability-path vocabulary;
    // `hostState` was removed in 0.4.1 and no longer exists.
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

    // channels.streaming is now DECLARABLE — the whole point of #4 item 1.
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

    // Server sends push/event to client
    const eventParams: PushEventParams = {
      featureSet: 'lobby',
      eventId: 'evt_001',
      timestamp: '2026-02-12T00:00:00Z',
      payload: {
        content: [textContent('User joined lobby')],
      },
    };

    const serverPromise = server.sendRequest(method.PUSH_EVENT, eventParams);

    // Client receives and responds
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

    // Server registers channels
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

    // Client receives register request
    const regMsg = await client.nextMessage();
    assert.equal(regMsg.type, 'request');
    if (regMsg.type !== 'request') throw new Error('unreachable');
    assert.equal(regMsg.request.method, 'channels/register');

    const regP = regMsg.request.params as ChannelsRegisterParams;
    assert.equal(regP.channels.length, 1);
    assert.equal(regP.channels[0].id, 'game');

    client.sendResponse(regMsg.request.id, {});
    await serverRegPromise;

    // Client opens a channel
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

    await assert.rejects(clientPromise, (err: Error) => {
      assert.ok(err.name === 'RpcError');
      assert.ok(err.message.includes('-32005'));
      assert.ok(err.message.includes('Checkpoint not found'));
      // No data was sent, so none is invented.
      assert.equal((err as unknown as { data?: unknown }).data, undefined);
      return true;
    });

    client.close();
    server.close();
  });

  it('error response with the SPEC §6.6 data member', async () => {
    const [client, server] = await connectedPair();

    const clientPromise = client.sendRequest(method.CHANNELS_PUBLISH, {
      conversationId: 'c1', channelId: 'game:live-1', content: [],
    });

    const msg = await server.nextMessage();
    assert.equal(msg.type, 'request');
    if (msg.type !== 'request') throw new Error('unreachable');

    // §6.6 documents concrete `data` shapes — e.g. `data: { capability }` on a
    // capability-denied error — which sendError must be able to emit.
    server.sendError(msg.request.id, ERR_CAPABILITY_DENIED, 'Capability denied', { capability: 'channels.publish' });

    await assert.rejects(clientPromise, (err: Error) => {
      assert.ok(err.name === 'RpcError');
      assert.deepEqual((err as unknown as { data?: unknown }).data, { capability: 'channels.publish' });
      return true;
    });

    client.close();
    server.close();
  });

  it('content block serialization', async () => {
    const [client, server] = await connectedPair();

    // Send various content blocks through a notification
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

  it('close rejects pending requests', async () => {
    const [client, server] = await connectedPair();

    // Send a request that won't be answered
    const promise = client.sendRequest('test/slow', {});

    // Close the connection before responding
    client.close();

    await assert.rejects(promise, (err: Error) => {
      assert.equal(err.name, 'ConnectionClosedError');
      return true;
    });

    server.close();
  });

  it('close rejects nextMessage waiters', async () => {
    const [client, server] = await connectedPair();

    // Start waiting for a message that won't come
    const promise = server.nextMessage();

    // Close the connection
    server.close();

    await assert.rejects(promise, (err: Error) => {
      assert.equal(err.name, 'ConnectionClosedError');
      return true;
    });

    client.close();
  });

  it('request timeout', async () => {
    const [client, server] = await connectedPair();

    // Send a request with a very short timeout
    const promise = client.sendRequest('test/slow', {}, 50);

    // Don't respond — let it timeout
    await assert.rejects(promise, (err: Error) => {
      assert.equal(err.name, 'RpcError');
      assert.ok(err.message.includes('timed out'));
      return true;
    });

    client.close();
    server.close();
  });

  it('fromStreams factory', async () => {
    const { PassThrough } = await import('node:stream');

    const aToB = new PassThrough();
    const bToA = new PassThrough();

    const connA = McplConnection.fromStreams(bToA, aToB);
    const connB = McplConnection.fromStreams(aToB, bToA);

    connA.sendNotification('test/hello', { from: 'A' });

    const msg = await connB.nextMessage();
    assert.equal(msg.type, 'notification');
    if (msg.type === 'notification') {
      assert.equal(msg.notification.method, 'test/hello');
      assert.deepEqual(msg.notification.params, { from: 'A' });
    }

    connA.close();
    connB.close();
  });

  it('emits error on malformed JSON', async () => {
    const { PassThrough } = await import('node:stream');

    const aToB = new PassThrough();
    const bToA = new PassThrough();

    const conn = McplConnection.fromStreams(aToB, bToA);

    const errors: Error[] = [];
    conn.on('error', (err) => errors.push(err));

    // Write malformed JSON directly to the stream
    aToB.write('this is not json\n');

    // Give the event loop a tick
    await new Promise((resolve) => setTimeout(resolve, 10));

    assert.equal(errors.length, 1);
    assert.ok(errors[0].message.includes('Malformed JSON-RPC'));

    conn.close();
  });

  describe('a transport that ends or fails closes the connection', () => {
    /** A server-side connection, and the raw client socket at the other end. */
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

    /**
     * What the connection reports: its errors, how often it closed, and the
     * order of both ('error' and 'close' as they were emitted).
     */
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

    /** One turn of the event loop: whatever a stream emits on the same tick or the next has landed. */
    const turn = () => new Promise((resolve) => setImmediate(resolve));

    /** A WebSocket stand-in for fromWebSocket. */
    function fakeWebSocket(readyState = 1) {
      const ws = Object.assign(new EventEmitter(), { readyState, sent: [] as string[], closeCalls: 0 });
      return Object.assign(ws, {
        send(data: string) { ws.sent.push(data); },
        close() { ws.closeCalls++; },
      });
    }
    const overWebSocket = (ws: ReturnType<typeof fakeWebSocket>) =>
      McplConnection.fromWebSocket(ws as unknown as Parameters<typeof McplConnection.fromWebSocket>[0]);

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
      conn.on('close', () => closes++);
      const waiting = conn.nextMessage();

      assert.throws(() => input.emit('error', new Error('unheard')), { message: 'unheard' });
      assert.equal(conn.isClosed, true);
      assert.equal(closes, 1);
      await assert.rejects(waiting, { name: 'ConnectionClosedError' });
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
      assert.equal(input.isPaused(), true, 'a closed connection stops reading its input');
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

    it('an output ended on purpose leaves the input to finish', { timeout: 5000 }, async () => {
      const input = new PassThrough();
      const output = new PassThrough();
      output.resume(); // the peer reads what was sent, so the output can finish
      const conn = McplConnection.fromStreams(input, output);
      const seen = watch(conn);
      const outputClosed = new Promise((resolve) => output.once('close', resolve));
      output.end();
      await outputClosed;
      await turn();
      assert.equal(conn.isClosed, false, 'still open: the peer may still be answering');

      input.write(JSON.stringify({ jsonrpc: '2.0', method: 'test/last', params: {} }) + '\n');
      const msg = await conn.nextMessage();
      assert.equal(msg.type === 'notification' && msg.notification.method, 'test/last');
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
      const sent: string[] = [];
      output.on('data', (chunk) => sent.push(String(chunk)));
      const conn = McplConnection.fromStreams(input, output);
      const seen = watch(conn);
      let closedWhenReported: boolean | undefined;
      let request: Promise<unknown> | undefined;
      let read: Promise<unknown> | undefined;
      conn.on('error', () => {
        closedWhenReported = conn.isClosed;
        request = conn.sendRequest('test/side-effect', {}, 0);
        conn.sendNotification('test/cleanup', {});
        read = conn.nextMessage();
      });
      input.destroy(new Error('input failure'));

      await seen.closed;
      assert.equal(closedWhenReported, true);
      await assert.rejects(request!, { name: 'ConnectionClosedError' });
      await assert.rejects(read!, { name: 'ConnectionClosedError' });
      await turn();
      assert.deepEqual(sent, [], 'nothing reaches the output, though it still works');
      assert.deepEqual(seen.order, ['error', 'close']);
    });

    it('a stream error raised while the first is being reported is not reported', { timeout: 5000 }, async () => {
      const input = new PassThrough();
      const output = new PassThrough();
      const conn = McplConnection.fromStreams(input, output);
      const seen = watch(conn);
      // A listener whose cleanup fails the other stream at once.
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

    it("close() releases the streams even when a 'close' listener throws", { timeout: 5000 }, async () => {
      const output = new PassThrough();
      const overStreams = McplConnection.fromStreams(new PassThrough(), output);
      overStreams.on('close', () => { throw new Error('listener'); });
      assert.throws(() => overStreams.close(), { message: 'listener' });
      assert.equal(output.destroyed, true);

      const ws = fakeWebSocket();
      const overWs = overWebSocket(ws);
      overWs.on('close', () => { throw new Error('listener'); });
      assert.throws(() => overWs.close(), { message: 'listener' });
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
      const line = (m: string) => JSON.stringify({ jsonrpc: '2.0', method: m, params: {} }) + '\n';
      input.write(line('test/first') + line('test/second'));
      await turn();

      assert.deepEqual(heard, ['test/first']);
      const queued = await conn.nextMessage();
      assert.equal(queued.type === 'notification' && queued.notification.method, 'test/first');
      await assert.rejects(conn.nextMessage(), { name: 'ConnectionClosedError' });
    });

    it('streams that had already ended or been destroyed close the connection on the next tick', { timeout: 5000 }, async () => {
      const destroyedInput = new PassThrough();
      destroyedInput.destroy();
      const endedInput = new PassThrough();
      endedInput.resume();
      endedInput.end();
      await new Promise((resolve) => endedInput.once('end', resolve));
      const destroyedOutput = new PassThrough();
      destroyedOutput.destroy();

      for (const [input, output] of [
        [destroyedInput, new PassThrough()],
        [endedInput, new PassThrough()],
        [new PassThrough(), destroyedOutput],
      ] as const) {
        const conn = McplConnection.fromStreams(input, output);
        assert.equal(conn.isClosed, false, 'open until the caller has had it to listen on');
        const seen = watch(conn);
        await seen.closed;
        await assert.rejects(conn.nextMessage(), { name: 'ConnectionClosedError' });
        assert.equal(seen.errors.length, 0);
      }
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
      assert.equal(ws.closeCalls, 0);
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

    it('a WebSocket already closed when it is wrapped closes the connection', { timeout: 5000 }, async () => {
      const conn = overWebSocket(fakeWebSocket(3));
      const seen = watch(conn);
      await seen.closed;
      await assert.rejects(conn.nextMessage(), { name: 'ConnectionClosedError' });
      assert.equal(seen.errors.length, 0);
    });
  });

  it('ignores messages without jsonrpc 2.0', async () => {
    const { PassThrough } = await import('node:stream');

    const aToB = new PassThrough();
    const bToA = new PassThrough();

    const conn = McplConnection.fromStreams(aToB, bToA);

    // Write a valid JSON object but without jsonrpc field
    aToB.write(JSON.stringify({ id: 1, method: 'test', params: {} }) + '\n');

    // Write a valid JSON-RPC 2.0 notification
    aToB.write(JSON.stringify({ jsonrpc: '2.0', method: 'test/real', params: {} }) + '\n');

    // Only the valid JSON-RPC 2.0 message should come through
    const msg = await conn.nextMessage();
    assert.equal(msg.type, 'notification');
    if (msg.type === 'notification') {
      assert.equal(msg.notification.method, 'test/real');
    }

    conn.close();
  });

  it('event emitter API', async () => {
    const [client, server] = await connectedPair();

    // Test the EventEmitter API alongside pull-based
    const received: string[] = [];

    server.on('request', (req) => {
      received.push(`request:${req.method}`);
    });
    server.on('notification', (notif) => {
      received.push(`notification:${notif.method}`);
    });

    client.sendNotification('test/ping', { n: 1 });
    client.sendNotification('test/ping', { n: 2 });

    // Pull-based also sees the messages (dual delivery)
    const msg1 = await server.nextMessage();
    assert.equal(msg1.type, 'notification');

    const msg2 = await server.nextMessage();
    assert.equal(msg2.type, 'notification');

    // Events were also emitted
    assert.equal(received.length, 2);
    assert.equal(received[0], 'notification:test/ping');
    assert.equal(received[1], 'notification:test/ping');

    client.close();
    server.close();
  });
});
