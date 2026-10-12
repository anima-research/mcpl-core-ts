/**
 * Integration tests for McplConnection.
 * Mirrors mcpl-core/tests/connection_test.rs
 */

import { describe, it, type TestContext } from 'node:test';
import * as assert from 'node:assert/strict';
import * as net from 'node:net';

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

// Each test has its own timeout, and everything it opens is torn down by
// t.after, which runs when the test ends or times out: a `finally` after an
// accept that never settles would never run, and the server it leaves
// listening would keep the process alive. (A suite's timeout cancels the suite
// without ending a test still waiting, so the bound is per test.)
const BOUNDED = { timeout: 5_000 };

describe('McplConnection.acceptTcp', () => {
  const failedAccept = (): NodeJS.ErrnoException =>
    Object.assign(new Error('accept EMFILE'), { code: 'EMFILE', syscall: 'accept' });

  /** A listening server, and every socket and connection a test opens on it,
   *  each tracked as it is created and closed by t.after. Clients are reset
   *  last, after every server-side socket the test holds is closed: a reset
   *  also ends a server-side socket nothing took, such as one a stalled
   *  accept dropped, which would otherwise keep the process alive. */
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
      /** A client connected to the server, tracked before it connects. */
      connect: () => new Promise<net.Socket>((resolve, reject) => {
        const socket = net.createConnection({ host: '127.0.0.1', port }, () => resolve(socket));
        clients.push(socket);
        socket.once('error', reject);
      }),
      /** An accept, whose connection is tracked once it settles. */
      accept: async (): Promise<McplConnection> => {
        const conn = await McplConnection.acceptTcp(server);
        connections.push(conn);
        return conn;
      },
      /** Sockets the caller's own listener receives, tracked as they arrive. */
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
    // What a failed accept emits on a listening server.
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
    // After a successful accept: the case this change exposes. Before it,
    // the accept's leftover 'error' listener swallowed this error.
    await Promise.all([accept(), connect()]);
    const afterSuccess = failedAccept();
    assert.throws(() => server.emit('error', afterSuccess), (err) => err === afterSuccess);
    // After a failed accept: nothing of acceptTcp's remains to take it either.
    const failing = accept();
    const first = failedAccept();
    server.emit('error', first);
    await assert.rejects(failing, (err) => err === first);
    const afterFailure = failedAccept();
    assert.throws(() => server.emit('error', afterFailure), (err) => err === afterFailure);
  });

  it("keeps the caller's own listeners, and an error after an accept settles reaches only them", BOUNDED, async (t) => {
    const { server, connect, accept, track } = await listening(t);
    // The caller's own listeners, as an accept loop keeps them.
    const callerErrors: Error[] = [];
    const callerSockets: net.Socket[] = [];
    const onCallerError = (err: Error): void => { callerErrors.push(err); };
    const onCallerConnection = (socket: net.Socket): void => { callerSockets.push(socket); track(socket); };
    server.on('error', onCallerError);
    server.on('connection', onCallerConnection);
    await Promise.all([accept(), connect()]);
    assert.deepEqual(server.listeners('error'), [onCallerError]);
    assert.deepEqual(server.listeners('connection'), [onCallerConnection]);
    // A failed accept for a later client, with no accept pending: it reaches
    // the caller's listener, and emit doesn't throw.
    const failure = failedAccept();
    assert.doesNotThrow(() => server.emit('error', failure));
    assert.deepEqual(callerErrors, [failure]);
    assert.equal(callerSockets.length, 1, "the caller's connection listener saw the accepted socket too");
  });
});
