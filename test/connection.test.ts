/**
 * Integration tests for McplConnection.
 * Mirrors mcpl-core/tests/connection_test.rs
 */

import * as vm from 'node:vm';
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import * as net from 'node:net';
import { PassThrough, Readable, Writable } from 'node:stream';
import { performance } from 'node:perf_hooks';

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

/** The notifications a connection takes in, and its errors. */
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

    try {
      await assert.rejects(clientPromise, (err: Error) => {
        assert.ok(err.name === 'RpcError');
        assert.equal((err as RpcError).code, ERR_CHECKPOINT_NOT_FOUND);
        // The other end's message, as it sent it: the code is on the error, not in front of the text.
        assert.equal(err.message, 'Checkpoint not found');
        // No data was sent, so none is invented.
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

  it('request timeout: a ConnectionTimeoutError, still an RpcError with code -32000, that says the outcome is unknown', async () => {
    const [client, server] = await connectedPair();

    // Send a request with a very short timeout
    const promise = client.sendRequest('test/slow', {}, 50);

    // Don't respond — let it timeout
    try {
      await assert.rejects(promise, (err: Error) => {
        assert.ok(err instanceof ConnectionTimeoutError, 'a timeout is told apart by its class');
        assert.ok(err instanceof RpcError, 'a caller that catches RpcError still catches it');
        assert.equal(err.name, 'ConnectionTimeoutError');
        assert.equal(err.code, -32000);
        assert.equal((err as ConnectionTimeoutError).outcome, 'no-response', 'it may have run, as a close before the answer says');
        assert.equal(err.message, 'test/slow timed out: no reply came within 50 ms, so whether it ran is unknown. Check its effect before retrying, since a retry may repeat it.');
        assert.match(err.message, /whether it ran is unknown/);
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
      // close() destroys the output, so a write is seen where it's made.
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
        assert.equal(err.message, "Nothing was sent: this connection can no longer send (it has closed, or its output has ended), so the call didn't run. This connection doesn't reopen.");
        assert.match(err.message, /closed/, "mcpl-harness's fallback reads 'closed'");
        return true;
      });
      await turn();
      assert.deepEqual(written, [], 'nothing reached the output');
    });

    it('at a close, a pending request learns its outcome is unknown, and a waiting reader that no more messages will come', async () => {
      const [client, server] = await connectedPair();
      const pending = client.sendRequest('test/slow', {}, 0);
      const waiting = client.nextMessage();
      await server.nextMessage();
      client.close();

      try {
        await assert.rejects(pending, (err: Error) => {
          assert.ok(err instanceof ConnectionClosedError);
          assert.equal(err.outcome, 'no-response');
          assert.match(err.message, /closed before the call got an answer/);
          assert.match(err.message, /whether it ran is unknown/);
          return true;
        });
        await assert.rejects(waiting, (err: Error) => {
          assert.ok(err instanceof ConnectionClosedError);
          assert.equal(err.outcome, undefined, 'a reader made no call');
          assert.equal(err.message, 'The connection has closed, so no more messages will come.');
          return true;
        });
      } finally {
        server.close();
      }
    });

    it('nextMessage() on a closed connection with nothing queued says no more messages will come', async () => {
      const [client, server] = await connectedPair();
      client.close();

      try {
        await assert.rejects(client.nextMessage(), (err: Error) => {
          assert.ok(err instanceof ConnectionClosedError);
          assert.equal(err.outcome, undefined);
          assert.equal(err.message, 'The connection has closed, so no more messages will come.');
          return true;
        });
      } finally {
        server.close();
      }
    });

    it('an error response with an empty message, a null one, or none, names its code', { timeout: 5000 }, async () => {
      const toClient = new PassThrough();
      const fromClient = new PassThrough();
      const client = McplConnection.fromStreams(toClient, fromClient);
      const empty = client.sendRequest('test/empty', {}, 0);
      const absent = client.sendRequest('test/absent', {}, 0);
      const nulled = client.sendRequest('test/null', {}, 0);
      toClient.write(JSON.stringify({ jsonrpc: '2.0', id: 1, error: { code: -32603, message: '' } }) + '\n');
      toClient.write(JSON.stringify({ jsonrpc: '2.0', id: 2, error: { code: -32001 } }) + '\n');
      toClient.write(JSON.stringify({ jsonrpc: '2.0', id: 3, error: { code: -32002, message: null } }) + '\n');

      try {
        await assert.rejects(empty, (err: Error) => {
          assert.ok(err instanceof RpcError);
          assert.equal(err.code, -32603);
          assert.equal(err.message, 'The other end returned error -32603 with no message.');
          return true;
        });
        await assert.rejects(absent, (err: Error) => {
          assert.ok(err instanceof RpcError);
          assert.equal(err.code, -32001);
          assert.equal(err.message, 'The other end returned error -32001 with no message.');
          return true;
        });
        await assert.rejects(nulled, (err: Error) => {
          assert.ok(err instanceof RpcError);
          assert.equal(err.message, 'The other end returned error -32002 with no message.');
          return true;
        });
      } finally {
        client.close();
      }
    });

    it('an error response whose message is not text, or that is not an error object, settles its request with what came', { timeout: 5000 }, async () => {
      const toClient = new PassThrough();
      const fromClient = new PassThrough();
      const client = McplConnection.fromStreams(toClient, fromClient);
      // Each answer's error, the message it gives, and the code the RpcError carries: the number that came, or -32603.
      const answers: Array<[unknown, string, unknown]> = [
        [{ code: -32603, message: [] }, 'The other end returned error -32603 with a message that isn\'t text: []', -32603],
        [{ code: -32000, message: { detail: 'quota' } }, 'The other end returned error -32000 with a message that isn\'t text: {"detail":"quota"}', -32000],
        // A message String() can't convert: building the error must not throw, or the request would never settle.
        [{ code: -32000, message: { toString: 1, valueOf: 1 } }, 'The other end returned error -32000 with a message that isn\'t text: {"toString":1,"valueOf":1}', -32000],
        [{ message: '' }, 'The other end returned an error with no code and no message.', -32603],
        [{ message: [1] }, 'The other end returned an error with no code and a message that isn\'t text: [1]', -32603],
        [{ code: 'abc' }, 'The other end returned an error with the code "abc" and no message.', -32603],
        [{ code: -32601, message: 'Method not found' }, 'Method not found', -32601],
        ['boom', 'The other end returned an error that isn\'t a JSON-RPC error object: "boom"', -32603],
        [[], 'The other end returned an error that isn\'t a JSON-RPC error object: []', -32603],
        // A falsy error other than null is still an error: the request isn't resolved as a success.
        [0, 'The other end returned an error that isn\'t a JSON-RPC error object: 0', -32603],
      ];
      // Each request has a timeout, so one left unsettled fails here rather than hanging.
      const requests = answers.map((_, i) => client.sendRequest(`test/odd-${i}`, {}, 2000));
      answers.forEach(([error], i) => toClient.write(JSON.stringify({ jsonrpc: '2.0', id: i + 1, error }) + '\n'));
      try {
        for (const [i, [, text, code]] of answers.entries()) {
          await assert.rejects(requests[i], (err: Error) => {
            assert.ok(err instanceof RpcError);
            assert.ok(!(err instanceof ConnectionTimeoutError), 'settled by the answer, not the timeout');
            assert.equal(err.message, text);
            assert.equal(err.code, code);
            return true;
          });
        }
      } finally {
        client.close();
      }
    });

    it('a request whose params JSON can\'t encode is refused with a TypeError, and leaves nothing pending', async () => {
      const toClient = new PassThrough();
      const fromClient = new PassThrough();
      const client = McplConnection.fromStreams(toClient, fromClient);
      const written: string[] = [];
      fromClient.on('data', (chunk) => written.push(String(chunk)));
      try {
        await assert.rejects(client.sendRequest('test/bigint', { n: 1n }, 0), (err: Error) => {
          assert.ok(err instanceof TypeError);
          assert.match(err.message, /^Nothing was sent: test\/bigint's params can't be written as JSON/);
          assert.ok(err.cause instanceof TypeError, 'the serializer\'s own error is its cause');
          return true;
        });
        await turn();
        assert.deepEqual(written, [], 'nothing was written');
        assert.equal((client as unknown as { pending: Map<string, unknown> }).pending.size, 0, 'no pending entry is left');
      } finally {
        client.close();
      }
    });

    it('an error response nested deeper than JSON.stringify can write still settles its request', { timeout: 5000 }, async () => {
      const toClient = new PassThrough();
      const fromClient = new PassThrough();
      const client = McplConnection.fromStreams(toClient, fromClient);
      const depth = 100_000;
      // JSON.parse reads this nesting. Whether JSON.stringify can write it back
      // depends on the runtime: Node 22 and 24 overflow the stack, Node 26
      // doesn't. Either way the request settles with what came.
      const deep = '['.repeat(depth) + ']'.repeat(depth);
      const tooDeep = '(a value nested too deeply to show)';
      // The value at each place a value the other end sent is shown: the
      // message, the error itself, and the code. The text before it, and the
      // code the error carries.
      const sites: Array<[string, string, string, number]> = [
        [`{"code":-32000,"message":${deep}}`, "The other end returned error -32000 with a message that isn't text: ", '', -32000],
        [deep, "The other end returned an error that isn't a JSON-RPC error object: ", '', -32603],
        [`{"code":${deep}}`, 'The other end returned an error with the code ', ' and no message.', -32603],
      ];
      const requests = sites.map((_, i) => client.sendRequest(`test/deep-${i}`, {}, 2000));
      sites.forEach(([error], i) => toClient.write(`{"jsonrpc":"2.0","id":${i + 1},"error":${error}}\n`));
      try {
        for (const [i, [, before, after, code]] of sites.entries()) {
          await assert.rejects(requests[i], (err: Error) => {
            assert.ok(err instanceof RpcError);
            assert.ok(!(err instanceof ConnectionTimeoutError), 'settled by the answer, not the timeout');
            assert.ok(
              err.message === before + tooDeep + after || err.message === before + deep + after,
              `named, or shown whole: ${err.message.slice(0, 120)}`,
            );
            assert.equal(err.code, code);
            return true;
          });
        }
      } finally {
        client.close();
      }
    });

    it('a code that is not an integer is answered as -32603, and a message that is not text still names it', { timeout: 5000 }, async () => {
      const toClient = new PassThrough();
      const fromClient = new PassThrough();
      const client = McplConnection.fromStreams(toClient, fromClient);
      // JSON.parse reads 1e999 as Infinity, which JSON writes back as null:
      // it's shown as itself. The text, and the code the error carries.
      const lines: Array<[string, string, number]> = [
        ['{"code":1e999,"message":"boom"}', 'boom', -32603],
        ['{"code":1e999}', 'The other end returned error Infinity with no message.', -32603],
        ['{"code":1.5}', 'The other end returned error 1.5 with no message.', -32603],
        ['{"code":1.5,"message":{"x":1}}', 'The other end returned error 1.5 with a message that isn\'t text: {"x":1}', -32603],
        ['{"code":-32000,"message":1e999}', "The other end returned error -32000 with a message that isn't text: Infinity", -32000],
        ['-1e999', "The other end returned an error that isn't a JSON-RPC error object: -Infinity", -32603],
      ];
      const requests = lines.map((_, i) => client.sendRequest(`test/code-${i}`, {}, 2000));
      lines.forEach(([error], i) => toClient.write(`{"jsonrpc":"2.0","id":${i + 1},"error":${error}}\n`));
      try {
        for (const [i, [, text, code]] of lines.entries()) {
          await assert.rejects(requests[i], (err: Error) => {
            assert.ok(err instanceof RpcError);
            assert.equal(err.message, text);
            assert.equal(err.code, code);
            return true;
          });
        }
      } finally {
        client.close();
      }
    });

    it('a response whose error is null beside its result is a success, as some peers send one', { timeout: 5000 }, async () => {
      const toClient = new PassThrough();
      const fromClient = new PassThrough();
      const client = McplConnection.fromStreams(toClient, fromClient);
      const request = client.sendRequest('test/null-error', {}, 2000);
      toClient.write(JSON.stringify({ jsonrpc: '2.0', id: 1, result: { ok: true }, error: null }) + '\n');
      try {
        assert.deepEqual(await request, { ok: true });
      } finally {
        client.close();
      }
    });

    it('a response whose id is neither a number nor a string answers no request, and throws nothing', { timeout: 5000 }, async () => {
      const toClient = new PassThrough();
      const fromClient = new PassThrough();
      const client = McplConnection.fromStreams(toClient, fromClient);
      const errors: Error[] = [];
      client.on('error', (err) => errors.push(err));
      const request = client.sendRequest('test/ids', {}, 2000);
      // An object String() can't convert, and an array String() would turn into "1".
      toClient.write(JSON.stringify({ jsonrpc: '2.0', id: { toString: 1, valueOf: 1 }, error: { code: -32000, message: 'x' } }) + '\n');
      toClient.write(JSON.stringify({ jsonrpc: '2.0', id: [1], error: { code: -32000, message: 'array id' } }) + '\n');
      toClient.write(JSON.stringify({ jsonrpc: '2.0', id: 1, result: 'answered' }) + '\n');
      try {
        assert.equal(await request, 'answered', 'only the answer with the id that was sent settles it');
        assert.deepEqual(errors, []);
      } finally {
        client.close();
      }
    });

    it('a response that echoes the id as a string answers its request', { timeout: 5000 }, async () => {
      const toClient = new PassThrough();
      const fromClient = new PassThrough();
      const client = McplConnection.fromStreams(toClient, fromClient);
      const request = client.sendRequest('test/string-id', {}, 2000);
      toClient.write(JSON.stringify({ jsonrpc: '2.0', id: '1', result: 'echoed' }) + '\n');
      try {
        assert.equal(await request, 'echoed');
      } finally {
        client.close();
      }
    });

    it('params whose toJSON throws an Error from another realm are refused with its message', async () => {
      const toClient = new PassThrough();
      const fromClient = new PassThrough();
      const client = McplConnection.fromStreams(toClient, fromClient);
      const OtherError = vm.runInNewContext('Error') as ErrorConstructor;
      try {
        await assert.rejects(client.sendRequest('test/realm', { value: { toJSON() { throw new OtherError('from another realm'); } } }, 0), (err: Error) => {
          assert.ok(err instanceof TypeError);
          assert.equal(err.message, "Nothing was sent: test/realm's params can't be written as JSON (from another realm).");
          return true;
        });
      } finally {
        client.close();
      }
    });

    it("params whose toJSON throws something that isn't an Error are refused with a TypeError that says so", async () => {
      const toClient = new PassThrough();
      const fromClient = new PassThrough();
      const client = McplConnection.fromStreams(toClient, fromClient);
      try {
        await assert.rejects(client.sendRequest('test/odd-throw', { value: { toJSON() { throw 'no'; } } }, 0), (err: Error) => {
          assert.ok(err instanceof TypeError);
          assert.equal(err.message, "Nothing was sent: test/odd-throw's params can't be written as JSON (something that isn't an Error was thrown).");
          assert.equal(err.cause, 'no');
          return true;
        });
      } finally {
        client.close();
      }
    });

    it('a request whose output throws at once rejects with that error, and leaves nothing pending', async (t) => {
      const toClient = new PassThrough();
      const fromClient = new PassThrough();
      const client = McplConnection.fromStreams(toClient, fromClient);
      const failure = new Error('the output refused the write');
      fromClient.write = () => { throw failure; };
      const set = t.mock.method(globalThis, 'setTimeout');
      const cleared = t.mock.method(globalThis, 'clearTimeout');
      try {
        await assert.rejects(client.sendRequest('test/throwing-output', {}, 60_000), (err: Error) => err === failure);
        assert.equal((client as unknown as { pending: Map<string, unknown> }).pending.size, 0, 'no pending entry is left');
        const timer = set.mock.calls.find((c) => c.arguments[1] === 60_000)?.result;
        assert.ok(timer !== undefined, 'the request set its timer');
        assert.ok(cleared.mock.calls.some((c) => c.arguments[0] === timer), 'and cleared it');
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
        // A timer set past the limit would still be pending: fail on the next turn rather than hang.
        const settled = Promise.race([request, turn().then((): never => { throw new Error('still pending after the limit'); })]);
        await assert.rejects(settled, (err: Error) => {
          assert.ok(err instanceof ConnectionTimeoutError);
          assert.match(err.message, /no reply came within 2147483647 ms/);
          return true;
        });
      } finally {
        client.close();
      }
    });

    it('a ConnectionClosedError built with no arguments keeps the old message, and has no outcome', () => {
      const err = new ConnectionClosedError();
      assert.equal(err.message, 'Connection closed');
      assert.equal(err.name, 'ConnectionClosedError');
      assert.equal(err.outcome, undefined);
    });

    it('an RpcError keeps the message it is given, so passing one on adds nothing to it', () => {
      const err = new RpcError(-32602, 'bad params', { field: 'x' });
      assert.equal(err.message, 'bad params');
      assert.equal(err.code, -32602);
      assert.deepEqual(err.data, { field: 'x' });
      // What a handler answers with when it passes a caught error on.
      const passedOn = new RpcError(err.code, err.message, err.data);
      assert.equal(passedOn.message, 'bad params');
    });
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

  it('emits error on malformed JSON, and stays open', async () => {
    const { PassThrough } = await import('node:stream');

    const aToB = new PassThrough();
    const bToA = new PassThrough();

    const conn = McplConnection.fromStreams(aToB, bToA);

    const errors: Error[] = [];
    conn.on('error', (err) => errors.push(err));
    const reported = new Promise((resolve) => conn.once('error', resolve));

    // Write malformed JSON directly to the stream
    aToB.write('this is not json\n');

    await reported;
    // Anything else it set off has landed by the next turn.
    await new Promise((resolve) => setImmediate(resolve));

    assert.equal(errors.length, 1);
    assert.ok(errors[0].message.includes('Malformed JSON-RPC'));
    assert.equal(conn.isClosed, false, 'a malformed line leaves the connection open');

    // And the next valid line still arrives.
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

      // The next message is taken as usual.
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

  it('a listener that closes the connection leaves the message to a nextMessage() call already waiting', { timeout: 5000 }, async () => {
    const input = new PassThrough();
    const conn = McplConnection.fromStreams(input, new PassThrough());
    conn.on('notification', () => conn.close());
    const waiting = conn.nextMessage();
    input.write(JSON.stringify({ jsonrpc: '2.0', method: 'test/closing', params: {} }) + '\n');
    const msg = await waiting;
    assert.equal(msg.type === 'notification' && msg.notification.method, 'test/closing');
    assert.equal(conn.isClosed, true);
    await assert.rejects(conn.nextMessage(), { name: 'ConnectionClosedError' });
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

    /** The promise's outcome if it settles within a turn of the event loop; one still pending fails. */
    const withinATurn = <T>(p: Promise<T>): Promise<T> =>
      Promise.race([p, turn().then((): never => { throw new Error('still pending after a turn'); })]);

    /** A JSON-RPC notification as one line of the wire. */
    const line = (m: string) => JSON.stringify({ jsonrpc: '2.0', method: m, params: {} }) + '\n';

    /**
     * What reaches a stream from now on, read as it flows: one entry per
     * JSON-RPC line, its method, or `response <id>` for a response.
     */
    function capture(output: PassThrough): () => string[] {
      const chunks: string[] = [];
      output.on('data', (chunk) => chunks.push(String(chunk)));
      return () => chunks.join('').split('\n').filter(Boolean).map((text) => {
        const msg = JSON.parse(text) as { method?: string; id?: unknown };
        return msg.method ?? `response ${String(msg.id)}`;
      });
    }

    /** Every listener's attempt to send or read, as a listener of a closed connection would make it. */
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
      // 'close' comes on a later tick: the request refuses now, unwritten.
      const request = conn.sendRequest('test/x', {});
      assert.equal(conn.isClosed, false, "the output's 'close' hasn't come yet");
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

      // From end() on, before 'finish' and after it, a request rejects at
      // once and the other sends do nothing.
      const beforeFinish = conn.sendRequest('test/before-finish', {}, 0);
      conn.sendNotification('test/before-finish', {});
      await assert.rejects(withinATurn(beforeFinish), (err: Error) => {
        assert.ok(err instanceof ConnectionClosedError);
        assert.equal(err.outcome, 'not-sent');
        assert.match(err.message, /^Nothing was sent: .*its output has ended/);
        return true;
      });
      await outputClosed;
      await turn();
      assert.equal(conn.isClosed, false, 'still open: the peer may still be answering');
      await assert.rejects(withinATurn(conn.sendRequest('test/after-finish', {}, 0)), (err: Error) => {
        assert.ok(err instanceof ConnectionClosedError);
        assert.equal(err.outcome, 'not-sent');
        assert.match(err.message, /^Nothing was sent/);
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
      input.write(line('test/first') + line('test/second'));
      await turn();

      assert.deepEqual(heard, ['test/first']);
      const queued = await conn.nextMessage();
      assert.equal(queued.type === 'notification' && queued.notification.method, 'test/first');
      await assert.rejects(conn.nextMessage(), { name: 'ConnectionClosedError' });
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
      // Streams that failed, and whose errors were heard, before the hand-over.
      const failedInput = new PassThrough();
      failedInput.on('error', () => {});
      failedInput.destroy(new Error('input failed earlier'));
      const failedOutput = new PassThrough();
      failedOutput.on('error', () => {});
      failedOutput.destroy(new Error('output failed earlier'));
      await Promise.all([failedInput, failedOutput].map((s) => new Promise((resolve) => s.once('close', resolve))));
      // An input that ended but wasn't destroyed: its readable side ended, and
      // nothing destroyed it (autoDestroy off).
      const endedKeptInput = new PassThrough({ autoDestroy: false });
      endedKeptInput.resume();
      endedKeptInput.end();
      await new Promise((resolve) => endedKeptInput.once('end', resolve));
      assert.equal(endedKeptInput.readableEnded && !endedKeptInput.destroyed, true);
      // An output that finished, and then failed.
      const finishedThenFailedOutput = new PassThrough({ autoDestroy: false });
      finishedThenFailedOutput.resume();
      finishedThenFailedOutput.on('error', () => {});
      finishedThenFailedOutput.end();
      await new Promise((resolve) => finishedThenFailedOutput.once('finish', resolve));
      finishedThenFailedOutput.destroy(new Error('output failed after it finished'));
      await new Promise((resolve) => finishedThenFailedOutput.once('close', resolve));
      assert.equal(finishedThenFailedOutput.writableFinished && finishedThenFailedOutput.errored !== null, true);
      // Streams that failed and, with autoDestroy off, weren't destroyed: an
      // output whose write failed, and an input given a chunk it can't take.
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
      // No more connections; the one accepted is gone already, so nothing
      // keeps the server open, whatever the assertions find.
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
      // As acceptTcp hands over a connection: built in a 'connection'
      // listener, which resolves the promise the caller awaits.
      const destroyedInput = new PassThrough();
      destroyedInput.destroy();
      const server = new EventEmitter();
      const handedOver = new Promise<McplConnection>((resolve) => {
        server.once('connection', () => resolve(McplConnection.fromStreams(destroyedInput, new PassThrough())));
      });
      setImmediate(() => server.emit('connection'));

      const conn = await handedOver;
      const seen = watch(conn);
      await turn();
      await turn();
      assert.equal(seen.closes(), 1, "the caller that awaited it heard its 'close'");
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
      assert.equal(wire.join(''), '{"jsonrpc":"2.0","method":"test/separators","params":{"name":"one\\u2028two\\u2029three"}}\n',
        'each goes out as a JSON escape');
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

    it('over a stream, a character split between chunks is read whole, and a last line without \\n is read when the input ends', { timeout: 5000 }, async () => {
      const input = new PassThrough();
      const conn = McplConnection.fromStreams(input, new PassThrough());
      const seen = heard(conn);
      const bytes = Buffer.from(notification('test/split', { name: separators }) + '\n', 'utf8');
      const middle = bytes.indexOf(Buffer.from('\u2028', 'utf8')) + 1; // inside its three bytes
      input.write(bytes.subarray(0, middle));
      await turn();
      input.write(bytes.subarray(middle));
      const closed = new Promise((resolve) => conn.once('close', resolve));
      input.end(notification('test/last'));
      await closed;
      assert.deepEqual(seen.methods, ['test/split', 'test/last']);
      assert.deepEqual(seen.params[0], { name: separators });
      assert.deepEqual(seen.errors, []);
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
      // Raw separators, a '\r\n' ending, a multi-byte character, and a last
      // line that ends at the end of the input.
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
      // Two lines in one chunk, the first with raw separators; then a third
      // split inside its JSON, and again between its '\r' and its '\n'.
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

    it('over a stream, a 16 MiB line in 8 KiB chunks arrives whole within 2 s', { timeout: 30000 }, async () => {
      // Searching everything received so far at each chunk took seconds here;
      // searching each chunk once stays well inside the bound. The time is
      // from the first write to the message, without building the line.
      const input = new PassThrough();
      const conn = McplConnection.fromStreams(input, new PassThrough());
      const big = 'x'.repeat(16 * 1024 * 1024);
      const line = Buffer.from(notification('test/big', { big }) + '\n');
      const got = conn.nextMessage();
      const started = performance.now();
      for (let at = 0; at < line.length; at += 8 * 1024) input.write(line.subarray(at, at + 8 * 1024));
      const message = (await got) as { notification?: { method?: string; params?: { big?: string } } };
      const elapsed = performance.now() - started;
      conn.close();
      assert.equal(message.notification?.method, 'test/big');
      assert.equal(message.notification?.params?.big?.length, big.length);
      assert.ok(elapsed < 2000, `took ${Math.round(elapsed)} ms`);
    });

    it("over a stream, a blank line, or one holding only a '\\r', is no message and no error", { timeout: 5000 }, async () => {
      const input = new PassThrough();
      const conn = McplConnection.fromStreams(input, new PassThrough());
      const seen = heard(conn);
      input.write('\n\r\n' + notification('test/one') + '\n \t\n');
      await turn();
      conn.close();
      assert.deepEqual(seen.methods, ['test/one']);
      assert.deepEqual(seen.errors, []);
    });

    it("closing takes the connection's listeners off its input", { timeout: 5000 }, async () => {
      const input = new PassThrough();
      const conn = McplConnection.fromStreams(input, new PassThrough());
      assert.equal(input.listenerCount('data'), 1);
      assert.equal(input.listenerCount('end'), 1);
      const seen = heard(conn);
      conn.close();
      assert.equal(input.listenerCount('data'), 0, 'nothing more is taken in, even if something resumes the input');
      assert.equal(input.listenerCount('end'), 0);
      input.resume();
      input.write(notification('test/after') + '\n');
      await turn();
      assert.deepEqual(seen.methods, []);
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
      assert.throws(() => input.emit('end'), /Malformed JSON-RPC message/);
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
        assert.throws(() => input.emit('data', Buffer.from('{oops\n' + notification('test/behind') + '\n')), /Malformed JSON-RPC message/);
        if (ending) {
          // The input ends before that later turn: the end takes it first.
          input.emit('end');
          assert.deepEqual(methods, ['test/behind'], 'taken at the end');
          assert.equal(conn.isClosed, true);
          continue;
        }
        await turn();
        assert.deepEqual(methods, ['test/behind'], 'taken on a later turn');
        // The input is still read.
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
      assert.match(seen.errors[0]!, /Malformed JSON-RPC message/);
    });

    it('an input already done when it is wrapped delivers nothing it still held', { timeout: 5000 }, async () => {
      const line = notification('test/held') + '\n';
      const destroyed = new Readable({ read() {} });
      destroyed.push(line);
      destroyed.destroy();
      // A failed input that doesn't destroy itself on its error still holds the line.
      const failed = new Readable({ autoDestroy: false, read() {} });
      failed.push(line);
      const failedError = new Promise((resolve) => failed.once('error', resolve));
      failed.push(123 as unknown as string);
      await failedError;
      assert.equal(failed.readableLength > 0, true, 'the failed input still holds the line');
      for (const input of [destroyed, failed]) {
        const conn = McplConnection.fromStreams(input, new PassThrough());
        const seen = heard(conn);
        // Closed from the start, so this rejects at once: settle it now.
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
      /** An input a test pushes into by hand, and a connection that takes it. */
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
        // 'a' pushes the end of 'c' and all of 'd' while 'b' and the start
        // of 'c' are still in the chunk that carried 'a'.
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
        assert.throws(() => ws.emit('message', json('first')), /Malformed JSON-RPC message/);
        // The socket closes before the later turn that would take it.
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
