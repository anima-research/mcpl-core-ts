# @animalabs/mcpl-core

> Published on npm as **`@animalabs/mcpl-core`**. Inside the connectome
> ecosystem it's consumed as a sibling source checkout under the
> `@connectome/mcpl-core` dependency key (`file:../mcpl-core-ts`) — npm keys
> `file:` deps by name, so both work side by side.

TypeScript core for **MCPL** — an MCP-superset wire protocol that adds
server-initiated **push events**, **channels** (bidirectional, push-driven
message streams), conversation **branches**, inference hooks, host-managed
state, and capability / feature-set negotiation on top of plain JSON-RPC.

This package is the shared, transport-agnostic core used across the connectome
agents (relays, bridges, hosts). It contains no business logic — just the wire
contract and a connection primitive.

## What's in here

- **`McplConnection`** — a JSON-RPC 2.0 connection over a readable/writable
  stream pair (`McplConnection.fromStreams(stdin, stdout)`), with a pull-based
  `nextMessage()` for incoming requests/notifications and promise-based
  `sendRequest()` / `sendResponse()` / `sendNotification()` / `sendError()`.
  Over a stream, each message is one line, ended by `\n` (the last one also
  by the end of the input) and by nothing else, so a U+2028 or U+2029 inside
  a JSON string stays in its message; over a WebSocket (`fromWebSocket`),
  each text message is one message. It sends both characters escaped
  (`\u2028`, `\u2029`), so a peer whose line reader splits there, as Node
  24's readline does, still reads one message.
  It closes once: when either stream fails (a TCP reset, a WebSocket error, a
  child process's broken pipe), when its input ends or is destroyed, when its
  output is destroyed before it finished, or on `close()`. An output ended on
  purpose leaves the input to finish: the connection still reads, and sends
  nothing more. Closing takes effect before anything is reported: nothing
  more is sent or read, pending requests reject with `ConnectionClosedError`,
  and `nextMessage()` returns what was already queued and then rejects. Then
  the stream error that closed it is emitted, once, as `'error'`, and
  `'close'` follows; once it has closed, nothing more is read and a later
  stream error isn't reported. A malformed line is emitted as `'error'`,
  and so is an exception a `'request'` or `'notification'` listener throws:
  an Error as itself, and any other thrown value as the `cause` of an
  Error. Neither closes the connection, and the message a listener threw
  for still reaches `nextMessage()`. A request drained from
  the queue after the close can't be answered. A connection built on a stream that was already
  done is closed from the start, and reports it on a later turn of the event
  loop, to listeners added before anything else is awaited: that stream's
  error if it had one, then `'close'`. `close()` also releases the output,
  once (it destroys the output stream, and closes a WebSocket; an input
  given to `fromStreams` stays the caller's), even if a `'close'` listener
  throws, so call it after a close the peer caused, too.
- **`method`** — the method-name constants: `initialize`, `push/event`,
  `channels/*` (`list`, `open`, `close`, `publish`, `changed`, …),
  `branches/*`, `inference/*`, `state/*`, `model/info`, …
- **Typed params/results** for every method (e.g. `ChannelDescriptor`,
  `PushEventParams`, `ChannelsPublishParams`).
- **Content blocks** — `ContentBlock` (`text` / `image` / `audio`) plus helpers
  like `textContent(...)`.
- **Capabilities & feature sets** — negotiation types (`InitializeCapabilities`,
  `McplCapabilities`, `FeatureSetDeclaration`, context/inference hook caps), the
  closed `CAPABILITY_PATHS` vocabulary (SPEC §6.2), the recursive advertisement
  walk (`advertisedCapabilities`, §5.1), grant matching (`capabilityGranted`,
  §5.4) and fail-closed `uses` validation (`validateUses`, §6.4).
- **Manifest changes (§17 / RFC-003)** — `manifestDigest` (the canonical content
  digest), `changedDomains`, and `ManifestTracker`: call `setManifest(next)` and
  the digest, the diff, the per-connection `mcpl/manifestChanged` announcement
  and the `mcpl/manifest` snapshot all follow. Servers never hand-author an
  announcement.
- **Errors** — `ConnectionClosedError`, `ConnectionTimeoutError`, and `ERR_*`
  protocol error codes.

## Capability advertisement is a tree, and absence is denial

`channels`, `contextHooks.beforeInference` and `inferenceRequest` are **objects**
whose members are the leaves of the §6.2 vocabulary. A boolean `true` at any
level is shorthand for every leaf beneath it; `false` or absence means none.

```ts
const mcpl: McplCapabilities = {
  version: '0.5',
  channels: { register: true, publish: true, incoming: true, streaming: true },
  contextHooks: {
    beforeInference: { observe: false, inject: { beforeUser: true, afterUser: true } },
  },
};

advertisedCapabilities(mcpl);
// Set { 'channels.register', 'channels.publish', 'channels.incoming',
//       'channels.streaming',
//       'contextHooks.beforeInference.inject.beforeUser',
//       'contextHooks.beforeInference.inject.afterUser' }
```

That is an **input to** the host's grant computation, never an authorization.
`effectiveCapabilities` (§5.4) is the sole normative allowlist: every path not
present is denied. `deniedCapabilities` is diagnostics and MUST NOT reach an
authorization decision.

## Conformance vectors

`test/vectors/manifest-digest-vectors.json` is a verbatim copy of the frozen
interop vectors from
[`anima-research/mcpl`](https://github.com/anima-research/mcpl)
`conformance/manifest-digest-vectors.json`. It is not edited here — a vector
believed wrong is an issue against that repo. Run a newer checkout with:

```bash
MCPL_DIGEST_VECTORS=/path/to/mcpl/conformance/manifest-digest-vectors.json npm test
```

## Install

```bash
npm install @animalabs/mcpl-core
```

## Usage

```ts
import { McplConnection, ConnectionClosedError, textContent, method } from '@animalabs/mcpl-core';

const conn = McplConnection.fromStreams(process.stdin, process.stdout);

// Stream errors (a reset, a broken pipe), malformed lines and exceptions
// thrown by 'request' or 'notification' listeners arrive here. A stream
// destroyed without an error closes the connection with no 'error'.
// Like any EventEmitter, a connection with no 'error' listener throws them.
conn.on('error', (err) => console.error('mcpl:', err.message));

// server-initiated push, at any time while the connection is open:
conn.sendNotification(method.PUSH_EVENT, { /* … */ });

try {
  while (true) {
    let msg;
    try {
      msg = await conn.nextMessage();
    } catch (err) {
      // The connection closed: a stream ended or failed, or close() was called.
      if (err instanceof ConnectionClosedError) break;
      throw err;
    }
    if (msg.type === 'request' && msg.request.method === 'tools/list') {
      conn.sendResponse(msg.request.id, { tools: [] });
    }
  }
} finally {
  // Releases the output (stdout here), whichever side closed the connection.
  // The input stays this program's: the connection only stops reading it.
  conn.close();
}
```

## Build & test

```bash
npm install
npm run build   # tsc → dist/
npm test
```

Building needs Node 20.10 or later on the 20 line. TypeScript 7's `tsc` is
an extensionless file in an ES module package, which Node before 20.10
refuses to load. The published package runs on Node 20.0 and up, as
`engines` says, since its JavaScript is built already.

## License

MIT — see [LICENSE](./LICENSE).
