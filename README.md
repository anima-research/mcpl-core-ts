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
import { McplConnection, textContent, method } from '@animalabs/mcpl-core';

const conn = McplConnection.fromStreams(process.stdin, process.stdout);

while (!conn.isClosed) {
  const msg = await conn.nextMessage();
  if (msg.type === 'request' && msg.request.method === 'tools/list') {
    conn.sendResponse(msg.request.id, { tools: [] });
  }
}

// server-initiated push:
conn.sendNotification(method.PUSH_EVENT, { /* … */ });
```

## Build & test

```bash
npm install
npm run build   # tsc → dist/
npm test
```

## License

MIT — see [LICENSE](./LICENSE).
