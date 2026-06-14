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
  `McplCapabilities`, `FeatureSetDeclaration`, context/inference hook caps).
- **Errors** — `ConnectionClosedError`, `ConnectionTimeoutError`, and `ERR_*`
  protocol error codes.

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
