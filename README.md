<div align="center">
  <h1>PearDrop Core</h1>
  <p><strong>The shared engine behind PearDrop.</strong></p>
  <p>Peer-to-peer transfers, invites, and worker communication<br />for the desktop and mobile apps.</p>
  <p><a href="https://peardrop.online">Website</a> · <a href="https://github.com/geordangesink/PearDrop-Desktop">Desktop</a> · <a href="LICENSE">Apache-2.0</a></p>
</div>

## Install

Add the core module to your application:

```sh
npm install pear-drop-core
```

This is a CommonJS library used by the PearDrop apps. The transfer worker runs in their Bare/Pear runtime; use an app's worker entry point as a reference when integrating it.

## What lives here

- **Transfers** — hosting and downloading through Hyperdrive, Corestore, and Hyperswarm.
- **Invites** — creating and parsing `peardrops://` links.
- **Worker RPC** — communication between the app interface and transfer runtime.
- **Persistence** — transfer metadata backed by HyperDB.
- **Updates** — shared runtime update handling.

## API

Import the public API from `pear-drop-core`:

| Export | Purpose |
| --- | --- |
| `bootstrapTransferWorker` | Initialize the backend, updater, and RPC handlers for an app worker. |
| `TransferBackend` | Manage transfer sessions and their storage and network connections. |
| `createInvite`, `parseInvite` | Create invite links and read their connection details. |
| `RpcCommand` | Shared command identifiers for worker communication. |
| `createRpcServer`, `createRpcClient` | Connect RPC handlers and callers over an IPC stream. |
| `UpdaterWorker` | Manage runtime updates inside the worker. |

### Start a transfer worker

Call this from a Bare worker with an IPC stream and writable storage paths:

```js
const { bootstrapTransferWorker } = require("pear-drop-core");

async function main() {
  await bootstrapTransferWorker({
    ipc: Bare.IPC,
    baseDir: "/absolute/path/to/transfers",
    metadataDir: "/absolute/path/to/metadata",
  });
}

main().catch(console.error);
```

`ipc` and `baseDir` are required. Optional settings include `metadataDir`, `relayUrl`, and `updaterConfig`. The host app supplies platform-specific storage paths, updater settings, and lifecycle handling.

For a complete integration, see the [desktop worker](https://github.com/geordangesink/PearDrop-Desktop/blob/main/workers/main.js).

## Development

```sh
git clone https://github.com/geordangesink/peardrop-core.git
cd peardrop-core
npm install
npm run lint
```

### Source layout

- `src/index.js` — public exports.
- `src/services/` — transfer backend.
- `src/worker/` — worker startup and command handlers.
- `src/rpc/` — IPC bridge and command definitions.
- `src/persistence/` — metadata storage and schema.
- `src/runtime/` — updater and runtime integration.
- `src/utils/` — invite helpers and runtime compatibility.

### Verify changes in an app

To try local core changes, keep the core and app repositories beside each other. From the desktop or mobile app directory, run:

```sh
npm install --no-save --package-lock=false ../peardrop-core
```

Then run the app using its README. For mobile worker changes, regenerate the worker bundle with `npm run bundle:bare` before rebuilding. Check both desktop and mobile when changing shared behavior.

This repository currently has no test script. Run `npm run lint` here and the relevant tests in the consuming apps. In your pull request, describe the transfer scenarios and platforms you verified.

## Contributing

Pull requests are welcome, including bug fixes, performance improvements, documentation, and compatibility fixes.

1. Fork the repository and create a focused branch.
2. Make your change and run `npm run lint`.
3. Verify affected behavior in the consuming apps.
4. Open a pull request explaining the change and how you tested it.

Keep app-specific UI and platform setup in the app repositories. For larger core changes or public API changes, open an issue first to discuss the approach. Bug reports should include your runtime, platform, reproduction steps, and relevant logs.

## License

[Apache-2.0](LICENSE)
