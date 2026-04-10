const { TransferBackend } = require("../services/transfer-backend");
const { RpcCommand, createRpcServer } = require("../rpc/hrpc-bridge");
const { UpdaterWorker } = require("../runtime/updater-worker");

async function bootstrapTransferWorker({
  ipc,
  baseDir,
  metadataDir = "",
  updaterConfig = {},
  relayUrl = "",
}) {
  if (!ipc || typeof ipc.write !== "function") {
    throw new Error("A worker IPC stream is required");
  }

  const backend = new TransferBackend({
    baseDir,
    metadataDir,
    relayUrl: relayUrl || updaterConfig.relayUrl || "",
  });

  await readyBackendWithLockRetry(backend);

  const updaterWorker = new UpdaterWorker(updaterConfig);
  let updaterError = null;
  let closing = false;
  let closePromise = null;

  const updaterReady = (async () => {
    try {
      await updaterWorker.ready({
        swarm: backend.swarm,
        store: backend.store,
      });

      if (
        updaterWorker.updater?.store &&
        updaterWorker.updater.store !== backend.store
      ) {
        backend.swarm.on("connection", (socket) => {
          if (socket && typeof socket.on === "function") {
            socket.on("error", (error) => onSwarmSocketError(error));
          }
          const replication = updaterWorker.updater.store.replicate(socket);
          if (replication && typeof replication.on === "function") {
            replication.on("error", (error) => onSwarmSocketError(error));
          }
        });
      }

      const updaterDiscoveryKey =
        updaterWorker.updater?.drive?.core?.discoveryKey;
      if (updaterDiscoveryKey) {
        backend.swarm.join(updaterDiscoveryKey, {
          client: true,
          server: false,
        });
      }
    } catch (error) {
      updaterError = error?.message || String(error);
    }
  })();

  const close = async () => {
    if (closePromise) return closePromise;
    closing = true;

    closePromise = (async () => {
      await updaterReady.catch(() => {});
      await updaterWorker.close();
      await backend.close();
    })();

    return closePromise;
  };

  const onIpcClosed = () => {
    void close("ipc closed or errored");
  };

  if (typeof ipc.on === "function") {
    ipc.on("close", onIpcClosed);
    ipc.on("error", onIpcClosed);
  }

  if (typeof process !== "undefined" && typeof process.once === "function") {
    const onSignal = () => {
      void close("process signal");
    };
    process.once("SIGINT", onSignal);
    process.once("SIGTERM", onSignal);
  }

  if (typeof globalThis.Bare !== "undefined" && typeof Bare.on === "function") {
    Bare.on("exit", () => {
      void close("bare exit");
    });
  }

  const guard = async (fn) => {
    if (closing) throw new Error("Worker is shutting down");
    return fn();
  };

  createRpcServer(ipc, {
    [RpcCommand.SHUTDOWN]: async () => {
      await close("rpc shutdown");
      return { closed: true };
    },
    [RpcCommand.INIT]: async () =>
      guard(async () => ({
        version: updaterConfig.version || "0.1.0",
        transfers: await backend.listTransfers(),
        updater: updaterWorker.status(),
        updaterError,
      })),
    [RpcCommand.LIST_TRANSFERS]: async () =>
      guard(async () => ({
        transfers: await backend.listTransfers(),
      })),
    [RpcCommand.CREATE_UPLOAD]: async (payload) =>
      guard(() => backend.createUpload(payload)),
    [RpcCommand.GET_MANIFEST]: async (payload) =>
      guard(() => backend.getManifest(payload)),
    [RpcCommand.DOWNLOAD]: async (payload) =>
      guard(() => backend.download(payload)),
    [RpcCommand.READ_ENTRY]: async (payload) =>
      guard(() => backend.readEntry(payload)),
    [RpcCommand.READ_ENTRY_CHUNK]: async (payload) =>
      guard(() => backend.readEntryChunk(payload)),
    [RpcCommand.LIST_ACTIVE_HOSTS]: async () =>
      guard(() => backend.listActiveHosts()),
    [RpcCommand.STOP_HOST]: async (payload) =>
      guard(() => backend.stopHost(payload)),
    [RpcCommand.START_HOST_FROM_TRANSFER]: async (payload) =>
      guard(() => backend.startHostFromTransfer(payload)),
  });

  return { backend, updaterWorker, close };
}

async function readyBackendWithLockRetry(backend) {
  let attempt = 0;
  let delayMs = 100;

  while (true) {
    try {
      await backend.ready();
      return;
    } catch (error) {
      if (!isStorageLockError(error) || attempt >= 25) throw error;
      attempt += 1;
      await sleep(delayMs);
      delayMs = Math.min(delayMs * 2, 2000);
    }
  }
}

function isStorageLockError(error) {
  const code = String(error?.code || "");
  const message = String(error?.message || "");
  return (
    code === "EIO" ||
    code === "EBUSY" ||
    code === "EAGAIN" ||
    message.includes("File descriptor could not be locked") ||
    (message.includes("LOCK") &&
      (message.includes("Resource temporarily unavailable") ||
        message.includes("could not be locked")))
  );
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

module.exports = {
  bootstrapTransferWorker,
};

function onSwarmSocketError(error) {
  if (isBenignConnectionError(error)) return;
  console.error("Updater swarm connection error:", error);
}

function isBenignConnectionError(error) {
  const code = String(error?.code || "");
  const message = String(error?.message || "");
  return (
    code === "ECONNRESET" ||
    code === "EPIPE" ||
    code === "ETIMEDOUT" ||
    message.includes("connection reset by peer") ||
    message.includes("stream was destroyed") ||
    message.includes("socket closed")
  );
}
