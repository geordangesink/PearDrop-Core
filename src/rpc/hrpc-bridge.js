const RPC = require("bare-rpc");

const RpcCommand = {
  INIT: 0,
  LIST_TRANSFERS: 1,
  CREATE_UPLOAD: 2,
  GET_MANIFEST: 3,
  DOWNLOAD: 4,
  SHUTDOWN: 5,
  READ_ENTRY: 6,
  LIST_ACTIVE_HOSTS: 7,
  STOP_HOST: 8,
  START_HOST_FROM_TRANSFER: 9,
};

function createRpcServer(ipc, handlers) {
  return new RPC(ipc, async (req) => {
    try {
      const h = handlers[req.command];
      if (!h) throw new Error(`Unknown RPC command: ${req.command}`);

      const parsed = req.data ? JSON.parse(req.data.toString("utf8")) : {};
      const result = await h(parsed);
      req.reply(
        Buffer.from(JSON.stringify({ ok: true, result: result || {} }), "utf8"),
      );
    } catch (error) {
      const message = error && error.message ? error.message : String(error);
      req.reply(
        Buffer.from(JSON.stringify({ ok: false, error: message }), "utf8"),
      );
    }
  });
}

function createRpcClient(ipc) {
  const rpc = new RPC(ipc, () => {});

  return {
    async request(command, payload = {}) {
      const req = rpc.request(command);
      req.send(Buffer.from(JSON.stringify(payload), "utf8"));
      const response = await req.reply();
      const parsed = JSON.parse(response.toString("utf8"));
      if (parsed && parsed.ok === false) {
        throw new Error(parsed.error || "RPC request failed");
      }
      return parsed && parsed.ok === true ? parsed.result : parsed;
    },
  };
}

module.exports = {
  RpcCommand,
  createRpcServer,
  createRpcClient,
};
