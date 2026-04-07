const { path } = require("../utils/runtime-compat");
const HyperDB = require("hyperdb");
const { COLLECTION_NAME, definition } = require("./hyperdb-definition");

class HyperdbPersistence {
  constructor(baseDir) {
    this.baseDir = baseDir;
    this.dbPath = path.join(baseDir, "metadata.rocks");
    this.db = null;
  }

  async ready() {
    this.db = HyperDB.rocks(this.dbPath, definition);
  }

  async listTransfers() {
    const stream = this.db.find(COLLECTION_NAME, {});
    const transfers = await stream.toArray();
    return transfers.sort(
      (a, b) => Number(b.createdAt || 0) - Number(a.createdAt || 0),
    );
  }

  async appendTransfer(record) {
    const persisted = {
      ...record,
      id: record.id || randomId(),
      createdAt: record.createdAt || Date.now(),
    };

    await this.db.insert(COLLECTION_NAME, persisted);
    await this.db.flush();

    return persisted;
  }

  async getTransferById(id) {
    const key = String(id || "");
    if (!key) return null;
    return this.db.get(COLLECTION_NAME, { id: key });
  }

  async close() {
    if (!this.db || typeof this.db.close !== "function") return;
    await this.db.close();
    this.db = null;
  }
}

function randomId() {
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}

module.exports = {
  HyperdbPersistence,
};
