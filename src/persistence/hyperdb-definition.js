const COLLECTION_NAME = "@peardrops/transfers";

const transferCollection = {
  name: COLLECTION_NAME,
  id: 0,
  encodeKey(record = {}) {
    const id = String(record.id || "");
    if (!id) throw new Error("Transfer record id is required");
    return Buffer.from(id, "utf8");
  },
  encodeKeyRange({ gt, gte, lt, lte } = {}) {
    return {
      gt: gt ? Buffer.from(String(gt.id || ""), "utf8") : null,
      gte: gte ? Buffer.from(String(gte.id || ""), "utf8") : null,
      lt: lt ? Buffer.from(String(lt.id || ""), "utf8") : null,
      lte: lte ? Buffer.from(String(lte.id || ""), "utf8") : null,
    };
  },
  encodeValue(version, record = {}) {
    return Buffer.from(JSON.stringify(record), "utf8");
  },
  trigger: null,
  reconstruct(version, keyBuf, valueBuf) {
    const record = JSON.parse(valueBuf.toString("utf8"));
    record.id = keyBuf.toString("utf8");
    return record;
  },
  reconstructKey(keyBuf) {
    return { id: keyBuf.toString("utf8") };
  },
  indexes: [],
};

const definition = {
  version: 1,
  collections: [transferCollection],
  indexes: [],
  resolveCollection(name) {
    return name === COLLECTION_NAME ? transferCollection : null;
  },
  resolveIndex() {
    return null;
  },
};

module.exports = {
  COLLECTION_NAME,
  definition,
};
