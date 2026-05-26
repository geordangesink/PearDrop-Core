let PearRuntime = null;
const { overrideApplyUpdate } = require("./pear-runtime-updater-override");
const UPDATE_APPLIED_TOKEN = "__PEARDROP_UPDATE_APPLIED__";
const UPDATE_READY_TOKEN = "__PEARDROP_UPDATE_READY__";

try {
  PearRuntime = require("#pear");
} catch {
  PearRuntime = null;
}

class UpdaterWorker {
  constructor(config = {}, PearClass = PearRuntime) {
    this.config = config;
    this.PearClass = PearClass;
    this.pear = null;
    this.updater = null;
    this._onUpdated = null;
    this._onError = null;
    this._applyPromise = null;
  }

  isEnabled() {
    const upgrade = this.config?.upgrade;
    if (!upgrade || String(upgrade) === "pear://updates-disabled") return false;
    return this.config?.updates !== false;
  }

  async ready({ swarm, store } = {}) {
    if (!this.PearClass || !this.isEnabled()) return false;

    const runtimeConfig = {
      ...this.config,
      swarm,
      store,
    };

    this.pear = new this.PearClass(runtimeConfig);
    if (typeof this.pear.ready === "function") {
      await this.pear.ready();
    }

    this.updater = this.pear.updater || null;
    overrideApplyUpdate(this.updater, this.config?.squirrel);
    this._bindUpdaterEvents();
    return true;
  }

  status() {
    return {
      enabled: this.isEnabled(),
      active: Boolean(this.updater),
      version: this.updater?.version ? String(this.updater.version) : null,
    };
  }

  _bindUpdaterEvents() {
    if (!this.updater || typeof this.updater.on !== "function") return;

    this._onError = (error) => {
      console.error("[peardrops:updater] updater error", error);
    };

    this._onUpdated = async () => {
      if (this._shouldApplyOnClose()) {
        console.log(UPDATE_READY_TOKEN);
        return;
      }
      await this._applyPendingUpdate("updated");
    };

    this.updater.on("error", this._onError);
    this.updater.on("updated", this._onUpdated);
  }

  async close() {
    if (this._shouldApplyOnClose()) {
      await this._applyPendingUpdate("close");
    }

    if (this.updater && typeof this.updater.off === "function") {
      if (this._onError) this.updater.off("error", this._onError);
      if (this._onUpdated) this.updater.off("updated", this._onUpdated);
    }

    this._onError = null;
    this._onUpdated = null;
    this.updater = null;

    if (this.pear && typeof this.pear.close === "function") {
      await this.pear.close();
    }
    this.pear = null;
  }

  _isWindowsPlatform() {
    const platform = String(this.config?.platform || "")
      .trim()
      .toLowerCase();
    if (platform.startsWith("win")) return true;
    return Boolean(this.config?.squirrel);
  }

  _shouldApplyOnClose() {
    return this._isWindowsPlatform();
  }

  async _applyPendingUpdate(reason = "unknown") {
    if (!this.updater || typeof this.updater.applyUpdate !== "function") return;

    if (
      !this.updater.updated ||
      this.updater.applied ||
      !this.updater.bundled
    ) {
      return;
    }

    if (this._applyPromise) {
      await this._applyPromise;
      return;
    }

    this._applyPromise = (async () => {
      try {
        await this.updater.applyUpdate();
        console.log(UPDATE_APPLIED_TOKEN);
      } catch (error) {
        console.error(
          `[peardrops:updater] apply update failed (${reason})`,
          error,
        );
      }
    })();

    try {
      await this._applyPromise;
    } finally {
      this._applyPromise = null;
    }
  }
}

module.exports = {
  UpdaterWorker,
  UPDATE_APPLIED_TOKEN,
  UPDATE_READY_TOKEN,
};
