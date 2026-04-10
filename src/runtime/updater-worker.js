let PearRuntime = null;

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
      if (!this.updater || typeof this.updater.applyUpdate !== "function") {
        return;
      }
      try {
        await this.updater.applyUpdate();
      } catch (error) {
        console.error("[peardrops:updater] apply update failed", error);
      }
    };

    this.updater.on("error", this._onError);
    this.updater.on("updated", this._onUpdated);
  }

  async close() {
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
}

module.exports = {
  UpdaterWorker,
};
