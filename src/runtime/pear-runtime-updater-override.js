const { fs, path } = require("../utils/runtime-compat");
const { arch, platform } = require("which-runtime");
const Localdrive = require("localdrive");
const { SquirrelManager } = require("./squirrel-manager");

const host = `${platform}-${arch}`;

function overrideApplyUpdate(updater, opts = {}) {
  opts = normalizeOptions(opts);
  const squirrel = new SquirrelManager(opts);

  if (!updater || typeof updater.applyUpdate !== "function") return updater;
  if (updater.applyUpdate.__pearDropCoreOverride) return updater;

  const originalApplyUpdate = updater.applyUpdate.bind(updater);

  updater.applyUpdate = async function () {
    if (!isSquirrelUpdate(this, opts, squirrel)) {
      return originalApplyUpdate();
    }

    if (!this.updated || this.applied || !this.bundled || this.applying) return;
    this.applying = true;

    try {
      const nextApp = getNextApp(this);
      const payload = await getSquirrelPayload(this, nextApp, opts, squirrel);
      await squirrel.apply(payload, {
        updater: this,
        installerArgs: opts.installerArgs,
        updateArgs: opts.updateArgs,
        updateExe: opts.updateExe,
      });

      this.applied = true;
      await fs.promises.rm(this.next, { recursive: true, force: true });
    } finally {
      this.applying = false;
    }
  };

  updater.applyUpdate.__pearDropCoreOverride = true;
  return updater;
}

function normalizeOptions(opts) {
  if (opts === true) return { enabled: true };
  return opts || {};
}

function isSquirrelUpdate(updater, opts, squirrel) {
  if (platform !== "win32") return false;
  if (!updater || !updater.next || !updater.name) return false;
  if (opts.enabled === true) return true;
  return squirrel.isSquirrelName(updater.name);
}

function getNextApp(updater) {
  return path.join(updater.next, "by-arch", host, "app", updater.name);
}

async function getSquirrelPayload(updater, nextApp, opts, squirrel) {
  const payload = await squirrel.payloadFromPath(nextApp);
  if (payload) return payload;

  return stageSquirrelPayloadFromCheckout(updater, nextApp, opts, squirrel);
}

async function stageSquirrelPayloadFromCheckout(
  updater,
  nextApp,
  opts,
  squirrel,
) {
  if (!updater?.drive || !updater?.next) throwMissingPayload(nextApp);

  const checkout = updater.drive.checkout(
    updater.length || updater.drive.core.length,
  );
  const local = new Localdrive(updater.next);
  const appRoot = `/by-arch/${host}/app`;

  try {
    const candidate = await squirrel.findCandidate(checkout, appRoot, opts);
    if (!candidate) throwMissingPayload(nextApp);

    for await (const data of checkout.mirror(local, {
      prefix: candidate.prefix,
    })) {
      updater.emit("updating-delta", data);
    }

    const payload = await squirrel.payloadFromPath(
      path.join(updater.next, candidate.key),
    );
    if (!payload) throwMissingPayload(path.join(updater.next, candidate.key));
    return payload;
  } finally {
    await checkout.close();
    await local.close();
  }
}

function throwMissingPayload(nextApp) {
  throw new Error(`Squirrel update payload not found at ${nextApp}`);
}

module.exports = {
  overrideApplyUpdate,
};
