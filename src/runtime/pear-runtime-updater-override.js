const { fs, path } = require("../utils/runtime-compat");
const { arch, platform } = require("which-runtime");
const Localdrive = require("localdrive");

const host = `${platform}-${arch}`;
const SQUIRREL_EXTENSIONS = new Set([".nupkg"]);

function overrideApplyUpdate(updater, opts = {}) {
  opts = normalizeOptions(opts);

  if (!updater || typeof updater.applyUpdate !== "function") return updater;
  if (updater.applyUpdate.__pearDropCoreOverride) return updater;

  const originalApplyUpdate = updater.applyUpdate.bind(updater);

  updater.applyUpdate = async function () {
    if (!isSquirrelUpdate(this, opts)) {
      return originalApplyUpdate();
    }

    if (!this.updated || this.applied || !this.bundled || this.applying) return;
    this.applying = true;

    try {
      const nextApp = getNextApp(this);
      const payload = await getSquirrelPayload(this, nextApp, opts);

      if (payload.type === "installer") {
        await run(payload.file, opts.installerArgs || ["--silent"]);
      } else {
        const updateExe = getSquirrelUpdateExe(this, opts);
        await run(updateExe, [
          ...(opts.updateArgs || ["--update"]),
          payload.feed,
        ]);
      }

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

function isSquirrelUpdate(updater, opts) {
  if (platform !== "win32") return false;
  if (!updater || !updater.next || !updater.name) return false;
  if (opts.enabled === true) return true;

  const ext = path.extname(updater.name).toLowerCase();
  if (SQUIRREL_EXTENSIONS.has(ext)) return true;
  if (updater.name.toLowerCase() === "releases") return true;
  if (updater.name.toLowerCase().includes("squirrel")) return true;

  return false;
}

function getNextApp(updater) {
  return path.join(updater.next, "by-arch", host, "app", updater.name);
}

async function getSquirrelPayload(updater, nextApp, opts) {
  const payload = await getLocalSquirrelPayload(nextApp);
  if (payload) return payload;

  return stageSquirrelPayloadFromCheckout(updater, nextApp, opts);
}

async function getLocalSquirrelPayload(nextApp) {
  const stat = await statIfExists(nextApp);
  if (!stat) return null;
  if (stat.isDirectory()) return { type: "feed", feed: nextApp };

  const parent = path.dirname(nextApp);
  const ext = path.extname(nextApp).toLowerCase();

  if (ext === ".nupkg") return { type: "feed", feed: parent };
  if (ext === ".exe") {
    return { type: "installer", file: nextApp };
  }

  throw new Error(`unsupported Squirrel update payload: ${nextApp}`);
}

async function stageSquirrelPayloadFromCheckout(updater, nextApp, opts) {
  if (!updater?.drive || !updater?.next) throwMissingPayload(nextApp);

  const checkout = updater.drive.checkout(
    updater.length || updater.drive.core.length,
  );
  const local = new Localdrive(updater.next);
  const appRoot = `/by-arch/${host}/app`;

  try {
    const candidate = await findSquirrelCandidate(checkout, appRoot, opts);
    if (!candidate) throwMissingPayload(nextApp);

    for await (const data of checkout.mirror(local, {
      prefix: candidate.prefix,
    })) {
      updater.emit("updating-delta", data);
    }

    const payload = await getLocalSquirrelPayload(
      path.join(updater.next, candidate.key),
    );
    if (!payload) throwMissingPayload(path.join(updater.next, candidate.key));
    return payload;
  } finally {
    await checkout.close();
    await local.close();
  }
}

async function findSquirrelCandidate(checkout, appRoot, opts) {
  const requested = normalizedName(opts.name);
  const candidates = [];

  for await (const entry of checkout.list(appRoot)) {
    const key = entry.key || String(entry);
    const basename = path.basename(key).toLowerCase();
    const ext = path.extname(key).toLowerCase();
    const parent = path.dirname(key).replace(/\\/g, "/");

    if (basename === "releases") {
      candidates.push({
        key: parent,
        prefix: parent,
        score: scoreCandidate(parent, requested),
      });
    } else if (ext === ".nupkg") {
      candidates.push({
        key: parent,
        prefix: parent,
        score: scoreCandidate(parent, requested),
      });
    } else if (ext === ".exe") {
      candidates.push({
        key,
        prefix: key,
        score: scoreCandidate(key, requested) + 1,
      });
    }
  }

  candidates.sort((a, b) => b.score - a.score || a.key.localeCompare(b.key));
  return candidates[0] || null;
}

function scoreCandidate(key, requested) {
  const normalized = normalizedName(key);
  let score = 0;
  if (normalized.includes("squirrel")) score += 10;
  if (requested && normalized.includes(requested)) score += 5;
  if (normalized.includes("setup")) score += 2;
  return score;
}

function normalizedName(name) {
  return String(name || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "");
}

async function statIfExists(file) {
  try {
    return await fs.promises.stat(file);
  } catch (error) {
    if (error && error.code === "ENOENT") return null;
    throw error;
  }
}

function throwMissingPayload(nextApp) {
  throw new Error(`Squirrel update payload not found at ${nextApp}`);
}

function getSquirrelUpdateExe(updater, opts) {
  const candidates = [
    opts.updateExe,
    updater.squirrelUpdateExe,
    getProcess()?.env?.SQUIRREL_UPDATE_EXE,
    fromProcessExecPath(),
    fromAppPath(updater.app),
  ].filter(Boolean);

  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) return candidate;
  }

  throw new Error("Squirrel Update.exe not found");
}

function fromProcessExecPath() {
  const process = getProcess();
  if (!process?.execPath) return null;

  const appVersionDir = path.dirname(process.execPath);
  return path.join(path.dirname(appVersionDir), "Update.exe");
}

function fromAppPath(app) {
  if (!app) return null;

  const appDir = path.extname(app) ? path.dirname(app) : app;
  return path.join(path.dirname(appDir), "Update.exe");
}

function run(command, args) {
  return new Promise((resolve, reject) => {
    const spawn = getSpawn();
    const child = spawn(command, args, {
      stdio: "ignore",
      windowsHide: true,
    });

    child.once("error", reject);
    child.once("exit", (code, signal) => {
      if (code === 0) return resolve();
      reject(
        new Error(
          `${path.basename(command)} failed with ${
            signal || `exit code ${code}`
          }`,
        ),
      );
    });
  });
}

function getProcess() {
  return typeof globalThis.process !== "undefined" ? globalThis.process : null;
}

function getSpawn() {
  if (typeof globalThis.Bare !== "undefined") {
    try {
      return require("bare-subprocess").spawn;
    } catch {
      throw new Error("Squirrel updates require bare-subprocess support");
    }
  }

  try {
    return eval("require")("child_process").spawn;
  } catch {
    throw new Error("Squirrel updates require Node.js child_process support");
  }
}

module.exports = {
  overrideApplyUpdate,
};
