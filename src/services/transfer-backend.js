const { fs, path } = require("../utils/runtime-compat");
const b4a = require("b4a");
const Corestore = require("corestore");
const Hyperdrive = require("hyperdrive");
const Hyperswarm = require("hyperswarm");
const FlockManager = require("flockmanager");
const DHT = require("@hyperswarm/dht-relay");
const RelayStream = require("@hyperswarm/dht-relay/ws");
const { HyperdbPersistence } = require("../persistence/hyperdb-persistence");
const { createInvite, parseInvite } = require("../utils/invite");

class TransferBackend {
  constructor({
    baseDir,
    metadataDir = "",
    relayUrl = "",
    uploadProgress = null,
    swarmOptions = {},
    resolveWaitMs = 25000,
    resolveRetryMs = 150,
    flockJoinWaitMs = 8000,
  }) {
    this.baseDir = baseDir;
    this.relayUrl = relayUrl;
    this.swarmOptions = swarmOptions;
    this.resolveWaitMs = resolveWaitMs;
    this.resolveRetryMs = resolveRetryMs;
    this.flockJoinWaitMs = flockJoinWaitMs;
    this.store = null;
    this.inviteStore = null;
    this.swarm = null;
    this.metadataDir = metadataDir || path.join(baseDir, "db");
    this.inviteCacheDir = path.join(baseDir, "invite-cache");
    this.persistence = new HyperdbPersistence(this.metadataDir);
    this.liveDrives = new Map();
    this.inviteDrives = new Map();
    this.driveDiscoveries = new Map();
    this.liveFlocks = new Map();
    this.liveHosts = new Map();
    this.webHosts = new Map();
    this.flockManager = null;
    this.uploadProgress =
      typeof uploadProgress === "function" ? uploadProgress : null;
  }

  async ready() {
    await fs.promises.mkdir(this.baseDir, { recursive: true });
    await fs.promises.mkdir(this.metadataDir, { recursive: true });
    await fs.promises.mkdir(this.inviteCacheDir, { recursive: true });
    this.store = new Corestore(path.join(this.baseDir, "corestore"));
    this.inviteStore = new Corestore(
      path.join(this.inviteCacheDir, "corestore"),
    );
    await this.store.ready();
    await this.inviteStore.ready();
    const keyPair = await this.store.createKeyPair("peardrops-swarm");
    this.swarm = new Hyperswarm({ keyPair, ...this.swarmOptions });
    this.swarm.on("connection", (socket) => {
      attachSocketErrorHandler(socket);
      const replications = [
        this.store?.replicate?.(socket),
        this.inviteStore?.replicate?.(socket),
      ];
      for (const replication of replications) {
        if (replication && typeof replication.on === "function") {
          replication.on("error", (error) => onBenignConnectionError(error));
        }
      }
    });
    this.flockManager = new FlockManager(null, {
      swarm: this.swarm,
      store: this.store,
    });
    await this.flockManager.ready();
    await this.persistence.ready();
  }

  listTransfers() {
    return this.persistence.listTransfers();
  }

  async createUpload({ files, sessionName = "", nativeInvite = "" }) {
    if (!Array.isArray(files) || files.length === 0) {
      throw new Error("At least one file is required");
    }

    const transferId = randomId();
    const drive = new Hyperdrive(
      this.store.namespace(`transfer-${transferId}`),
    );
    await drive.ready();

    const fileManifest = await this._writeFilesAndManifest({ drive, files });

    const hosted = await this._startHostingDrive({
      drive,
      transferId,
      fileManifest,
      sessionName,
      nativeInvite,
    });
    return hosted;
  }

  async updateActiveHost({ invite, files, sessionName = "" }) {
    const key = String(invite || "");
    if (!key) throw new Error("Invite is required");
    if (!Array.isArray(files) || files.length === 0) {
      throw new Error("At least one file is required");
    }

    let host = this.liveHosts.get(key);
    if (!host) {
      const parsed = parseInvite(key);
      host = Array.from(this.liveHosts.values()).find(
        (item) => item.roomInvite === parsed.roomInvite,
      );
    }
    if (!host) throw new Error("Active host session not found");

    const drive = await this._attachDrive(host.driveKey);
    const fileManifest = await this._writeFilesAndManifest({ drive, files });

    const nextSessionName =
      String(sessionName || host.sessionName || "").trim() || "Host Session";

    host.fileManifest = fileManifest;
    host.sessionName = nextSessionName;

    this.liveHosts.set(host.invite, host);

    return {
      invite: host.invite,
      nativeInvite: host.invite,
      webSwarmLink: host.webSwarmLink || "",
      manifest: fileManifest,
      hostSession: {
        invite: host.invite,
        sessionName: host.sessionName,
        sessionLabel: host.sessionLabel || "",
      },
    };
  }

  listActiveHosts() {
    this._pruneZombieHosts();
    const hosts = Array.from(this.liveHosts.values())
      .map((host) => ({
        transferId: host.transferId,
        invite: host.invite,
        webSwarmLink: host.webSwarmLink || "",
        driveKey: host.driveKey,
        roomInvite: host.roomInvite,
        sessionName: host.sessionName,
        sessionLabel: host.sessionLabel,
        createdAt: host.createdAt,
        fileCount: host.fileManifest.length,
        totalBytes: host.fileManifest.reduce(
          (sum, item) => sum + Number(item.byteLength || 0),
          0,
        ),
        manifest: host.fileManifest,
        online: host?.flock?.closed !== true,
      }))
      .sort((a, b) => Number(b.createdAt || 0) - Number(a.createdAt || 0));
    return { hosts };
  }

  async stopHost({ invite }) {
    const key = String(invite || "");
    if (!key) throw new Error("Invite is required");
    let host = this.liveHosts.get(key);
    if (!host) {
      try {
        const parsed = parseInvite(key);
        host = Array.from(this.liveHosts.values()).find(
          (item) => item.roomInvite === parsed.roomInvite,
        );
      } catch {}
    }
    if (!host) return { stopped: false };

    let closeError = null;
    try {
      if (host.flock && typeof host.flock.close === "function") {
        await host.flock.close();
      }
    } catch (error) {
      closeError = error || new Error("Failed closing host flock");
    } finally {
      this.liveFlocks.delete(host.roomInvite);
      this.liveHosts.delete(host.invite);

      if (host.topicHex && !this._isTopicInUse(host.topicHex)) {
        const webHost = this.webHosts.get(host.topicHex);
        if (webHost) {
          try {
            await webHost.discovery.destroy();
          } catch {}
          try {
            await webHost.swarm.destroy();
          } catch {}
        }
        this.webHosts.delete(host.topicHex);
      }
    }

    return {
      stopped: true,
      invite: host.invite,
      warning: closeError ? String(closeError.message || closeError) : "",
    };
  }

  async startHostFromTransfer({ transferId, sessionName = "" }) {
    const transfer = await this.persistence.getTransferById(transferId);
    if (!transfer) throw new Error("Transfer not found");
    if (!transfer.driveKey) {
      throw new Error("Transfer does not include a drive key");
    }
    if (!transfer.invite) {
      throw new Error("Transfer does not include an invite");
    }

    if (this.liveHosts.has(transfer.invite)) {
      const host = this.liveHosts.get(transfer.invite);
      return {
        invite: host.invite,
        nativeInvite: host.invite,
        webSwarmLink: host.webSwarmLink || "",
        transfer,
        manifest: host.fileManifest || [],
        hostSession: {
          invite: host.invite,
          sessionName:
            host.sessionName || transfer.sessionName || "Host Session",
          sessionLabel: host.sessionLabel || transfer.sessionLabel || "",
        },
      };
    }

    const drive = await this._attachDrive(transfer.driveKey);
    const manifest =
      transfer.manifest ||
      (await this.getManifest({ invite: transfer.invite }));
    const reusedSessionName =
      String(transfer.sessionName || "").trim() ||
      String(sessionName || "").trim() ||
      "Host Session";
    const reusedSessionLabel =
      String(transfer.sessionLabel || "").trim() ||
      formatHostSessionLabel(reusedSessionName);

    return this._startHostingDrive({
      drive,
      transferId: transfer.transferId || transfer.id || randomId(),
      fileManifest: manifest.files || manifest,
      sessionName: reusedSessionName,
      sessionLabel: reusedSessionLabel,
      nativeInvite: transfer.invite,
      persistTransfer: false,
      existingTransfer: transfer,
    });
  }

  async getManifest({ invite }) {
    const parsedInvite = parseInvite(invite);
    const { driveKey, roomInvite } = parsedInvite;
    if (this._shouldPreferRelayDrive(parsedInvite)) {
      try {
        return await this._getManifestViaRelay(parsedInvite);
      } catch (error) {
        console.error(
          "[transfer-backend] relay manifest read failed, falling back to direct:",
          error?.message || String(error),
        );
      }
    }
    const candidates = [];

    // Prefer explicit drive keys first; room rendezvous may be stale after host restarts.
    if (driveKey) candidates.push(driveKey);

    if (roomInvite) {
      try {
        const flock = await this._attachFlock(roomInvite);
        const fromRoom = await this._waitForFlockDriveKey(flock);
        if (fromRoom) candidates.push(fromRoom);
      } catch (error) {
        console.error(
          "[transfer-backend] manifest room key resolution failed:",
          error?.message || String(error),
        );
      }
    }

    const uniqueCandidates = [...new Set(candidates.filter(Boolean))];
    if (uniqueCandidates.length === 0) {
      throw new Error("Invite does not contain a usable drive key");
    }

    let lastError = null;
    for (const key of uniqueCandidates) {
      try {
        console.log(
          `[transfer-backend] trying manifest key ${String(key).slice(0, 12)}...`,
        );
        const drive = await this._attachReadableDrive(key);
        const raw = await this._waitForEntry(drive, "/manifest.json");
        return JSON.parse(raw.toString("utf8"));
      } catch (error) {
        lastError = error;
        console.error(
          `[transfer-backend] manifest read failed for key ${String(key).slice(0, 12)}...:`,
          error?.message || String(error),
        );
      }
    }

    throw lastError || new Error("Failed to load invite manifest");
  }

  async download({ invite, targetDir = path.join(this.baseDir, "downloads") }) {
    const parsedInvite = parseInvite(invite);
    const { driveKey, roomInvite } = parsedInvite;
    if (this._shouldPreferRelayDrive(parsedInvite)) {
      try {
        return await this._downloadViaRelay({
          parsedInvite,
          invite,
          targetDir,
        });
      } catch (error) {
        console.error(
          "[transfer-backend] relay download failed, falling back to direct:",
          error?.message || String(error),
        );
      }
    }
    const resolvedDriveKey = await this._resolveDriveKey({
      driveKey,
      roomInvite,
    });
    const drive = await this._attachReadableDrive(resolvedDriveKey);
    const manifest = await this.getManifest({ invite });

    await fs.promises.mkdir(targetDir, { recursive: true });

    const saved = [];
    for (const entry of manifest.files) {
      const data = await this._waitForEntry(drive, entry.drivePath);
      const outPath = path.join(targetDir, entry.name);
      await fs.promises.writeFile(outPath, data);
      saved.push({
        name: entry.name,
        path: outPath,
        byteLength: data.byteLength,
      });
    }

    const persisted = await this.persistence.appendTransfer({
      type: "download",
      driveKey: resolvedDriveKey,
      invite,
      fileCount: saved.length,
      totalBytes: saved.reduce((sum, item) => sum + item.byteLength, 0),
    });

    return {
      transfer: persisted,
      files: saved,
    };
  }

  async readEntry({ invite, drivePath }) {
    if (!drivePath || typeof drivePath !== "string") {
      throw new Error("drivePath is required");
    }

    const parsedInvite = parseInvite(invite);
    const { driveKey, roomInvite } = parsedInvite;
    if (this._shouldPreferRelayDrive(parsedInvite)) {
      try {
        return await this._readEntryViaRelay({ parsedInvite, drivePath });
      } catch (error) {
        console.error(
          "[transfer-backend] relay file read failed, falling back to direct:",
          error?.message || String(error),
        );
      }
    }
    const resolvedDriveKey = await this._resolveDriveKey({
      driveKey,
      roomInvite,
    });
    const drive = await this._attachReadableDrive(resolvedDriveKey);
    const data = await this._waitForEntry(drive, drivePath);

    return {
      drivePath,
      dataBase64: b4a.toString(data, "base64"),
      byteLength: data.byteLength,
    };
  }

  async readEntryChunk({ invite, drivePath, offset = 0, length = 256 * 1024 }) {
    if (!drivePath || typeof drivePath !== "string") {
      throw new Error("drivePath is required");
    }

    const parsedInvite = parseInvite(invite);
    const { driveKey, roomInvite } = parsedInvite;
    if (this._shouldPreferRelayDrive(parsedInvite)) {
      try {
        return await this._readEntryChunkViaRelay({
          parsedInvite,
          drivePath,
          offset,
          length,
        });
      } catch (error) {
        console.error(
          "[transfer-backend] relay chunk read failed, falling back to direct:",
          error?.message || String(error),
        );
      }
    }
    const resolvedDriveKey = await this._resolveDriveKey({
      driveKey,
      roomInvite,
    });
    const drive = await this._attachReadableDrive(resolvedDriveKey);

    const safeOffset = Math.max(0, Number(offset || 0));
    const safeLength = Math.max(1, Math.min(1024 * 1024, Number(length || 0)));
    const bytes = await readDriveChunk(
      drive,
      drivePath,
      safeOffset,
      safeLength,
    );

    return {
      drivePath,
      offset: safeOffset,
      byteLength: bytes.byteLength,
      dataBase64: b4a.toString(bytes, "base64"),
    };
  }

  async _attachDrive(driveKeyHex) {
    if (this.liveDrives.has(driveKeyHex)) {
      return this.liveDrives.get(driveKeyHex);
    }

    const key = b4a.from(driveKeyHex, "hex");
    const drive = new Hyperdrive(this.store, key);
    await drive.ready();
    this._ensureDriveDiscovery(driveKeyHex, drive.discoveryKey, {
      client: true,
      server: false,
    });
    this.liveDrives.set(driveKeyHex, drive);
    return drive;
  }

  async _attachInviteDrive(driveKeyHex) {
    if (this.inviteDrives.has(driveKeyHex)) {
      return this.inviteDrives.get(driveKeyHex);
    }

    const key = b4a.from(driveKeyHex, "hex");
    const drive = new Hyperdrive(this.inviteStore, key);
    await drive.ready();
    this._ensureDriveDiscovery(driveKeyHex, drive.discoveryKey, {
      client: true,
      server: false,
    });
    this.inviteDrives.set(driveKeyHex, drive);
    return drive;
  }

  async _attachReadableDrive(driveKeyHex) {
    if (this.liveDrives.has(driveKeyHex)) {
      return this.liveDrives.get(driveKeyHex);
    }
    return this._attachInviteDrive(driveKeyHex);
  }

  _ensureDriveDiscovery(driveKeyHex, discoveryKey, { client, server }) {
    const existing = this.driveDiscoveries.get(driveKeyHex);
    if (!existing) {
      const handle = this.swarm.join(discoveryKey, { client, server });
      this.driveDiscoveries.set(driveKeyHex, {
        handle,
        client: Boolean(client),
        server: Boolean(server),
      });
      return;
    }

    if (server && !existing.server) {
      const handle = this.swarm.join(discoveryKey, {
        client: true,
        server: true,
      });
      this.driveDiscoveries.set(driveKeyHex, {
        handle,
        client: true,
        server: true,
      });
    }
  }

  async _resolveDriveKey({ driveKey, roomInvite }) {
    // Prefer explicit drive keys first for stable/persistent links.
    if (driveKey) return driveKey;

    if (roomInvite) {
      const flock = await this._attachFlock(roomInvite);
      const fromRoom = await this._waitForFlockDriveKey(flock);
      if (fromRoom) return fromRoom;
    }

    throw new Error("Invite does not contain a usable drive key");
  }

  async _attachFlock(roomInvite) {
    if (this.liveFlocks.has(roomInvite)) return this.liveFlocks.get(roomInvite);
    const flock = await withTimeout(
      this.flockManager.create(roomInvite),
      this.flockJoinWaitMs,
      "Timed out joining transfer room",
    );
    if (!flock) throw new Error("Failed to join transfer room from invite");
    this.liveFlocks.set(roomInvite, flock);
    return flock;
  }

  async _waitForFlockDriveKey(flock) {
    const start = Date.now();
    while (true) {
      const value = await flock.get("peardrops/drive-key");
      const key = normalizeDriveKey(value);
      if (key) return key;
      if (Date.now() - start > this.resolveWaitMs) return "";
      await sleep(this.resolveRetryMs);
    }
  }

  async _waitForEntry(drive, drivePath) {
    const start = Date.now();
    while (true) {
      const data = await drive.get(drivePath);
      if (data) return data;

      if (Date.now() - start > this.resolveWaitMs) {
        throw new Error(`Timed out waiting for ${drivePath}`);
      }
      await sleep(this.resolveRetryMs);
    }
  }

  async _startHostingDrive({
    drive,
    transferId,
    fileManifest,
    sessionName = "",
    sessionLabel = "",
    nativeInvite = "",
    persistTransfer = true,
    existingTransfer = null,
  }) {
    const driveKeyHex = drive.key.toString("hex");
    this._ensureDriveDiscovery(driveKeyHex, drive.discoveryKey, {
      client: true,
      server: true,
    });
    this.liveDrives.set(driveKeyHex, drive);

    const webHost = await this._createWebHostForDrive(drive);
    const flock = await this.flockManager.create();
    await flock.set("peardrops/drive-key", driveKeyHex);
    this.liveFlocks.set(flock.invite, flock);

    let parsedNativeInvite = null;
    if (String(nativeInvite || "").trim()) {
      try {
        parsedNativeInvite = parseInvite(nativeInvite);
      } catch {}
    }

    const relayForLinks =
      String(parsedNativeInvite?.relayUrl || "").trim() ||
      String(this.relayUrl || "").trim();
    const topicForLinks =
      String(parsedNativeInvite?.topic || "").trim() ||
      String(webHost.topicHex || "").trim();
    const roomForInvite =
      String(parsedNativeInvite?.roomInvite || "").trim() ||
      String(flock.invite || "").trim();
    const driveForInvite =
      String(parsedNativeInvite?.driveKey || "").trim() ||
      String(driveKeyHex || "").trim();
    const inviteApp = (() => {
      const requested = String(parsedNativeInvite?.app || "native")
        .trim()
        .toLowerCase();
      return requested === "web" ? "native" : requested || "native";
    })();

    // Keep web host key authoritative from the currently-running web swarm host.
    const webKeyForLinks = String(webHost.hostPublicKey || "").trim();

    const resolvedNativeInvite = createInvite({
      driveKey: driveForInvite,
      roomInvite: roomForInvite,
      topic: topicForLinks,
      webKey: webKeyForLinks,
      relayUrl: relayForLinks,
      app: inviteApp,
    });

    const webSwarmLink = createInvite({
      topic: topicForLinks,
      relayUrl: relayForLinks,
      webKey: webKeyForLinks,
      app: "web",
    }).replace("peardrops://invite", "peardrops-web://join");

    const hostSessionName = String(sessionName || "").trim() || "Host Session";
    const hostSessionLabel =
      String(sessionLabel || "").trim() ||
      formatHostSessionLabel(hostSessionName);
    const createdAt = Date.now();
    const persisted = persistTransfer
      ? await this.persistence.appendTransfer({
          type: "upload",
          transferId,
          invite: resolvedNativeInvite,
          driveKey: driveKeyHex,
          fileCount: fileManifest.length,
          totalBytes: fileManifest.reduce(
            (sum, item) => sum + Number(item.byteLength || 0),
            0,
          ),
          sessionName: hostSessionName,
          sessionLabel: hostSessionLabel,
          manifest: fileManifest,
        })
      : existingTransfer || {
          type: "upload",
          transferId,
          invite: resolvedNativeInvite,
          driveKey: driveKeyHex,
          fileCount: fileManifest.length,
          totalBytes: fileManifest.reduce(
            (sum, item) => sum + Number(item.byteLength || 0),
            0,
          ),
          sessionName: hostSessionName,
          sessionLabel: hostSessionLabel,
          manifest: fileManifest,
          createdAt,
        };

    this.liveHosts.set(resolvedNativeInvite, {
      transferId,
      invite: resolvedNativeInvite,
      driveKey: driveKeyHex,
      roomInvite: flock.invite,
      sessionName: hostSessionName,
      sessionLabel: hostSessionLabel,
      createdAt,
      fileManifest,
      flock,
      topicHex: webHost.topicHex,
      webSwarmLink,
    });

    return {
      invite: resolvedNativeInvite,
      nativeInvite: resolvedNativeInvite,
      webSwarmLink,
      transfer: persisted,
      manifest: fileManifest,
      hostSession: {
        invite: resolvedNativeInvite,
        sessionName: hostSessionName,
        sessionLabel: hostSessionLabel,
      },
    };
  }

  _isTopicInUse(topicHex) {
    return Array.from(this.liveHosts.values()).some(
      (item) => item.topicHex === topicHex,
    );
  }

  _shouldPreferRelayDrive(parsedInvite) {
    return Boolean(String(parsedInvite?.webKey || "").trim());
  }

  _resolveRelayUrl(parsedInvite) {
    return (
      String(parsedInvite?.relayUrl || "").trim() ||
      String(this.relayUrl || "").trim() ||
      "wss://pear-drops.up.railway.app"
    );
  }

  async _openRelayDriveSession(parsedInvite) {
    const webKeyHex = String(parsedInvite?.webKey || "").trim();
    if (!webKeyHex) throw new Error("Invite is missing web host key");
    const relayUrl = this._resolveRelayUrl(parsedInvite);
    const WebSocketCtor = globalThis.WebSocket;
    if (typeof WebSocketCtor !== "function") {
      throw new Error("WebSocket runtime is unavailable");
    }

    const relaySocket = new WebSocketCtor(relayUrl);
    await onceWebSocketOpen(relaySocket);

    const dht = new DHT(new RelayStream(true, relaySocket));
    const stream = dht.connect(b4a.from(webKeyHex, "hex"));
    stream.on("error", (error) => onBenignConnectionError(error));
    await onceRelayStreamOpen(stream);
    const peer = createLinePeer(stream);

    let nextId = 1;
    const pending = new Map();

    peer.onMessage((message) => {
      const id = Number(message?.id || 0);
      if (!id || !pending.has(id)) return;
      const waiter = pending.get(id);
      pending.delete(id);
      if (message?.ok === false) {
        waiter.reject(
          new Error(String(message?.error || "Peer request failed")),
        );
        return;
      }
      waiter.resolve(message || {});
    });

    return {
      async request(payload) {
        const id = nextId++;
        peer.send({ id, ...(payload || {}) });
        return await new Promise((resolve, reject) => {
          pending.set(id, { resolve, reject });
        });
      },
      async close() {
        for (const waiter of pending.values()) {
          waiter.reject(new Error("Relay drive session closed"));
        }
        pending.clear();
        try {
          stream.destroy();
        } catch {}
        try {
          await dht.destroy();
        } catch {}
        try {
          relaySocket.close();
        } catch {}
      },
    };
  }

  async _getManifestViaRelay(parsedInvite) {
    const relay = await this._openRelayDriveSession(parsedInvite);
    try {
      const response = await relay.request({ type: "manifest" });
      return response?.manifest || { files: [] };
    } finally {
      await relay.close();
    }
  }

  async _readEntryViaRelay({ parsedInvite, drivePath }) {
    const relay = await this._openRelayDriveSession(parsedInvite);
    try {
      const response = await relay.request({ type: "file", path: drivePath });
      const dataBase64 = String(response?.dataBase64 || "");
      const bytes = b4a.from(dataBase64, "base64");
      return {
        drivePath,
        dataBase64,
        byteLength: Number(response?.byteLength || bytes.byteLength || 0),
      };
    } finally {
      await relay.close();
    }
  }

  async _readEntryChunkViaRelay({
    parsedInvite,
    drivePath,
    offset = 0,
    length = 256 * 1024,
  }) {
    const safeOffset = Math.max(0, Number(offset || 0));
    const safeLength = Math.max(1, Math.min(1024 * 1024, Number(length || 0)));
    const relay = await this._openRelayDriveSession(parsedInvite);
    try {
      const response = await relay.request({
        type: "file-chunk",
        path: drivePath,
        offset: safeOffset,
        length: safeLength,
      });
      const dataBase64 = String(response?.dataBase64 || "");
      const bytes = b4a.from(dataBase64, "base64");
      return {
        drivePath,
        offset: safeOffset,
        byteLength: Number(response?.byteLength || bytes.byteLength || 0),
        dataBase64,
      };
    } finally {
      await relay.close();
    }
  }

  async _downloadViaRelay({ parsedInvite, invite, targetDir }) {
    const relay = await this._openRelayDriveSession(parsedInvite);
    try {
      const manifestResponse = await relay.request({ type: "manifest" });
      const manifest = manifestResponse?.manifest || { files: [] };
      const files = Array.isArray(manifest.files) ? manifest.files : [];

      await fs.promises.mkdir(targetDir, { recursive: true });

      const saved = [];
      for (const entry of files) {
        const drivePath = String(entry?.drivePath || "");
        if (!drivePath) continue;
        const response = await relay.request({ type: "file", path: drivePath });
        const data = b4a.from(String(response?.dataBase64 || ""), "base64");
        const outPath = path.join(
          targetDir,
          sanitizeName(String(entry?.name || "file.bin")),
        );
        await fs.promises.writeFile(outPath, data);
        saved.push({
          name: String(entry?.name || path.basename(outPath)),
          path: outPath,
          byteLength: data.byteLength,
        });
      }

      const persisted = await this.persistence.appendTransfer({
        type: "download",
        driveKey: String(parsedInvite?.driveKey || ""),
        invite,
        fileCount: saved.length,
        totalBytes: saved.reduce((sum, item) => sum + item.byteLength, 0),
      });

      return {
        transfer: persisted,
        files: saved,
      };
    } finally {
      await relay.close();
    }
  }

  _pruneZombieHosts() {
    for (const host of Array.from(this.liveHosts.values())) {
      const hasFlockCloser =
        host?.flock && typeof host.flock.close === "function";
      const flockClosed = host?.flock?.closed === true;
      if (hasFlockCloser && !flockClosed) continue;
      this.liveFlocks.delete(host?.roomInvite);
      this.liveHosts.delete(host?.invite);
    }
  }

  async close() {
    for (const flock of this.liveFlocks.values()) {
      await flock.close();
    }
    this.liveFlocks.clear();
    this.liveHosts.clear();
    for (const host of this.webHosts.values()) {
      await host.discovery.destroy();
      await host.swarm.destroy();
    }
    this.webHosts.clear();
    await this.flockManager?.close();
    for (const drive of this.liveDrives.values()) {
      await drive.close();
    }
    this.liveDrives.clear();
    for (const drive of this.inviteDrives.values()) {
      await drive.close();
    }
    this.inviteDrives.clear();
    for (const entry of this.driveDiscoveries.values()) {
      try {
        await entry?.handle?.destroy?.();
      } catch {}
    }
    this.driveDiscoveries.clear();
    await this.swarm?.destroy();
    await this.inviteStore?.close();
    await this.store?.close();
    await this.persistence.close();
    await removeDirectorySafe(this.inviteCacheDir);
  }

  async _createWebHostForDrive(drive) {
    const driveKeyHex = drive.key.toString("hex");
    const topic = deriveWebTopic(drive.discoveryKey);
    const topicHex = b4a.toString(topic, "hex");

    if (this.webHosts.has(topicHex)) return { topicHex };

    const keyPair = await this.store.createKeyPair(
      `peardrops-web-${driveKeyHex}`,
    );
    const swarm = new Hyperswarm({
      keyPair,
      ...this.swarmOptions,
    });
    swarm.on("connection", (socket) => {
      attachSocketErrorHandler(socket);
      try {
        this._handleWebTransferSocket(socket, drive);
      } catch {
        socket.destroy();
      }
    });

    const discovery = swarm.join(topic, { server: true, client: false });
    await discovery.flushed();

    const hostPublicKey = b4a.toString(keyPair.publicKey, "hex");
    this.webHosts.set(topicHex, { swarm, discovery, drive });
    return {
      topicHex,
      hostPublicKey,
    };
  }

  _handleWebTransferSocket(socket, drive) {
    let buffered = "";

    socket.on("data", async (chunk) => {
      buffered += b4a.toString(chunk, "utf8");
      let newline = buffered.indexOf("\n");
      while (newline !== -1) {
        const line = buffered.slice(0, newline).trim();
        buffered = buffered.slice(newline + 1);
        if (line) await this._handleWebRequestLine(socket, drive, line);
        newline = buffered.indexOf("\n");
      }
    });
  }

  async _handleWebRequestLine(socket, drive, line) {
    let request = null;
    try {
      request = JSON.parse(line);
    } catch {
      return;
    }

    const id = typeof request.id === "number" ? request.id : 0;
    const send = (payload) => {
      socket.write(b4a.from(`${JSON.stringify({ id, ...payload })}\n`, "utf8"));
    };

    try {
      if (request.type === "manifest") {
        const raw = await this._waitForEntry(drive, "/manifest.json");
        send({
          ok: true,
          manifest: JSON.parse(raw.toString("utf8")),
        });
        return;
      }

      if (request.type === "file") {
        const drivePath = String(request.path || "");
        if (!drivePath.startsWith("/files/")) {
          throw new Error("Invalid file path");
        }
        const data = await this._waitForEntry(drive, drivePath);
        send({
          ok: true,
          dataBase64: b4a.toString(data, "base64"),
        });
        return;
      }

      if (request.type === "file-chunk") {
        const drivePath = String(request.path || "");
        if (!drivePath.startsWith("/files/")) {
          throw new Error("Invalid file path");
        }
        const offset = Math.max(0, Number(request.offset || 0));
        const length = Math.max(
          1,
          Math.min(1024 * 1024, Number(request.length || 0)),
        );
        const bytes = await readDriveChunk(drive, drivePath, offset, length);
        send({
          ok: true,
          offset,
          byteLength: bytes.byteLength,
          dataBase64: b4a.toString(bytes, "base64"),
        });
        return;
      }

      throw new Error("Unknown request type");
    } catch (error) {
      send({
        ok: false,
        error: error.message || String(error),
      });
    }
  }

  async _writeFilesAndManifest({ drive, files }) {
    const totalBytes = files.reduce(
      (sum, file) => sum + Math.max(0, Number(file?.byteLength || 0)),
      0,
    );
    let completedBytes = 0;
    this._emitUploadProgress({
      phase: "start",
      completedBytes: 0,
      totalBytes,
      fileIndex: 0,
      fileCount: files.length,
      fileName: "",
      remainingBytes: totalBytes,
    });

    const fileManifest = [];
    for (let i = 0; i < files.length; i++) {
      const file = files[i];
      const name = sanitizeName(file.name || "file.bin");
      const drivePath = sanitizeDrivePath(file.drivePath || `/files/${name}`);
      const hasInlineData = Object.prototype.hasOwnProperty.call(
        file,
        "dataBase64",
      );
      const data = hasInlineData
        ? b4a.from(String(file.dataBase64 || ""), "base64")
        : await readUploadFile(file.path);

      await drive.put(drivePath, data);
      completedBytes += Math.max(0, Number(data.byteLength || 0));
      this._emitUploadProgress({
        phase: "file",
        completedBytes,
        totalBytes,
        fileIndex: i + 1,
        fileCount: files.length,
        fileName: name,
        remainingBytes: Math.max(0, totalBytes - completedBytes),
      });
      fileManifest.push({
        name,
        drivePath,
        byteLength: data.byteLength,
        mimeType: file.mimeType || "application/octet-stream",
      });
    }

    await drive.put(
      "/manifest.json",
      b4a.from(JSON.stringify({ files: fileManifest }, null, 2)),
    );
    this._emitUploadProgress({
      phase: "done",
      completedBytes: Math.max(completedBytes, totalBytes),
      totalBytes,
      fileIndex: files.length,
      fileCount: files.length,
      fileName: "",
      remainingBytes: 0,
    });

    return fileManifest;
  }

  _emitUploadProgress(progress) {
    if (!this.uploadProgress) return;
    try {
      this.uploadProgress(progress || {});
    } catch {}
  }
}

function normalizeDriveKey(value) {
  if (!value) return "";
  if (typeof value === "string") return value;
  if (b4a.isBuffer(value)) return b4a.toString(value, "utf8");
  return "";
}

function sanitizeName(name) {
  return String(name).replace(/[/\\]/g, "_");
}

function sanitizeDrivePath(value) {
  const raw = String(value || "").trim();
  if (!raw) return "/files/file.bin";
  const prefixed = raw.startsWith("/") ? raw : `/${raw}`;
  const noTraversal = prefixed.replace(/\.\./g, "_");
  return noTraversal
    .split("/")
    .map((part, index) => (index === 0 ? "" : sanitizeName(part)))
    .join("/");
}

async function readUploadFile(sourcePath) {
  const resolved = String(sourcePath || "").trim();
  if (!resolved) {
    throw new Error("Upload file path is required");
  }
  try {
    return await fs.promises.readFile(resolved);
  } catch (error) {
    if (error && String(error.code || "") === "ENOENT") {
      throw new Error(`Source file does not exist: ${resolved}`);
    }
    throw error;
  }
}

function randomId() {
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}

function deriveWebTopic(discoveryKey) {
  const topic = b4a.from(discoveryKey);
  topic[0] ^= 0x70;
  topic[1] ^= 0x64;
  return topic;
}

function formatHostSessionLabel(baseName) {
  const safeName = String(baseName || "Host Session").trim() || "Host Session";
  const now = new Date();
  const date = [
    now.getFullYear(),
    String(now.getMonth() + 1).padStart(2, "0"),
    String(now.getDate()).padStart(2, "0"),
  ].join("-");
  const hex = Math.floor(Math.random() * 0xffff)
    .toString(16)
    .padStart(4, "0");
  return `${safeName} ${date} ${hex}`;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function removeDirectorySafe(dirPath) {
  const target = String(dirPath || "").trim();
  if (!target) return;
  try {
    if (typeof fs.promises.rm === "function") {
      await fs.promises.rm(target, { recursive: true, force: true });
      return;
    }
  } catch {}
  try {
    if (typeof fs.promises.rmdir === "function") {
      await fs.promises.rmdir(target, { recursive: true });
    }
  } catch {}
}

function withTimeout(promise, ms, message) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(message)), ms);
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error) => {
        clearTimeout(timer);
        reject(error);
      },
    );
  });
}

module.exports = {
  TransferBackend,
};

function attachSocketErrorHandler(socket) {
  if (!socket || typeof socket.on !== "function") return;
  socket.on("error", (error) => onBenignConnectionError(error));
}

function onBenignConnectionError(error) {
  if (isBenignConnectionError(error)) return;
  console.error("Non-benign swarm connection error:", error);
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

function createLinePeer(stream) {
  let buffered = "";
  const listeners = new Set();

  stream.on("data", (chunk) => {
    buffered += b4a.toString(chunk, "utf8");
    let newline = buffered.indexOf("\n");
    while (newline !== -1) {
      const line = buffered.slice(0, newline).trim();
      buffered = buffered.slice(newline + 1);
      if (line) {
        try {
          const message = JSON.parse(line);
          for (const listener of listeners) void listener(message);
        } catch {}
      }
      newline = buffered.indexOf("\n");
    }
  });

  return {
    send(message) {
      stream.write(b4a.from(`${JSON.stringify(message || {})}\n`, "utf8"));
    },
    onMessage(listener) {
      listeners.add(listener);
    },
  };
}

function onceWebSocketOpen(socket) {
  const openedStates = new Set([1, socket?.OPEN]);
  if (openedStates.has(socket?.readyState)) return Promise.resolve();

  return new Promise((resolve, reject) => {
    let settled = false;

    const cleanup = () => {
      if (typeof socket?.removeEventListener === "function") {
        socket.removeEventListener("open", onOpen);
        socket.removeEventListener("error", onError);
      }
      if (socket) {
        if (socket.onopen === onOpen) socket.onopen = null;
        if (socket.onerror === onError) socket.onerror = null;
      }
    };

    const finish = (fn, value) => {
      if (settled) return;
      settled = true;
      cleanup();
      fn(value);
    };

    const onOpen = () => finish(resolve);
    const onError = (error) =>
      finish(reject, error || new Error("Relay websocket connection failed"));

    if (typeof socket?.addEventListener === "function") {
      socket.addEventListener("open", onOpen);
      socket.addEventListener("error", onError);
      return;
    }
    if (socket) {
      socket.onopen = onOpen;
      socket.onerror = onError;
      return;
    }
    finish(reject, new Error("Relay websocket is unavailable"));
  });
}

function onceRelayStreamOpen(stream) {
  if (stream?.opened || stream?.writable) return Promise.resolve();
  return new Promise((resolve, reject) => {
    let settled = false;

    const cleanup = () => {
      stream?.off?.("open", onOpen);
      stream?.off?.("error", onError);
      stream?.removeListener?.("open", onOpen);
      stream?.removeListener?.("error", onError);
    };

    const finish = (fn, value) => {
      if (settled) return;
      settled = true;
      cleanup();
      fn(value);
    };

    const onOpen = () => finish(resolve);
    const onError = (error) =>
      finish(reject, error || new Error("Relay stream failed"));

    stream?.on?.("open", onOpen);
    stream?.on?.("error", onError);
  });
}

async function readDriveChunk(drive, drivePath, offset, length) {
  if (!drive || typeof drive.createReadStream !== "function") {
    throw new Error("Drive does not support ranged reads");
  }

  if (length <= 0) return b4a.alloc(0);

  const start = Math.max(0, Number(offset || 0));
  const target = Math.max(1, Number(length || 0));
  const end = start + target - 1;

  return await new Promise((resolve, reject) => {
    const chunks = [];
    let total = 0;
    const stream = drive.createReadStream(drivePath, { start, end });
    stream.on("data", (chunk) => {
      const bytes = b4a.from(chunk);
      chunks.push(bytes);
      total += bytes.byteLength;
    });
    stream.once("error", reject);
    stream.once("end", () => {
      if (total === 0) return resolve(b4a.alloc(0));
      const joined = b4a.concat(chunks, total);
      if (joined.byteLength <= target) return resolve(joined);
      resolve(joined.subarray(0, target));
    });
  });
}
