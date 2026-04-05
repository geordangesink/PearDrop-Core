const { fs, path } = require('../utils/runtime-compat')
const b4a = require('b4a')
const Corestore = require('corestore')
const Hyperdrive = require('hyperdrive')
const Hyperswarm = require('hyperswarm')
const FlockManager = require('flockmanager')
const { HyperdbPersistence } = require('../persistence/hyperdb-persistence')
const { createInvite, parseInvite } = require('../utils/invite')

class TransferBackend {
  constructor({
    baseDir,
    relayUrl = '',
    swarmOptions = {},
    resolveWaitMs = 10000,
    resolveRetryMs = 150,
    flockJoinWaitMs = 8000
  }) {
    this.baseDir = baseDir
    this.relayUrl = relayUrl
    this.swarmOptions = swarmOptions
    this.resolveWaitMs = resolveWaitMs
    this.resolveRetryMs = resolveRetryMs
    this.flockJoinWaitMs = flockJoinWaitMs
    this.store = null
    this.swarm = null
    this.persistence = new HyperdbPersistence(path.join(baseDir, 'db'))
    this.liveDrives = new Map()
    this.liveFlocks = new Map()
    this.webHosts = new Map()
    this.flockManager = null
  }

  async ready() {
    await fs.promises.mkdir(this.baseDir, { recursive: true })
    this.store = new Corestore(path.join(this.baseDir, 'corestore'))
    await this.store.ready()
    const keyPair = await this.store.createKeyPair('peardrops-swarm')
    this.swarm = new Hyperswarm({ keyPair, ...this.swarmOptions })
    this.swarm.on('connection', (socket) => {
      attachSocketErrorHandler(socket)
      const replication = this.store.replicate(socket)
      if (replication && typeof replication.on === 'function') {
        replication.on('error', (error) => onBenignConnectionError(error))
      }
    })
    this.flockManager = new FlockManager(null, {
      swarm: this.swarm,
      store: this.store
    })
    await this.flockManager.ready()
    await this.persistence.ready()
  }

  async listTransfers() {
    return this.persistence.listTransfers()
  }

  async createUpload({ files }) {
    if (!Array.isArray(files) || files.length === 0) {
      throw new Error('At least one file is required')
    }

    const transferId = randomId()
    const drive = new Hyperdrive(this.store.namespace(`transfer-${transferId}`))
    await drive.ready()

    const fileManifest = []
    for (const file of files) {
      const name = sanitizeName(file.name || 'file.bin')
      const drivePath = `/files/${name}`
      const data = file.dataBase64
        ? b4a.from(file.dataBase64, 'base64')
        : await fs.promises.readFile(file.path)

      await drive.put(drivePath, data)
      fileManifest.push({
        name,
        drivePath,
        byteLength: data.byteLength,
        mimeType: file.mimeType || 'application/octet-stream'
      })
    }

    await drive.put('/manifest.json', b4a.from(JSON.stringify({ files: fileManifest }, null, 2)))

    this.swarm.join(drive.discoveryKey, { server: true, client: true })
    this.liveDrives.set(drive.key.toString('hex'), drive)
    const webHost = await this._createWebHostForDrive(drive)

    const flock = await this.flockManager.create()
    await flock.set('peardrops/drive-key', drive.key.toString('hex'))
    this.liveFlocks.set(flock.invite, flock)

    const nativeInvite = createInvite({
      driveKey: drive.key.toString('hex'),
      roomInvite: flock.invite,
      topic: webHost.topicHex,
      relayUrl: this.relayUrl,
      app: 'native'
    })
    const webSwarmLink = createInvite({
      topic: webHost.topicHex,
      relayUrl: this.relayUrl,
      webKey: webHost.hostPublicKey,
      app: 'web'
    }).replace('peardrops://invite', 'peardrops-web://join')

    const persisted = await this.persistence.appendTransfer({
      type: 'upload',
      transferId,
      invite: nativeInvite,
      driveKey: drive.key.toString('hex'),
      fileCount: fileManifest.length,
      totalBytes: fileManifest.reduce((sum, item) => sum + item.byteLength, 0)
    })

    return {
      invite: nativeInvite,
      nativeInvite,
      webSwarmLink,
      transfer: persisted,
      manifest: fileManifest
    }
  }

  async getManifest({ invite }) {
    const { driveKey, roomInvite } = parseInvite(invite)
    const resolvedDriveKey = await this._resolveDriveKey({ driveKey, roomInvite })
    const drive = await this._attachDrive(resolvedDriveKey)
    const raw = await this._waitForEntry(drive, '/manifest.json')
    return JSON.parse(raw.toString('utf8'))
  }

  async download({ invite, targetDir = path.join(this.baseDir, 'downloads') }) {
    const { driveKey, roomInvite } = parseInvite(invite)
    const resolvedDriveKey = await this._resolveDriveKey({ driveKey, roomInvite })
    const drive = await this._attachDrive(resolvedDriveKey)
    const manifest = await this.getManifest({ invite })

    await fs.promises.mkdir(targetDir, { recursive: true })

    const saved = []
    for (const entry of manifest.files) {
      const data = await this._waitForEntry(drive, entry.drivePath)
      const outPath = path.join(targetDir, entry.name)
      await fs.promises.writeFile(outPath, data)
      saved.push({ name: entry.name, path: outPath, byteLength: data.byteLength })
    }

    const persisted = await this.persistence.appendTransfer({
      type: 'download',
      driveKey: resolvedDriveKey,
      invite,
      fileCount: saved.length,
      totalBytes: saved.reduce((sum, item) => sum + item.byteLength, 0)
    })

    return {
      transfer: persisted,
      files: saved
    }
  }

  async readEntry({ invite, drivePath }) {
    if (!drivePath || typeof drivePath !== 'string') {
      throw new Error('drivePath is required')
    }

    const { driveKey, roomInvite } = parseInvite(invite)
    const resolvedDriveKey = await this._resolveDriveKey({ driveKey, roomInvite })
    const drive = await this._attachDrive(resolvedDriveKey)
    const data = await this._waitForEntry(drive, drivePath)

    return {
      drivePath,
      dataBase64: b4a.toString(data, 'base64'),
      byteLength: data.byteLength
    }
  }

  async _attachDrive(driveKeyHex) {
    if (this.liveDrives.has(driveKeyHex)) return this.liveDrives.get(driveKeyHex)

    const key = b4a.from(driveKeyHex, 'hex')
    const drive = new Hyperdrive(this.store, key)
    await drive.ready()
    this.swarm.join(drive.discoveryKey, { client: true, server: false })
    this.liveDrives.set(driveKeyHex, drive)
    return drive
  }

  async _resolveDriveKey({ driveKey, roomInvite }) {
    if (driveKey) return driveKey

    if (roomInvite) {
      const flock = await this._attachFlock(roomInvite)
      const fromRoom = await this._waitForFlockDriveKey(flock)
      if (fromRoom) return fromRoom
    }

    throw new Error('Invite does not contain a usable drive key')
  }

  async _attachFlock(roomInvite) {
    if (this.liveFlocks.has(roomInvite)) return this.liveFlocks.get(roomInvite)
    const flock = await withTimeout(
      this.flockManager.create(roomInvite),
      this.flockJoinWaitMs,
      'Timed out joining transfer room'
    )
    if (!flock) throw new Error('Failed to join transfer room from invite')
    this.liveFlocks.set(roomInvite, flock)
    return flock
  }

  async _waitForFlockDriveKey(flock) {
    const start = Date.now()
    while (true) {
      const value = await flock.get('peardrops/drive-key')
      const key = normalizeDriveKey(value)
      if (key) return key
      if (Date.now() - start > this.resolveWaitMs) return ''
      await sleep(this.resolveRetryMs)
    }
  }

  async _waitForEntry(drive, drivePath) {
    const start = Date.now()
    while (true) {
      const data = await drive.get(drivePath)
      if (data) return data

      if (Date.now() - start > this.resolveWaitMs) {
        throw new Error(`Timed out waiting for ${drivePath}`)
      }
      await sleep(this.resolveRetryMs)
    }
  }

  async close() {
    for (const flock of this.liveFlocks.values()) {
      await flock.close()
    }
    this.liveFlocks.clear()
    for (const host of this.webHosts.values()) {
      await host.discovery.destroy()
      await host.swarm.destroy()
    }
    this.webHosts.clear()
    await this.flockManager?.close()
    for (const drive of this.liveDrives.values()) {
      await drive.close()
    }
    this.liveDrives.clear()
    await this.swarm?.destroy()
    await this.store?.close()
    await this.persistence.close()
  }

  async _createWebHostForDrive(drive) {
    const driveKeyHex = drive.key.toString('hex')
    const topic = deriveWebTopic(drive.discoveryKey)
    const topicHex = b4a.toString(topic, 'hex')

    if (this.webHosts.has(topicHex)) return { topicHex }

    const keyPair = await this.store.createKeyPair(`peardrops-web-${driveKeyHex}`)
    const swarm = new Hyperswarm({
      keyPair,
      dht: this.swarm.dht
    })
    swarm.on('connection', (socket) => {
      attachSocketErrorHandler(socket)
      this._handleWebTransferSocket(socket, drive).catch(() => {
        socket.destroy()
      })
    })

    const discovery = swarm.join(topic, { server: true, client: false })
    await discovery.flushed()

    this.webHosts.set(topicHex, { swarm, discovery, drive })
    return {
      topicHex,
      hostPublicKey: b4a.toString(keyPair.publicKey, 'hex')
    }
  }

  async _handleWebTransferSocket(socket, drive) {
    let buffered = ''

    socket.on('data', async (chunk) => {
      buffered += b4a.toString(chunk, 'utf8')
      let newline = buffered.indexOf('\n')
      while (newline !== -1) {
        const line = buffered.slice(0, newline).trim()
        buffered = buffered.slice(newline + 1)
        if (line) await this._handleWebRequestLine(socket, drive, line)
        newline = buffered.indexOf('\n')
      }
    })
  }

  async _handleWebRequestLine(socket, drive, line) {
    let request = null
    try {
      request = JSON.parse(line)
    } catch {
      return
    }

    const id = typeof request.id === 'number' ? request.id : 0
    const send = (payload) => {
      socket.write(b4a.from(`${JSON.stringify({ id, ...payload })}\n`, 'utf8'))
    }

    try {
      if (request.type === 'manifest') {
        const raw = await this._waitForEntry(drive, '/manifest.json')
        send({
          ok: true,
          manifest: JSON.parse(raw.toString('utf8'))
        })
        return
      }

      if (request.type === 'file') {
        const drivePath = String(request.path || '')
        if (!drivePath.startsWith('/files/')) throw new Error('Invalid file path')
        const data = await this._waitForEntry(drive, drivePath)
        send({
          ok: true,
          dataBase64: b4a.toString(data, 'base64')
        })
        return
      }

      throw new Error('Unknown request type')
    } catch (error) {
      send({
        ok: false,
        error: error.message || String(error)
      })
    }
  }
}

function normalizeDriveKey(value) {
  if (!value) return ''
  if (typeof value === 'string') return value
  if (b4a.isBuffer(value)) return b4a.toString(value, 'utf8')
  return ''
}

function sanitizeName(name) {
  return String(name).replace(/[/\\]/g, '_')
}

function randomId() {
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`
}

function deriveWebTopic(discoveryKey) {
  const topic = b4a.from(discoveryKey)
  topic[0] ^= 0x70
  topic[1] ^= 0x64
  return topic
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function withTimeout(promise, ms, message) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(message)), ms)
    promise.then(
      (value) => {
        clearTimeout(timer)
        resolve(value)
      },
      (error) => {
        clearTimeout(timer)
        reject(error)
      }
    )
  })
}

module.exports = {
  TransferBackend
}

function attachSocketErrorHandler(socket) {
  if (!socket || typeof socket.on !== 'function') return
  socket.on('error', (error) => onBenignConnectionError(error))
}

function onBenignConnectionError(error) {
  if (isBenignConnectionError(error)) return
  console.error('Non-benign swarm connection error:', error)
}

function isBenignConnectionError(error) {
  const code = String(error?.code || '')
  const message = String(error?.message || '')
  return (
    code === 'ECONNRESET' ||
    code === 'EPIPE' ||
    code === 'ETIMEDOUT' ||
    message.includes('connection reset by peer') ||
    message.includes('stream was destroyed') ||
    message.includes('socket closed')
  )
}
