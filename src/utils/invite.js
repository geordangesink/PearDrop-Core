const INVITE_PROTOCOL = 'peardrops:'

function createInvite({ driveKey = '', roomInvite = '', relayUrl = '', app = 'native' }) {
  if (!driveKey && !roomInvite) throw new Error('driveKey or roomInvite is required')
  const url = new URL('peardrops://invite')
  if (driveKey) url.searchParams.set('drive', String(driveKey))
  if (roomInvite) url.searchParams.set('room', String(roomInvite))
  if (relayUrl) url.searchParams.set('relay', relayUrl)
  url.searchParams.set('app', app)
  return url.toString()
}

function parseInvite(value) {
  const url = new URL(String(value || '').trim())
  if (url.protocol !== INVITE_PROTOCOL) {
    throw new Error('Invite URL must start with peardrops://')
  }

  const driveKey = url.searchParams.get('drive') || ''
  const roomInvite = url.searchParams.get('room') || ''
  if (!driveKey && !roomInvite) {
    throw new Error('Invite is missing drive key and room invite')
  }

  return {
    driveKey,
    roomInvite,
    relayUrl: url.searchParams.get('relay') || '',
    app: url.searchParams.get('app') || 'native'
  }
}

module.exports = {
  INVITE_PROTOCOL,
  createInvite,
  parseInvite
}
