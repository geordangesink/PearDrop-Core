const INVITE_PROTOCOL = "peardrops:";

function createInvite({
  driveKey = "",
  roomInvite = "",
  topic = "",
  relayUrl = "",
  webKey = "",
  app = "native",
}) {
  if (!driveKey && !roomInvite && !topic && !webKey) {
    throw new Error("driveKey, roomInvite, topic, or webKey is required");
  }
  const url = new URL("peardrops://invite");
  if (driveKey) url.searchParams.set("drive", String(driveKey));
  if (roomInvite) url.searchParams.set("room", String(roomInvite));
  if (topic) url.searchParams.set("topic", String(topic));
  if (relayUrl) url.searchParams.set("relay", relayUrl);
  if (webKey) url.searchParams.set("web", String(webKey));
  url.searchParams.set("app", app);
  return url.toString();
}

function parseInvite(value) {
  const url = new URL(String(value || "").trim());
  if (url.protocol !== INVITE_PROTOCOL) {
    throw new Error("Invite URL must start with peardrops://");
  }

  const driveKey = url.searchParams.get("drive") || "";
  const roomInvite = url.searchParams.get("room") || "";
  if (!driveKey && !roomInvite) {
    throw new Error("Invite is missing drive key and room invite");
  }

  return {
    driveKey,
    roomInvite,
    topic: url.searchParams.get("topic") || "",
    relayUrl: url.searchParams.get("relay") || "",
    webKey: url.searchParams.get("web") || "",
    app: url.searchParams.get("app") || "native",
  };
}

module.exports = {
  INVITE_PROTOCOL,
  createInvite,
  parseInvite,
};
