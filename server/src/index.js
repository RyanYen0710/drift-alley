// Drift Alley multiplayer server.
// One Durable Object per room code. Every player opens a WebSocket to
//   wss://<your-worker>.workers.dev/room/<CODE>?id=<playerId>
// The room keeps each player's latest state and forwards updates to everyone else.
// Uses the WebSocket Hibernation API, so an idle room costs nothing.

import { DurableObject } from 'cloudflare:workers';

const MAX_PLAYERS = 12;     // per room
const MAX_MSG = 4096;       // bytes per message
const MIN_GAP_MS = 40;      // per-player rate limit (~25 updates/s)

const CORS = { 'Access-Control-Allow-Origin': '*' };

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === '/' || url.pathname === '/health') {
      return new Response('Drift Alley server is running', { headers: CORS });
    }
    const m = url.pathname.match(/^\/room\/([A-Z0-9]{3,8})$/);
    if (!m) return new Response('Not found', { status: 404, headers: CORS });
    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('Expected a WebSocket', { status: 426, headers: CORS });
    }
    const stub = env.ROOMS.get(env.ROOMS.idFromName(m[1]));
    return stub.fetch(request);
  },
};

export class Room extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    // Answer client pings without waking the object up.
    ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair('ping', 'pong'));
  }

  async fetch(request) {
    const url = new URL(request.url);
    const id = (url.searchParams.get('id') || '').slice(0, 24);
    if (!/^[a-z0-9]{4,24}$/i.test(id)) return new Response('Bad id', { status: 400 });

    const sockets = this.ctx.getWebSockets();
    // Same player reconnecting: drop the old socket.
    for (const ws of sockets) {
      const a = ws.deserializeAttachment();
      if (a && a.id === id) { try { ws.close(1000, 'replaced'); } catch (e) {} }
    }
    const full = this.ctx.getWebSockets().filter(w => w.readyState === 1).length >= MAX_PLAYERS;
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    if (full) {
      // Accept just long enough to tell the player why.
      server.accept();
      server.send(JSON.stringify({ t: 'full', max: MAX_PLAYERS }));
      server.close(4003, 'room full');
      return new Response(null, { status: 101, webSocket: client });
    }
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({ id, p: null, t: 0 });

    // Send the newcomer everyone already in the room.
    const peers = {};
    for (const ws of this.ctx.getWebSockets()) {
      if (ws === server) continue;
      const a = ws.deserializeAttachment();
      if (a && a.p && a.id !== id) peers[a.id] = a.p;
    }
    server.send(JSON.stringify({ t: 'snap', me: id, peers }));
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, message) {
    if (typeof message !== 'string' || message.length > MAX_MSG) return;
    const a = ws.deserializeAttachment();
    if (!a) return;
    const now = Date.now();
    if (now - a.t < MIN_GAP_MS) return;
    let msg;
    try { msg = JSON.parse(message); } catch (e) { return; }
    if (!msg || msg.t !== 'p' || !msg.d || typeof msg.d !== 'object' || Array.isArray(msg.d)) return;
    a.t = now;
    a.p = message.length < 1800 ? msg.d : null; // attachments are capped at 2 KB; big states still get forwarded
    try { ws.serializeAttachment(a); } catch (e) { a.p = null; try { ws.serializeAttachment(a); } catch (e2) {} }
    this.broadcast(JSON.stringify({ t: 'p', id: a.id, d: msg.d }), ws);
  }

  async webSocketClose(ws, code) { this.leave(ws); try { ws.close(code, 'bye'); } catch (e) {} }
  async webSocketError(ws) { this.leave(ws); }

  leave(ws) {
    const a = ws.deserializeAttachment();
    if (!a) return;
    // Only announce if this player has no other live socket (reconnects replace sockets).
    const still = this.ctx.getWebSockets().some(w => w !== ws && w.readyState === 1 && (w.deserializeAttachment() || {}).id === a.id);
    if (!still) this.broadcast(JSON.stringify({ t: 'x', id: a.id }), ws);
  }

  broadcast(text, except) {
    for (const ws of this.ctx.getWebSockets()) {
      if (ws === except) continue;
      try { ws.send(text); } catch (e) {}
    }
  }
}
