// Drift Alley multiplayer server.
// One Durable Object per room code. Every player opens a WebSocket to
//   wss://<your-worker>.workers.dev/room/<CODE>?id=<playerId>
// The room keeps each player's latest state and forwards updates to everyone else.
// Uses the WebSocket Hibernation API, so an idle room costs nothing.

import { DurableObject } from 'cloudflare:workers';

const MAX_PLAYERS = 12;     // per room
const MAX_MSG = 4096;       // bytes per message
const MIN_GAP_MS = 40;      // per-player rate limit (~25 updates/s)

// Only the game's own website may connect (stops other sites from using your server).
const ALLOWED_ORIGINS = [/^https:\/\/drift-alley\.vercel\.app$/, /^https:\/\/drift-alley-[a-z0-9-]+-ryanyens-projects\.vercel\.app$/];
const MAX_MSGS_PER_10S = 400;   // hard cap per player; normal play is ~150-250

const SECURITY_HEADERS = {
  'Content-Type': 'text/plain; charset=utf-8',
  'X-Content-Type-Options': 'nosniff',
  'X-Frame-Options': 'DENY',
  'Referrer-Policy': 'no-referrer',
  'Strict-Transport-Security': 'max-age=63072000; includeSubDomains',
  'Content-Security-Policy': "default-src 'none'; frame-ancestors 'none'",
  'Permissions-Policy': 'camera=(), microphone=(), geolocation=()',
};
const reply = (text, status = 200) => new Response(text, { status, headers: SECURITY_HEADERS });
const okOrigin = o => !!o && ALLOWED_ORIGINS.some(r => r.test(o));

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === '/' || url.pathname === '/health') {
      return reply('Drift Alley server is running');
    }
    const m = url.pathname.match(/^\/room\/([A-Z0-9]{3,8})$/);
    if (!m) return reply('Not found', 404);
    if (request.headers.get('Upgrade') !== 'websocket') return reply('Expected a WebSocket', 426);
    if (!okOrigin(request.headers.get('Origin'))) return reply('Forbidden', 403);
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
    // The server picks each player's id, so nobody can pretend to be (or kick) someone else.
    const id = 'p' + crypto.randomUUID().replace(/-/g, '').slice(0, 15);
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
    server.serializeAttachment({ id, p: null, t: 0, w: Date.now(), n: 0 });

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
    if (now - a.w > 10000) { a.w = now; a.n = 0; }
    if (++a.n > MAX_MSGS_PER_10S) { try { ws.close(4008, 'too many messages'); } catch (e) {} return; }
    if (now - a.t < MIN_GAP_MS) { try { ws.serializeAttachment(a); } catch (e) {} return; }
    let msg;
    try { msg = JSON.parse(message); } catch (e) { return; }
    if (!msg || msg.t !== 'p' || !msg.d || typeof msg.d !== 'object' || Array.isArray(msg.d)) return;
    if (Object.keys(msg.d).length > 60) return;
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
    this.broadcast(JSON.stringify({ t: 'x', id: a.id }), ws);
  }

  broadcast(text, except) {
    for (const ws of this.ctx.getWebSockets()) {
      if (ws === except) continue;
      try { ws.send(text); } catch (e) {}
    }
  }
}
