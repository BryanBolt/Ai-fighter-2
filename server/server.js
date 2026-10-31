// Relais en ligne d'AI Fighter 2 : salons, parties rapides, relais des entrees (lockstep deterministe),
// journal des entrees (reprise, arrivee en cours de partie), detection de desynchronisation, AFK, expulsion.
// Le serveur ne simule RIEN : chaque client fait tourner le jeu complet.
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { WebSocketServer } from 'ws';

export const PROTOCOL_VERSION = 1;
const MAX_SLOTS = 4;
const REJOIN_MARGIN = 120; // images d'avance pour tout evenement de controle
const HOLD_MS = 20 * 60 * 1000; // une place coupee reste reservee 20 min
const ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // sans caracteres ambigus (0/O, 1/I)

export function createServer(opts = {}) {
  const afkMs = opts.afkMs ?? Number(process.env.AFK_MS || 60000);
  const holdMs = opts.holdMs ?? HOLD_MS;
  const root = opts.root ?? process.env.WEB_ROOT ?? path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'web');
  const rooms = new Map(); // code -> salon
  const banned = new Map(); // code -> Set(ip)
  const ideasRate = new Map();

  const rnd = (n) => crypto.randomInt(n);
  const newCode = () => {
    for (;;) {
      let c = '';
      for (let i = 0; i < 5; i++) c += ALPHABET[rnd(ALPHABET.length)];
      if (!rooms.has(c)) return c;
    }
  };
  const newToken = () => crypto.randomBytes(12).toString('hex');
  const send = (ws, msg) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(msg)); };

  // ---------------------------------------------------------------- HTTP
  const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.wasm': 'application/wasm', '.json': 'application/json', '.png': 'image/png', '.webmanifest': 'application/manifest+json', '.txt': 'text/plain; charset=utf-8' };
  const server = http.createServer((req, res) => {
    const url = new URL(req.url, 'http://x');
    if (url.pathname === '/health') { res.writeHead(200, { 'content-type': 'text/plain' }); res.end('ok'); return; }
    if (url.pathname === '/api/rooms') {
      res.writeHead(200, { 'content-type': 'application/json', 'access-control-allow-origin': '*' });
      res.end(JSON.stringify(publicList())); return;
    }
    if (url.pathname === '/api/ideas' && req.method === 'POST') { handleIdea(req, res); return; }
    let p = decodeURIComponent(url.pathname);
    if (p === '/') p = '/index.html';
    const file = path.normalize(path.join(root, p));
    if (!file.startsWith(path.normalize(root)) || !fs.existsSync(file) || fs.statSync(file).isDirectory()) {
      res.writeHead(404, { 'content-type': 'text/plain' }); res.end('introuvable'); return;
    }
    res.writeHead(200, { 'content-type': MIME[path.extname(file)] || 'application/octet-stream', 'cache-control': 'no-cache' });
    fs.createReadStream(file).pipe(res);
  });

  function handleIdea(req, res) {
    const ip = req.socket.remoteAddress || '?';
    const now = Date.now();
    if (now - (ideasRate.get(ip) || 0) < 10000) { res.writeHead(429); res.end('trop vite'); return; }
    ideasRate.set(ip, now);
    let body = '';
    req.on('data', (d) => { body += d; if (body.length > 4000) req.destroy(); });
    req.on('end', () => {
      let text = '';
      try { text = String(JSON.parse(body).text || '').slice(0, 1500); } catch { /* ignore */ }
      if (!text.trim()) { res.writeHead(400); res.end('vide'); return; }
      const line = JSON.stringify({ at: new Date().toISOString(), text }) + '\n';
      try { fs.appendFileSync(process.env.IDEAS_FILE || 'ideas.jsonl', line); } catch { /* disque en lecture seule */ }
      if (process.env.IDEAS_WEBHOOK) {
        fetch(process.env.IDEAS_WEBHOOK, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ content: 'Idee AI Fighter 2 : ' + text }) }).catch(() => {});
      }
      res.writeHead(200, { 'content-type': 'text/plain' }); res.end('merci');
    });
  }

  // ---------------------------------------------------------------- Salons
  const emptySlot = () => ({ ws: null, name: '', char: 0, ready: false, token: null, mode: 'free', hi: -1, journal: [], lastMove: Date.now(), lastBits: 0, heldUntil: 0, ip: '' });
  function makeRoom(isPublic) {
    const room = {
      code: newCode(), public: !!isPublic, started: false, created: Date.now(),
      options: { size: 2, stage: 0 }, // size = nombre de heros en partie (les places vides sont tenues par l'IA)
      slots: Array.from({ length: MAX_SLOTS }, emptySlot),
      seed: 0, config: null, ctl: [], hashes: new Map(),
    };
    rooms.set(room.code, room);
    return room;
  }
  const humans = (room) => room.slots.map((s, i) => [s, i]).filter(([s]) => s.ws);
  const lobbyView = (room) => ({
    t: 'room', code: room.code, public: room.public, started: room.started, options: room.options,
    players: room.slots.map((s, i) => ({ slot: i, name: s.name, char: s.char, ready: s.ready, online: !!s.ws, mode: s.mode })),
  });
  const broadcast = (room, msg, except = null) => { for (const [s] of humans(room)) if (s.ws !== except) send(s.ws, msg); };
  const publicList = () => [...rooms.values()].filter((r) => r.public).map((r) => ({ code: r.code, n: humans(r).length, max: r.options.size, started: r.started, host: r.slots.find((s) => s.ws)?.name || '' }));
  const hostSlot = (room) => room.slots.findIndex((s) => s.ws);

  function attach(room, ws, c, name) {
    const now = Date.now();
    const limit = room.started ? room.config.size : MAX_SLOTS;
    const idx = room.slots.findIndex((s, i) => i < limit && !s.ws && s.heldUntil < now && (!room.started || s.mode === 'ai'));
    if (idx < 0) return -1;
    const s = room.slots[idx];
    s.ws = ws; s.name = (name || 'Joueur').slice(0, 14); s.token = c.token; s.ip = c.ip; s.heldUntil = 0;
    s.ready = false; s.lastMove = now;
    c.room = room; c.slot = idx;
    if (!room.started) s.mode = 'human';
    return idx;
  }

  function startGame(room) {
    const taken = room.slots.filter((s) => s.ws).length;
    const size = Math.max(room.options.size, taken, 1);
    const used = new Set(room.slots.filter((s) => s.ws).map((s) => s.char));
    room.slots.forEach((s, i) => {
      if (i >= size) { s.mode = 'free'; return; }
      if (s.ws) s.mode = 'human';
      else { // place vide : heros IA, personnage different des autres
        let ch = 0; while (used.has(ch)) ch++;
        used.add(ch); s.char = ch; s.mode = 'ai'; s.name = 'IA';
      }
      s.hi = -1; s.journal = []; s.lastMove = Date.now(); s.lastBits = 0;
    });
    room.started = true; room.ctl = []; room.hashes = new Map();
    room.seed = (rnd(0x7fffffff) >>> 0) || 1;
    room.config = { seed: room.seed, size, stage: room.options.stage | 0, slots: room.slots.slice(0, size).map((s, i) => ({ slot: i, char: s.char, name: s.name, ai: s.mode === 'ai' })) };
    broadcast(room, { t: 'start', ...room.config });
  }

  const highest = (room) => Math.max(-1, ...room.slots.slice(0, room.config.size).map((s) => s.hi));
  function control(room, slot, from, extra) {
    const ev = { t: 'ctl', slot, from, ...extra };
    room.ctl.push(ev);
    broadcast(room, ev);
  }
  // Journal RLE : [valeur, repetitions, ...]
  function rle(arr) {
    const out = [];
    for (let i = 0; i < arr.length;) {
      const v = arr[i] | 0; let n = 1;
      while (i + n < arr.length && (arr[i + n] | 0) === v) n++;
      out.push(v, n); i += n;
    }
    return out;
  }
  const resyncPayload = (room) => ({
    t: 'resync', ...room.config, ctl: room.ctl,
    journal: room.slots.slice(0, room.config.size).map((s) => rle(s.journal)),
    hi: room.slots.slice(0, room.config.size).map((s) => s.hi),
  });

  // Un joueur humain disparait (coupure, depart, AFK, expulsion) : une IA prend sa place des sa derniere entree connue.
  function dropToAi(room, slot) {
    const s = room.slots[slot];
    if (s.mode !== 'human') return;
    s.mode = 'ai';
    control(room, slot, s.hi + 1, { ai: true });
  }

  function leaveRoom(c, ws, permanent) {
    const room = c.room;
    if (!room) return;
    const s = room.slots[c.slot];
    if (s && s.ws === ws) {
      s.ws = null; s.ready = false;
      if (room.started) {
        dropToAi(room, c.slot);
        s.heldUntil = permanent ? 0 : Date.now() + holdMs;
      } else { s.mode = 'free'; s.name = ''; s.token = null; }
    }
    c.room = null;
    if (humans(room).length === 0) { rooms.delete(room.code); return; }
    if (!room.started) broadcast(room, lobbyView(room));
  }

  // ---------------------------------------------------------------- WebSocket
  const wss = new WebSocketServer({ server, maxPayload: 16 * 1024 });
  const clients = new Map(); // ws -> etat
  wss.on('connection', (ws, req) => {
    const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').toString().split(',')[0].trim();
    const c = { id: crypto.randomBytes(4).toString('hex'), token: newToken(), room: null, slot: -1, ip, name: 'Joueur', msgs: 0, since: Date.now() };
    clients.set(ws, c);
    ws.on('message', (data) => {
      let m;
      try { m = JSON.parse(data.toString()); } catch { return; }
      if (!m || typeof m.t !== 'string') return;
      const now = Date.now(); // limite de debit grossiere (les entrees arrivent a 60/s)
      if (now - c.since > 1000) { c.since = now; c.msgs = 0; }
      if (++c.msgs > 240) return;
      try { handle(ws, c, m); } catch (e) { send(ws, { t: 'error', msg: 'message invalide' }); }
    });
    ws.on('close', () => { leaveRoom(c, ws, false); clients.delete(ws); });
    ws.on('error', () => {});
  });

  function joinRoom(ws, c, code) {
    const room = rooms.get(String(code || '').toUpperCase());
    if (!room) return send(ws, { t: 'error', msg: 'Salon introuvable' });
    if (banned.get(room.code)?.has(c.ip)) return send(ws, { t: 'error', msg: 'Tu as ete expulse de ce salon' });
    if (c.room) leaveRoom(c, ws, true);
    const idx = attach(room, ws, c, c.name);
    if (idx < 0) return send(ws, { t: 'error', msg: room.started ? 'Plus de place libre en cours de partie' : 'Salon complet' });
    finishJoin(room, ws, c, idx);
  }
  function finishJoin(room, ws, c, idx) {
    send(ws, { ...lobbyView(room), you: idx, token: c.token });
    if (room.started) {
      // Arrivee en cours de partie : rejeu du journal cote client, la place est reprise a une image future identique pour tous.
      const from = highest(room) + REJOIN_MARGIN;
      const s = room.slots[idx];
      s.mode = 'human'; s.lastMove = Date.now();
      send(ws, { ...resyncPayload(room), you: idx });
      control(room, idx, from, { ai: false, name: s.name });
    } else broadcast(room, lobbyView(room), ws);
  }

  function handle(ws, c, m) {
    switch (m.t) {
      case 'hello':
        if (m.v !== PROTOCOL_VERSION) return send(ws, { t: 'error', msg: 'Version incompatible : mets le jeu a jour', fatal: true });
        c.name = String(m.name || 'Joueur').slice(0, 14);
        if (m.token && /^[0-9a-f]{24}$/.test(m.token)) c.token = m.token;
        return send(ws, { t: 'welcome', v: PROTOCOL_VERSION, id: c.id, token: c.token });
      case 'list': return send(ws, { t: 'rooms', rooms: publicList() });
      case 'create': {
        if (c.room) leaveRoom(c, ws, true);
        const room = makeRoom(!!m.public);
        const idx = attach(room, ws, c, c.name);
        return send(ws, { ...lobbyView(room), you: idx, token: c.token });
      }
      case 'quick': {
        if (c.room) leaveRoom(c, ws, true);
        const open = [...rooms.values()].filter((r) => r.public && !r.started && humans(r).length < r.options.size)
          .sort((a, b) => humans(b).length - humans(a).length)[0];
        if (open) return joinRoom(ws, c, open.code);
        const room = makeRoom(true);
        const idx = attach(room, ws, c, c.name);
        return send(ws, { ...lobbyView(room), you: idx, token: c.token });
      }
      case 'join': return joinRoom(ws, c, m.code);
      case 'rejoin': {
        const room = rooms.get(String(m.code || '').toUpperCase());
        const idx = room ? room.slots.findIndex((s) => s.token && s.token === m.token && !s.ws) : -1;
        if (!room || idx < 0) return send(ws, { t: 'error', msg: 'Reprise impossible', rejoin: true });
        const s = room.slots[idx];
        s.ws = ws; s.heldUntil = 0; c.room = room; c.slot = idx; c.token = m.token;
        return finishJoin(room, ws, c, idx);
      }
      case 'leave': leaveRoom(c, ws, true); return send(ws, { t: 'left' });
      case 'ping': return send(ws, { t: 'pong', n: m.n });
      default: break;
    }
    const room = c.room;
    if (!room) return;
    const s = room.slots[c.slot];
    switch (m.t) {
      case 'lobby':
        if (room.started) return;
        s.char = Math.max(0, Math.min(99, m.char | 0));
        s.ready = !!m.ready;
        if (m.name) { s.name = String(m.name).slice(0, 14); c.name = s.name; }
        return broadcast(room, lobbyView(room));
      case 'options':
        if (room.started || c.slot !== hostSlot(room)) return;
        room.options = { size: Math.max(1, Math.min(MAX_SLOTS, (m.size | 0) || 2)), stage: Math.max(0, Math.min(11, m.stage | 0)) };
        if (typeof m.public === 'boolean') room.public = m.public;
        return broadcast(room, lobbyView(room));
      case 'start':
        if (room.started || c.slot !== hostSlot(room)) return;
        if (humans(room).some(([x]) => !x.ready && x !== s)) return send(ws, { t: 'error', msg: 'Tout le monde doit etre pret' });
        return startGame(room);
      case 'kick': {
        if (c.slot !== hostSlot(room)) return;
        const k = room.slots[m.slot | 0];
        if (!k || !k.ws || k === s) return;
        if (!banned.has(room.code)) banned.set(room.code, new Set());
        banned.get(room.code).add(k.ip);
        const kws = k.ws; const kc = clients.get(kws);
        send(kws, { t: 'kicked' });
        if (kc) leaveRoom(kc, kws, true);
        return;
      }
      case 'input': {
        if (!room.started || !Array.isArray(m.bits) || m.bits.length > 8 || !Number.isInteger(m.f0)) return;
        if (s.mode !== 'human') return;
        m.bits.forEach((b, k) => {
          const f = m.f0 + k; b = b | 0;
          if (f < 0 || f > s.hi + 600) return;
          if (f > s.hi) { s.journal[f] = b; s.hi = f; }
          if (b !== s.lastBits) { s.lastBits = b; s.lastMove = Date.now(); }
        });
        return broadcast(room, { t: 'input', slot: c.slot, f0: m.f0, bits: m.bits.map((b) => b | 0) }, ws);
      }
      case 'hash': {
        if (!room.started) return;
        const f = m.f | 0;
        const h = room.hashes.get(f) || new Map();
        h.set(c.slot, String(m.h));
        room.hashes.set(f, h);
        const need = room.slots.filter((x) => x.ws && x.mode === 'human').length;
        if (h.size >= need) {
          const ref = h.get(Math.min(...h.keys()));
          for (const [k, v] of h) {
            if (v === ref) continue;
            send(room.slots[k].ws, { t: 'desync', f });
            send(room.slots[k].ws, { ...resyncPayload(room), you: k, forced: true });
          }
          room.hashes.delete(f);
        }
        for (const k of room.hashes.keys()) if (k < f - 600) room.hashes.delete(k);
        return;
      }
      case 'resume': { // un joueur remplace par l'IA (AFK) reprend la main
        if (!room.started || s.mode !== 'ai' || s.ws !== ws) return;
        s.mode = 'human'; s.lastMove = Date.now(); s.lastBits = 0;
        return control(room, c.slot, highest(room) + REJOIN_MARGIN, { ai: false, name: s.name });
      }
      case 'chat': {
        const text = String(m.text || '').slice(0, 120);
        if (text) broadcast(room, { t: 'chat', slot: c.slot, name: s.name, text });
        return;
      }
      case 'end': // fin de partie : retour au salon
        if (!room.started) return;
        room.started = false; room.config = null; room.ctl = [];
        room.slots.forEach((x) => { x.ready = false; x.hi = -1; x.journal = []; if (x.ws) x.mode = 'human'; else { x.mode = 'free'; x.name = ''; x.token = null; } });
        return broadcast(room, lobbyView(room));
      default: break;
    }
  }

  // AFK et menage.
  const timer = setInterval(() => {
    const now = Date.now();
    for (const room of [...rooms.values()]) {
      if (room.started) {
        for (let i = 0; i < room.config.size; i++) {
          const s = room.slots[i];
          if (s.ws && s.mode === 'human' && now - s.lastMove > afkMs) { dropToAi(room, i); send(s.ws, { t: 'afk' }); }
        }
      }
      if (humans(room).length === 0 && now - room.created > holdMs) rooms.delete(room.code);
    }
  }, 1000);
  timer.unref?.();

  return { server, wss, rooms, close: () => new Promise((r) => { clearInterval(timer); for (const w of wss.clients) w.terminate(); wss.close(() => server.close(() => r())); }) };
}

// Lancement direct.
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const port = Number(process.env.PORT || 8787);
  createServer().server.listen(port, () => console.log(`AI Fighter 2 : serveur en ligne sur le port ${port}`));
}
