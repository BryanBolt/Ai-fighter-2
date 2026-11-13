// Telemetrie de jeu anonyme : POST /telemetry (sessions, captures, notes « bien / pas bien »), GET /telemetry?format=csv|json et GET /telemetry/captures
// (lecture protegee par la variable d'environnement ADMIN_KEY ; la cle n'est JAMAIS ecrite dans le depot ni dans les journaux).
// Aucune donnee personnelle : pas de pseudo, pas d'adresse (l'IP ne sert qu'a limiter le debit, en memoire), un identifiant de session aleatoire.
// Stockage borne : MAX_SESSIONS sessions (les plus anciennes partent), MAX_CAPS captures JPEG (<= MAX_CAP_BYTES chacune, MAX_CAPS_TOTAL au total),
// copie sur disque en JSON-lines (TELEMETRY_FILE, ecrite a la fin de session ou a la note, rechargee au demarrage).
import fs from 'node:fs';
import crypto from 'node:crypto';

const MAX_BODY = 160 * 1024;
const MAX_SESSIONS = 4000;
const MAX_CAPS = 150;
const MAX_CAP_BYTES = 30 * 1024; // image decodee
const MAX_CAPS_TOTAL = 5 * 1024 * 1024;
const MAX_CAPS_PER_SID = 12;
const RATE_PER_MIN = 90;

const clean = (v, n) => String(v ?? '').replace(/[\u0000-\u001f\u007f-\u009f\u200b-\u200f\u2028-\u202e]/g, '').slice(0, n);
const cleanKey = (v) => clean(v, 40).replace(/[^\w.:@+\-/ ]/g, '_');
const num = (v, hi = 1e9) => (Number.isFinite(v) ? Math.max(-1, Math.min(hi, Math.round(v * 100) / 100)) : null);
const PERSONAL = /^(name|nom|pseudo|ip|email|user|addr|token|pid)/i;

// Compteurs { cle: nombre } : au plus 64 cles, nombres bornes (boutons, ecrans, coups, pouvoirs...).
function counters(o, max = 64) {
  const out = {};
  if (!o || typeof o !== 'object' || Array.isArray(o)) return out;
  for (const [k, v] of Object.entries(o).slice(0, max)) { if (PERSONAL.test(k)) continue; const n = num(Number(v)); if (n !== null) out[cleanKey(k)] = n; }
  return out;
}
function list(a, n, f) { return Array.isArray(a) ? a.slice(0, n).map(f).filter((x) => x !== null && x !== undefined) : []; }

// Normalise une session (instantane CUMULATIF : le dernier envoi d'un meme `sid` remplace les precedents).
export function cleanSession(m) {
  if (!m || typeof m !== 'object' || !/^[0-9a-f]{6,16}$/.test(String(m.sid || ''))) return null;
  const net = m.net && typeof m.net === 'object' ? m.net : {};
  const nm = ['sync', 'host', 'auto'].includes(net.nm) ? net.nm : 'none';
  const nme = net.nme === 'host' ? 'host' : net.nme === 'sync' ? 'sync' : nm === 'none' ? 'none' : 'sync';
  const per = (a) => list(a, 8, (x) => num(Number(x), 60000));
  const fr = m.freezes && typeof m.freezes === 'object' ? m.freezes : {};
  return {
    sid: String(m.sid), at: Date.now(), v: num(m.v, 99) ?? 1, final: !!m.final,
    build: clean(m.build, 40), platform: cleanKey(m.platform), device: cleanKey(m.device), browser: cleanKey(m.browser), touch: !!m.touch, screen_w: num(m.screen_w, 20000), screen_h: num(m.screen_h, 20000),
    mode: cleanKey(m.mode), place: cleanKey(m.place), floor: num(m.floor, 9999), hero: cleanKey(m.hero),
    dur: num(m.dur, 1e7), combat_s: num(m.combat_s, 1e7), menu_s: num(m.menu_s, 1e7), load_ms: num(m.load_ms, 1e7), fps_med: num(m.fps_med, 1000), fps_p5: num(m.fps_p5, 1000),
    net: {
      nm, nme, players: num(net.players, 8) ?? 0, online_s: num(net.online_s, 1e7),
      rtt_med: num(net.rtt_med, 60000), rtt_p95: num(net.rtt_p95, 60000), rtt_worst_med: num(net.rtt_worst_med, 60000), rtt_worst_p95: num(net.rtt_worst_p95, 60000),
      peers_rtt_med: per(net.peers_rtt_med), peers_rtt_p95: per(net.peers_rtt_p95),
      resumes: num(net.resumes, 9999) ?? 0, resync_fixed: num(net.resync_fixed, 9999) ?? 0, resync_unfixed: num(net.resync_unfixed, 9999) ?? 0, reloads: num(net.reloads, 9999) ?? 0,
      soft_fills: num(net.soft_fills, 99999) ?? 0, ai_takeovers: num(net.ai_takeovers, 9999) ?? 0,
    },
    freezes: { n: num(fr.n, 99999) ?? 0, total_ms: num(fr.total_ms, 1e8) ?? 0, max_ms: num(fr.max_ms, 1e8) ?? 0, over1s: num(fr.over1s, 99999) ?? 0, list: list(fr.list, 40, (x) => num(Number(x), 1e7)) },
    errors: num(m.errors, 99999) ?? 0, error_msgs: list(m.error_msgs, 5, (x) => clean(x, 120)),
    outcome: ['win', 'lose', 'abandon', 'none'].includes(m.outcome) ? m.outcome : 'none', abandon_s: num(m.abandon_s, 1e7),
    rating: m.rating === 'good' || m.rating === 'bad' ? m.rating : null,
    screens: counters(m.screens), path: list(m.path, 16, (x) => cleanKey(x)), buttons: counters(m.buttons), moves: counters(m.moves),
    powers_offered: counters(m.powers_offered), powers_taken: counters(m.powers_taken), items_picked: counters(m.items_picked), items_used: counters(m.items_used),
    deaths: list(m.deaths, 20, (d) => (d && typeof d === 'object' ? { place: cleanKey(d.place), floor: num(d.floor, 9999), cause: cleanKey(d.cause), t: num(d.t, 1e7) } : null)),
    text: (() => { const t = m.text && typeof m.text === 'object' ? m.text : {}; return { side: num(t.side, 16384), dpi: num(t.dpi, 16), glyphs: num(t.glyphs, 1e6), core: num(t.core, 1e6), used_h: num(t.used_h, 16384), lazy: num(t.lazy, 1e7), purges: num(t.purges, 1e6), refused: num(t.refused, 1e6), rebuilds: num(t.rebuilds, 1e6), sizes: num(t.sizes, 1e4) }; })(),
    floors: list(m.floors, 30, (d) => (d && typeof d === 'object' ? { place: cleanKey(d.place), floor: num(d.floor, 9999), s: num(d.s, 1e7), died: !!d.died } : null)),
  };
}

export function createTelemetry(opts = {}) {
  const file = opts.file ?? process.env.TELEMETRY_FILE ?? 'telemetry.jsonl';
  const adminKey = () => opts.adminKey ?? process.env.ADMIN_KEY ?? '';
  const sessions = new Map();
  const caps = new Map(); let capsBytes = 0;
  const rate = new Map();
  const stats = { posts: 0, rejected: 0 };

  function load() {
    if (opts.noDisk || !file) return;
    try {
      const lines = fs.readFileSync(file, 'utf8').split('\n').filter(Boolean).slice(-MAX_SESSIONS);
      for (const l of lines) { try { const s = JSON.parse(l); if (s && s.sid) sessions.set(s.sid, s); } catch { /* ligne abimee */ } }
    } catch { /* pas de fichier */ }
  }
  function persist(s) {
    if (opts.noDisk || !file) return;
    try { fs.appendFileSync(file, JSON.stringify(s) + '\n'); } catch { /* disque en lecture seule */ }
  }
  function put(s) {
    const old = sessions.get(s.sid);
    if (old && old.rating && !s.rating) s.rating = old.rating; // la note posee a part n'est pas effacee par un instantane plus recent
    sessions.delete(s.sid); sessions.set(s.sid, s);
    while (sessions.size > MAX_SESSIONS) sessions.delete(sessions.keys().next().value);
    if (s.final || s.rating) persist(s);
  }
  function addCap(m) {
    if (!/^[0-9a-f]{6,16}$/.test(String(m.sid || ''))) return false;
    const mt = /^data:image\/jpeg;base64,([A-Za-z0-9+/=]+)$/.exec(String(m.img || ''));
    if (!mt) return false;
    const buf = Buffer.from(mt[1], 'base64');
    if (buf.length < 100 || buf.length > MAX_CAP_BYTES || buf[0] !== 0xff || buf[1] !== 0xd8) return false;
    const mine = [...caps.values()].filter((c) => c.sid === m.sid).length;
    if (mine >= MAX_CAPS_PER_SID) return false;
    const id = crypto.createHash('sha1').update(m.sid + ':' + Date.now() + ':' + buf.length).digest('hex').slice(0, 10);
    const kind = ['death', 'abandon', 'freeze', 'error', 'report', 'bad'].includes(m.kind) ? m.kind : 'other';
    caps.set(id, { id, sid: String(m.sid), kind, at: Date.now(), ctx: counters(m.ctx, 12), tctx: clean(m.where, 80), buf });
    capsBytes += buf.length;
    while (caps.size > MAX_CAPS || capsBytes > MAX_CAPS_TOTAL) { const k = caps.keys().next().value; capsBytes -= caps.get(k).buf.length; caps.delete(k); }
    return true;
  }
  function rateOk(ip) {
    const now = Date.now(); const l = (rate.get(ip) || []).filter((t) => now - t < 60000);
    if (l.length >= RATE_PER_MIN) { rate.set(ip, l); return false; }
    l.push(now); rate.set(ip, l); if (rate.size > 5000) rate.clear();
    return true;
  }
  // Un message : { k: 'sess' | 'cap' | 'rate', ... }. Renvoie 'ok' ou une raison de refus.
  function ingest(m) {
    stats.posts++;
    if (!m || typeof m !== 'object') return 'bad';
    if (m.k === 'cap') return addCap(m) ? 'ok' : 'bad';
    if (m.k === 'rate') {
      const s = sessions.get(String(m.sid || ''));
      if (!s || (m.r !== 'good' && m.r !== 'bad')) return 'bad';
      s.rating = m.r; s.rating_ctx = { mode: s.mode, place: s.place, floor: s.floor, nm: s.net.nme, device: s.device, platform: s.platform };
      persist(s); return 'ok';
    }
    const s = cleanSession(m);
    if (!s) return 'bad';
    put(s); return 'ok';
  }
  function handlePost(req, res, ip = '') {
    if (!rateOk(ip)) { res.writeHead(429); res.end('trop vite'); return; }
    let size = 0; const chunks = [];
    req.on('data', (c) => { size += c.length; if (size > MAX_BODY) { res.writeHead(413); res.end('trop gros'); req.destroy(); } else chunks.push(c); });
    req.on('end', () => {
      if (res.writableEnded) return;
      let m = null; try { m = JSON.parse(Buffer.concat(chunks).toString('utf8')); } catch { /* json invalide */ }
      const batch = Array.isArray(m) ? m.slice(0, 8) : [m];
      const r = batch.map(ingest);
      if (r.every((x) => x !== 'ok')) stats.rejected++;
      res.writeHead(r.some((x) => x === "ok") ? 204 : 400, { "access-control-allow-origin": "*" }); res.end();
    });
  }
  function authed(req, url) {
    const k = adminKey();
    if (!k) return false;
    const got = String(req.headers['x-admin-key'] || (/^Bearer (.+)$/.exec(req.headers.authorization || '') || [])[1] || url.searchParams.get('key') || '');
    const a = crypto.createHash('sha256').update(got).digest(); const b = crypto.createHash('sha256').update(k).digest();
    return crypto.timingSafeEqual(a, b);
  }
  const CSV_COLS = ['sid', 'at', 'build', 'platform', 'device', 'browser', 'mode', 'place', 'floor', 'hero', 'dur', 'combat_s', 'menu_s', 'fps_med', 'outcome', 'abandon_s', 'rating', 'errors',
    'net.nm', 'net.nme', 'net.players', 'net.rtt_med', 'net.rtt_p95', 'net.rtt_worst_med', 'net.rtt_worst_p95', 'net.resumes', 'net.resync_fixed', 'net.resync_unfixed', 'net.reloads', 'net.soft_fills',
    'freezes.n', 'freezes.over1s', 'freezes.total_ms', 'freezes.max_ms', 'text.dpi', 'text.purges', 'text.refused', 'text.rebuilds', 'text.lazy'];
  const get = (o, p) => p.split('.').reduce((a, k) => (a == null ? a : a[k]), o);
  function toCsv(rows) {
    const esc = (v) => { const s = v == null ? '' : String(v); return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s; };
    return [CSV_COLS.join(','), ...rows.map((r) => CSV_COLS.map((c) => esc(get(r, c))).join(','))].join('\n') + '\n';
  }
  // GET /telemetry?format=csv|json[&since=ms] et GET /telemetry/captures[?id=...] : 403 sans la bonne cle, 404 si ADMIN_KEY n'est pas definie sur le serveur.
  function handleGet(req, res, url) {
    if (!adminKey()) { res.writeHead(404); res.end('desactive'); return; }
    if (!authed(req, url)) { res.writeHead(403); res.end('cle invalide'); return; }
    if (url.pathname === '/telemetry/captures') {
      const id = url.searchParams.get('id');
      if (id) { const c = caps.get(id); if (!c) { res.writeHead(404); res.end(); return; } res.writeHead(200, { 'content-type': 'image/jpeg', 'cache-control': 'no-store' }); res.end(c.buf); return; }
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify([...caps.values()].map((c) => ({ id: c.id, sid: c.sid, kind: c.kind, at: c.at, bytes: c.buf.length, ctx: c.ctx, where: c.tctx }))));
      return;
    }
    const since = Number(url.searchParams.get('since')) || 0;
    const rows = [...sessions.values()].filter((s) => s.at >= since);
    if (url.searchParams.get('format') === 'csv') { res.writeHead(200, { 'content-type': 'text/csv; charset=utf-8', 'cache-control': 'no-store' }); res.end(toCsv(rows)); return; }
    res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' });
    res.end(JSON.stringify({ n: rows.length, caps: caps.size, sessions: rows }));
  }
  load();
  return { handlePost, handleGet, ingest, sessions, caps, stats, toCsv };
}
