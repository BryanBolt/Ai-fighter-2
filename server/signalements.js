// Signalements volontaires des joueurs (bouton « Signaler » en jeu) : POST /signalement (capture + note + etiquette + contexte + rejeu),
// GET /signalements (liste), GET /signalements?id=...&format=json|rec, GET /signalements/img?id=... : lecture protegee par la variable
// d'environnement ADMIN_KEY (jamais ecrite dans le depot ni dans les journaux ; sans ADMIN_KEY la lecture repond 404).
// Aucune donnee personnelle : pas de pseudo, pas d'adresse (l'IP ne sert qu'a limiter le debit, en memoire).
// Stockage borne : MAX_KEEP signalements gardes EN MEMOIRE (les plus anciens partent) ; copie sur disque (un .json + une image par signalement,
// dans SIGNALEMENTS_DIR) avec rotation : MAX_FILES signalements et MAX_DISK_BYTES au plus. ATTENTION : sur Render (offre gratuite) le disque
// n'est PAS persistant (efface a chaque deploiement ou reveil) : c'est la memoire qui sert, donc recuperer les signalements avec
// tools/signalements.sh avant un redeploiement. Un signalement est volontaire : il est accepte meme si le joueur a refuse la telemetrie.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { cleanRec } from './reports.js';
import { dropBody } from './overflow.js';

export const MAX_BODY = 640 * 1024;
export const MAX_IMG_BYTES = 160 * 1024; // image decodee (JPEG ou PNG)
export const MAX_NOTE = 400;
export const MAX_KEEP = 60;
export const MAX_FILES = 200;
export const MAX_DISK_BYTES = 40 * 1024 * 1024;
export const RATE_PER_MIN = 6;
export const RATE_PER_HOUR = 40;
export const TAGS = ['bug', 'moche', 'pas_clair', 'idee', 'autre'];

const clean = (v, n) => String(v ?? '').replace(/[\u0000-\u0008\u000b-\u001f\u007f-\u009f\u200b-\u200f\u2028-\u202e]/g, '').slice(0, n);
const cleanKey = (v, n = 40) => clean(v, n).replace(/[^\w.:@+\-/ ]/g, '_');
const num = (v, hi = 1e9) => (Number.isFinite(v) ? Math.max(-1, Math.min(hi, Math.round(v * 100) / 100)) : null);
const PERSONAL = /^(name|nom|pseudo|ip|email|user|addr|token|pid)/i;

// Dictionnaire { cle: texte court } (contexte du jeu) : au plus 40 cles, cles personnelles ecartees.
function dict(o) {
  const out = {};
  if (!o || typeof o !== 'object' || Array.isArray(o)) return out;
  for (const [k, v] of Object.entries(o).slice(0, 40)) { if (!PERSONAL.test(k)) out[cleanKey(k, 32)] = clean(v, 120); }
  return out;
}

// Decode l'image `data:image/(jpeg|png);base64,...` : renvoie { buf, ext } ou null (type annonce ET octets magiques verifies, taille bornee).
export function decodeImg(s) {
  const mt = /^data:image\/(jpeg|png);base64,([A-Za-z0-9+/=]+)$/.exec(String(s || ''));
  if (!mt) return null;
  const buf = Buffer.from(mt[2], 'base64');
  if (buf.length < 100 || buf.length > MAX_IMG_BYTES) return null;
  const jpeg = buf[0] === 0xff && buf[1] === 0xd8, png = buf[0] === 0x89 && buf[1] === 0x50 && buf[2] === 0x4e && buf[3] === 0x47;
  if (mt[1] === 'jpeg' ? !jpeg : !png) return null;
  return { buf, ext: mt[1] === 'jpeg' ? 'jpg' : 'png' };
}

// Normalise un signalement ; renvoie null s'il est inutilisable (ni note, ni etiquette, ni image : rien a lire).
export function cleanSignalement(m) {
  if (!m || typeof m !== 'object' || Array.isArray(m)) return null;
  const img = m.img ? decodeImg(m.img) : null;
  if (m.img && !img) return null;
  const tag = TAGS.includes(m.tag) ? m.tag : '';
  const note = clean(m.note, MAX_NOTE).trim();
  if (!img && !note && !tag) return null;
  const scr = m.screen && typeof m.screen === 'object' ? m.screen : {};
  const rec = cleanRec(m.rec);
  const snap = m.snap && typeof m.snap === 'object' ? JSON.stringify(m.snap).slice(0, 6000) : '';
  const out = {
    note, tag, build: clean(m.build, 48), proto: num(m.proto, 1000), version: clean(m.version, 24), platform: cleanKey(m.platform, 16),
    hero: cleanKey(m.hero, 40), place: cleanKey(m.place, 60), floor: num(m.floor, 9999), seed: clean(m.seed, 24), mode: cleanKey(m.mode, 24),
    input: cleanKey(m.input, 16), screen: { w: num(scr.w, 20000), h: num(scr.h, 20000), dpr: num(scr.dpr, 16) }, fps: num(m.fps, 1000), online: !!m.online,
    ctx: dict(m.ctx), sid: /^[0-9a-f]{6,16}$/.test(String(m.sid || '')) ? String(m.sid) : '', sent_late: !!m.late,
    // Rejouabilite : `rec` = enregistrement de rejeu du moteur (rejouable en local) ; sinon `snap` = instantane descriptif de l'etat + entrees recentes.
    replay: rec ? 'rec' : snap ? 'snap' : 'none', rec, snap: snap ? JSON.parse(snap.length < 6000 ? snap : '{}') : null,
    inputs: typeof m.inputs === 'string' && /^[A-Za-z0-9+/=]*$/.test(m.inputs) ? m.inputs.slice(0, 8000) : '',
  };
  return { out, img };
}

export function createSignalements(opts = {}) {
  const dir = opts.dir ?? process.env.SIGNALEMENTS_DIR ?? 'signalements';
  const adminKey = () => opts.adminKey ?? process.env.ADMIN_KEY ?? '';
  const keep = opts.keep ?? MAX_KEEP, maxFiles = opts.maxFiles ?? MAX_FILES, maxDisk = opts.maxDisk ?? MAX_DISK_BYTES;
  const perMin = opts.perMin ?? RATE_PER_MIN, perHour = opts.perHour ?? RATE_PER_HOUR;
  const mem = new Map(); // id -> { meta, img, ext } (les plus recents)
  const rate = new Map();
  const stats = { posts: 0, rejected: 0, stored: 0 };
  let seq = 0;

  function rateOk(ip) {
    const now = Date.now(); const l = (rate.get(ip) || []).filter((t) => now - t < 3600000);
    if (l.length >= perHour || l.filter((t) => now - t < 60000).length >= perMin) { rate.set(ip, l); return false; }
    l.push(now); rate.set(ip, l); if (rate.size > 5000) rate.clear();
    return true;
  }
  // Rotation du disque : au plus maxFiles signalements et maxDisk octets, les plus anciens d'abord.
  function rotate() {
    if (opts.noDisk) return;
    try {
      const names = fs.readdirSync(dir).filter((n) => /^[0-9]{13}-[0-9a-f]{8}\./.test(n));
      const ids = [...new Set(names.map((n) => n.split('.')[0]))].sort();
      const size = (id) => names.filter((n) => n.startsWith(id + '.')).reduce((a, n) => { try { return a + fs.statSync(path.join(dir, n)).size; } catch { return a; } }, 0);
      let total = ids.reduce((a, id) => a + size(id), 0);
      while (ids.length > 1 && (ids.length > maxFiles || total > maxDisk)) {
        const id = ids.shift(); total -= size(id);
        for (const n of names.filter((x) => x.startsWith(id + '.'))) { try { fs.unlinkSync(path.join(dir, n)); } catch { /* deja parti */ } }
      }
    } catch { /* dossier absent */ }
  }
  function load() {
    if (opts.noDisk) return;
    try {
      const ids = fs.readdirSync(dir).filter((n) => /^[0-9]{13}-[0-9a-f]{8}\.json$/.test(n)).map((n) => n.slice(0, -5)).sort().slice(-keep);
      for (const id of ids) {
        try {
          const meta = JSON.parse(fs.readFileSync(path.join(dir, id + '.json'), 'utf8'));
          const ext = meta.img_ext; let img = null;
          if (ext) { try { img = fs.readFileSync(path.join(dir, id + '.' + ext)); } catch { /* image perdue */ } }
          mem.set(id, { meta, img, ext: img ? ext : '' });
        } catch { /* fichier abime */ }
      }
    } catch { /* pas de dossier */ }
  }
  function store(c) {
    const at = Date.now();
    const id = String(at).padStart(13, '0') + '-' + crypto.createHash('sha1').update(String(seq++) + ':' + at + ':' + (c.img ? c.img.buf.length : 0) + c.out.note).digest('hex').slice(0, 8);
    const meta = { id, at, ...c.out, img_ext: c.img ? c.img.ext : '', img_bytes: c.img ? c.img.buf.length : 0 };
    mem.set(id, { meta, img: c.img ? c.img.buf : null, ext: c.img ? c.img.ext : '' });
    while (mem.size > keep) mem.delete(mem.keys().next().value);
    if (!opts.noDisk) {
      try {
        fs.mkdirSync(dir, { recursive: true });
        fs.writeFileSync(path.join(dir, id + '.json'), JSON.stringify(meta));
        if (c.img) fs.writeFileSync(path.join(dir, id + '.' + c.img.ext), c.img.buf);
        rotate();
      } catch { /* disque en lecture seule : la memoire suffit */ }
    }
    stats.stored++;
    return id;
  }
  // Un signalement (objet deja decode) : renvoie { id } ou { err }.
  function ingest(m) {
    stats.posts++;
    const c = cleanSignalement(m);
    if (!c) { stats.rejected++; return { err: 'bad' }; }
    return { id: store(c) };
  }
  function handlePost(req, res, ip = '') {
    const cors = { 'access-control-allow-origin': '*' };
    if (req.method === 'OPTIONS') { res.writeHead(204, { ...cors, 'access-control-allow-methods': 'POST', 'access-control-allow-headers': 'content-type' }); res.end(); return; }
    if (req.method !== 'POST') { res.writeHead(405, cors); res.end(); return; }
    if (!rateOk(ip)) { stats.rejected++; res.writeHead(429, cors); res.end('trop vite'); return; }
    const declared = Number(req.headers['content-length']);
    if (declared > MAX_BODY) { stats.rejected++; res.writeHead(413, cors); res.end('trop gros'); dropBody(req); return; }
    let size = 0; const chunks = [];
    req.on('data', (c) => { size += c.length; if (size > MAX_BODY) { if (!res.writableEnded) { stats.rejected++; res.writeHead(413, cors); res.end('trop gros'); } dropBody(req); } else chunks.push(c); });
    req.on('end', () => {
      if (res.writableEnded) return;
      let m = null; try { m = JSON.parse(Buffer.concat(chunks).toString('utf8')); } catch { /* json invalide */ }
      const r = ingest(m);
      if (r.err) { res.writeHead(400, cors); res.end(); return; }
      res.writeHead(201, { ...cors, 'content-type': 'application/json' }); res.end(JSON.stringify({ id: r.id }));
    });
  }
  function authed(req, url) {
    const k = adminKey();
    if (!k) return false;
    const got = String(req.headers['x-admin-key'] || (/^Bearer (.+)$/.exec(req.headers.authorization || '') || [])[1] || url.searchParams.get('key') || '');
    const a = crypto.createHash('sha256').update(got).digest(); const b = crypto.createHash('sha256').update(k).digest();
    return crypto.timingSafeEqual(a, b);
  }
  const summary = (e) => { const { rec, snap, inputs, ...rest } = e.meta; return { ...rest, rec_bytes: rec ? rec.epochs.reduce((a, x) => a + x.b64.length, 0) : 0, has_snap: !!snap }; };
  // GET /signalements[?since=ms] : liste (sans rejeu) ; ?id=..[&format=rec] : un signalement complet ou son rejeu seul ; GET /signalements/img?id=.. : l'image.
  function handleGet(req, res, url) {
    if (!adminKey()) { res.writeHead(404); res.end('desactive'); return; }
    if (!authed(req, url)) { res.writeHead(403); res.end('cle invalide'); return; }
    const id = url.searchParams.get('id');
    if (url.pathname === '/signalements/img') {
      const e = id && mem.get(id);
      if (!e || !e.img) { res.writeHead(404); res.end(); return; }
      res.writeHead(200, { 'content-type': e.ext === 'png' ? 'image/png' : 'image/jpeg', 'cache-control': 'no-store' }); res.end(e.img); return;
    }
    if (id) {
      const e = mem.get(id);
      if (!e) { res.writeHead(404); res.end(); return; }
      if (url.searchParams.get('format') === 'rec') {
        if (!e.meta.rec) { res.writeHead(404); res.end('pas de rejeu'); return; }
        res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' }); res.end(JSON.stringify(e.meta.rec)); return;
      }
      res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' }); res.end(JSON.stringify(e.meta)); return;
    }
    const since = Number(url.searchParams.get('since')) || 0;
    const rows = [...mem.values()].filter((e) => e.meta.at > since).map(summary);
    res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' });
    res.end(JSON.stringify({ n: rows.length, kept: mem.size, signalements: rows }));
  }
  load();
  return { handlePost, handleGet, ingest, mem, stats, rotate };
}
