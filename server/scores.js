// Dimension sociale SANS COMPTE : classements en ligne (run du jour, runs roguelite, defis entre amis) et presence des amis.
// Aucune donnee personnelle : un pseudo assaini (14 caracteres) et un identifiant anonyme aleatoire (`pid`, 16 hex) tires par le client.
// Rien n'est stocke sans le consentement explicite du client (`consent: true`) ; `forget` efface tout ce qui porte un `pid`.
// Anti-triche leger (le serveur ne rejoue pas la simulation) : graine du jour recalculee, code de defi derive de la graine,
// bornes de plausibilite score / temps / etage, une seule entree par `pid` et par tableau, limites de debit par adresse et par `pid`.
// Persistance : un fichier JSON (ecriture atomique, tailles bornees). Sans fichier (tests) : memoire seule.
import fs from 'node:fs';

const M = (1n << 64n) - 1n;
// Meme melangeur que `rl_daily::mix` cote Rust (test croise dans les deux langages).
export function mix(z) {
  z = (z + 0x9E3779B97F4A7C15n) & M;
  z = ((z ^ (z >> 30n)) * 0xBF58476D1CE4E5B9n) & M;
  z = ((z ^ (z >> 27n)) * 0x94D049BB133111EBn) & M;
  return z ^ (z >> 31n);
}
export const daySeed = (day) => mix(BigInt(day) ^ 0xDA115EED00000001n);
const ALPHA = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
// Code court d'un defi (meme graine = meme code) : 6 caracteres. Meme formule que `social::challenge_code` cote Rust.
export function chalCode(seed) {
  let z = mix(BigInt(seed) ^ 0xC4A11E4600000000n), out = '';
  for (let i = 0; i < 6; i++) { out += ALPHA[Number(z & 31n)]; z >>= 5n; }
  return out;
}

export const LIMITS = {
  topMax: 100, queryMax: 50, dailyDays: 8, chalMax: 300, chalTop: 50, scoreMax: 400000, stageMax: 60, heatMax: 12, heroMax: 99,
  bodyMax: 1024, ipGapMs: 2000, ipWindowMs: 10 * 60 * 1000, ipWindowMax: 40, pidGapMs: 5000, presenceTtlMs: 90 * 1000, presenceMax: 5000, presenceQuery: 20,
};

const PID_RE = /^[0-9a-f]{16}$/;
const FC_RE = /^[A-Z0-9]{5,8}$/;
const CHAL_RE = /^[A-Z0-9]{6}$/;
const int = (v, lo, hi) => (Number.isInteger(v) && v >= lo && v <= hi ? v : null);

// Pseudo assaini : lettres / chiffres / espace / _ - . ' seulement, 14 caracteres, pas de lien ni d'adresse.
export function cleanPseudo(v) {
  let s = String(v ?? '').normalize('NFKC').replace(/[^\p{L}\p{N} _\-.']/gu, '').split(' ').filter(Boolean).join(' ').slice(0, 14).trim();
  if (/https?|www|\.(com|net|org|fr|io|gg)\b|@/i.test(String(v ?? '')) || !s) s = 'Anonyme';
  return s;
}

// Plausibilite d'un score : borne en fonction de l'etage et de la duree reelle de la partie.
export function plausible(board, e) {
  if (e.ms < 4000) return 'trop court';
  const secs = e.ms / 1000;
  if (board === 'rl') {
    if (e.stage < 1) return 'etage';
    if (e.ms < 5000 * e.stage) return 'trop court pour cet etage';
    if (e.score > 8000 + secs * 450 + e.stage * 4000) return 'score implausible';
  } else {
    if (e.score > 3000 + secs * 150 + e.stage * 600) return 'score implausible';
  }
  return null;
}

export function createScores({ file = null, now = () => Date.now() } = {}) {
  let db = { daily: {}, rl: [], chal: {} };
  if (file) {
    try {
      const j = JSON.parse(fs.readFileSync(file, 'utf8'));
      if (j && typeof j === 'object') db = { daily: j.daily && typeof j.daily === 'object' ? j.daily : {}, rl: Array.isArray(j.rl) ? j.rl : [], chal: j.chal && typeof j.chal === 'object' ? j.chal : {} };
    } catch { /* premiere fois ou fichier abime : on repart de zero */ }
  }
  const ipLog = new Map(); // ip -> { last, stamps[] }
  const pidLast = new Map();
  const presence = new Map(); // code d'ami -> { pid, name, room, at }
  let timer = null;

  const today = () => Math.floor(now() / 86400000);
  const sorted = (a) => a.sort((x, y) => y.score - x.score || x.ms - y.ms || x.at - y.at);
  function prune() {
    const days = Object.keys(db.daily).map(Number).sort((a, b) => b - a);
    for (const d of days.slice(LIMITS.dailyDays)) delete db.daily[d];
    const keys = Object.keys(db.chal);
    if (keys.length > LIMITS.chalMax) {
      keys.sort((a, b) => Math.max(...db.chal[a].map((e) => e.at)) - Math.max(...db.chal[b].map((e) => e.at)));
      for (const k of keys.slice(0, keys.length - LIMITS.chalMax)) delete db.chal[k];
    }
  }
  function flush() {
    if (timer) { clearTimeout(timer); timer = null; }
    if (!file) return;
    try { fs.writeFileSync(file + '.tmp', JSON.stringify(db)); fs.renameSync(file + '.tmp', file); } catch { /* disque en lecture seule */ }
  }
  function later() { if (file && !timer) { timer = setTimeout(flush, 400); timer.unref?.(); } }
  const list = (board, key) => (board === 'rl' ? db.rl : board === 'daily' ? db.daily[key] : db.chal[key]) || null;

  function rateOk(ip, pid, t) {
    const l = ipLog.get(ip) || { last: 0, stamps: [] };
    l.stamps = l.stamps.filter((s) => t - s < LIMITS.ipWindowMs);
    if (t - l.last < LIMITS.ipGapMs || l.stamps.length >= LIMITS.ipWindowMax) return false;
    if (t - (pidLast.get(pid) || 0) < LIMITS.pidGapMs) return false;
    l.last = t; l.stamps.push(t); ipLog.set(ip, l); pidLast.set(pid, t);
    if (ipLog.size > 5000) ipLog.clear();
    if (pidLast.size > 5000) pidLast.clear();
    return true;
  }

  // m : { board, consent, pid, name, score, stage, ms, hero, heat, day?, seed?, code? }
  function submit(ip, m) {
    if (!m || typeof m !== 'object' || Array.isArray(m)) return { ok: false, err: 'entree invalide', code: 'bad' };
    if (m.consent !== true) return { ok: false, err: 'consentement requis', code: 'consent' };
    const board = m.board;
    if (board !== 'daily' && board !== 'rl' && board !== 'chal') return { ok: false, err: 'tableau inconnu', code: 'bad' };
    const pid = String(m.pid ?? '');
    if (!PID_RE.test(pid)) return { ok: false, err: 'identifiant invalide', code: 'bad' };
    const e = {
      score: int(m.score, 0, LIMITS.scoreMax), stage: int(m.stage, 0, LIMITS.stageMax), ms: int(m.ms, 0, 24 * 3600 * 1000),
      hero: int(m.hero, 0, LIMITS.heroMax), heat: int(m.heat ?? 0, 0, LIMITS.heatMax),
    };
    if (Object.values(e).some((v) => v === null)) return { ok: false, err: 'valeurs hors bornes', code: 'bad' };
    let key = '';
    if (board === 'daily') {
      const day = int(m.day, 0, 1e6), t = today();
      if (day === null || Math.abs(day - t) > 1) return { ok: false, err: 'jour invalide', code: 'seed' };
      if (String(m.seed) !== daySeed(day).toString()) return { ok: false, err: 'graine invalide', code: 'seed' };
      key = String(day);
    } else if (board === 'chal') {
      if (!/^\d{1,20}$/.test(String(m.seed ?? ''))) return { ok: false, err: 'graine invalide', code: 'seed' };
      const code = String(m.code ?? '').toUpperCase();
      if (!CHAL_RE.test(code) || chalCode(m.seed) !== code) return { ok: false, err: 'code de defi invalide', code: 'seed' };
      key = code;
    } else if (m.seed !== undefined && !/^\d{1,20}$/.test(String(m.seed))) return { ok: false, err: 'graine invalide', code: 'seed' };
    const why = plausible(board, e);
    if (why) return { ok: false, err: why, code: 'implausible' };
    const t = now();
    if (!rateOk(ip, pid, t)) return { ok: false, err: 'trop vite', code: 'rate' };
    let arr;
    if (board === 'rl') arr = db.rl;
    else if (board === 'daily') arr = db.daily[key] || (db.daily[key] = []);
    else arr = db.chal[key] || (db.chal[key] = []);
    const entry = { pid, name: cleanPseudo(m.name), ...e, at: t };
    const i = arr.findIndex((x) => x.pid === pid);
    let improved = true;
    if (i >= 0) { if (arr[i].score >= e.score) { improved = false; arr[i].name = entry.name; } else arr[i] = entry; } else arr.push(entry);
    sorted(arr);
    const cap = board === 'chal' ? LIMITS.chalTop : LIMITS.topMax;
    if (arr.length > cap) arr.length = cap;
    prune(); later();
    const r = arr.findIndex((x) => x.pid === pid);
    return { ok: true, improved, rank: r >= 0 ? r + 1 : null, total: arr.length, best: r >= 0 ? arr[r].score : null };
  }

  // q : { board, day?, code?, pid?, limit? } -> { ok, board, key, top:[...], total, me }
  function query(q) {
    const board = q && q.board;
    if (board !== 'daily' && board !== 'rl' && board !== 'chal') return { ok: false, err: 'tableau inconnu', code: 'bad' };
    let key = '';
    if (board === 'daily') key = String(q.day === undefined || q.day === '' ? today() : q.day);
    if (board === 'chal') key = String(q.code ?? '').toUpperCase();
    if (board === 'daily' && !/^\d{1,7}$/.test(key)) return { ok: false, err: 'jour invalide', code: 'bad' };
    if (board === 'chal' && !CHAL_RE.test(key)) return { ok: false, err: 'code invalide', code: 'bad' };
    const arr = list(board, key) || [];
    const limit = Math.max(1, Math.min(LIMITS.queryMax, Number.parseInt(q.limit, 10) || 10));
    const pid = String(q.pid ?? '');
    const mi = PID_RE.test(pid) ? arr.findIndex((x) => x.pid === pid) : -1;
    const pub = (e, i) => ({ rank: i + 1, name: e.name, score: e.score, stage: e.stage, ms: e.ms, hero: e.hero, heat: e.heat, me: i === mi });
    return { ok: true, board, key, top: arr.slice(0, limit).map(pub), total: arr.length, me: mi >= 0 ? pub(arr[mi], mi) : null };
  }

  function forget(pid) {
    pid = String(pid ?? '');
    if (!PID_RE.test(pid)) return { ok: false, err: 'identifiant invalide', code: 'bad' };
    let n = 0;
    const cut = (arr) => { const k = arr.length; const r = arr.filter((e) => e.pid !== pid); n += k - r.length; return r; };
    db.rl = cut(db.rl);
    for (const d of Object.keys(db.daily)) { db.daily[d] = cut(db.daily[d]); if (!db.daily[d].length) delete db.daily[d]; }
    for (const c of Object.keys(db.chal)) { db.chal[c] = cut(db.chal[c]); if (!db.chal[c].length) delete db.chal[c]; }
    for (const [fc, p] of presence) if (p.pid === pid) presence.delete(fc);
    flush();
    return { ok: true, removed: n };
  }

  // Presence des amis : memoire seule, TTL court, uniquement pour les clients qui l'ont activee.
  function presenceSet(m) {
    const fc = String(m?.fc ?? '').toUpperCase(), pid = String(m?.pid ?? '');
    if (m?.consent !== true) return { ok: false, err: 'consentement requis', code: 'consent' };
    if (!FC_RE.test(fc) || !PID_RE.test(pid)) return { ok: false, err: 'entree invalide', code: 'bad' };
    const t = now(), cur = presence.get(fc);
    if (cur && cur.pid !== pid && t - cur.at < LIMITS.presenceTtlMs) return { ok: false, err: 'code deja pris', code: 'taken' };
    if (!cur && presence.size >= LIMITS.presenceMax) { for (const [k, p] of presence) if (t - p.at > LIMITS.presenceTtlMs) presence.delete(k); if (presence.size >= LIMITS.presenceMax) return { ok: false, err: 'complet', code: 'full' }; }
    const room = String(m.room ?? '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 8);
    presence.set(fc, { pid, name: cleanPseudo(m.name), room: room.length >= 4 ? room : '', at: t });
    return { ok: true };
  }
  function presenceGet(m) {
    const codes = (Array.isArray(m?.codes) ? m.codes : []).slice(0, LIMITS.presenceQuery).map((c) => String(c).toUpperCase()).filter((c) => FC_RE.test(c));
    const t = now();
    return { ok: true, friends: codes.map((fc) => { const p = presence.get(fc); const on = !!p && t - p.at < LIMITS.presenceTtlMs; return { fc, online: on, name: on ? p.name : '', room: on ? p.room : '' }; }) };
  }
  function presenceClear(m) {
    const fc = String(m?.fc ?? '').toUpperCase(), pid = String(m?.pid ?? '');
    const p = presence.get(fc);
    if (p && p.pid === pid) presence.delete(fc);
    return { ok: true };
  }

  // Routes HTTP (lecture libre en GET ; ecriture en POST, corps borne). Retourne true si traitee.
  function handle(req, res, url, ip) {
    const p = url.pathname;
    if (!p.startsWith('/api/scores')) return false;
    const reply = (code, obj) => { res.writeHead(code, { 'content-type': 'application/json', 'cache-control': 'no-store', 'access-control-allow-origin': '*' }); res.end(JSON.stringify(obj)); return true; };
    if (req.method === 'OPTIONS') { res.writeHead(204, { 'access-control-allow-origin': '*', 'access-control-allow-methods': 'GET, POST', 'access-control-allow-headers': 'content-type' }); res.end(); return true; }
    if (p === '/api/scores' && req.method === 'GET') {
      const r = query(Object.fromEntries(url.searchParams));
      return reply(r.ok ? 200 : 400, r);
    }
    if ((p === '/api/scores' || p === '/api/scores/forget') && req.method === 'POST') {
      let body = '', dead = false;
      req.on('data', (d) => { body += d; if (body.length > LIMITS.bodyMax && !dead) { dead = true; reply(413, { ok: false, err: 'trop gros' }); req.destroy(); } });
      req.on('end', () => {
        if (dead) return;
        let j; try { j = JSON.parse(body); } catch { return reply(400, { ok: false, err: 'json invalide' }); }
        const r = p === '/api/scores' ? submit(ip, j) : forget(j && j.pid);
        reply(r.ok ? 200 : r.code === 'rate' ? 429 : 400, r);
      });
      return true;
    }
    return reply(404, { ok: false, err: 'introuvable' });
  }

  return { submit, query, forget, presenceSet, presenceGet, presenceClear, handle, flush, db: () => db, today };
}
