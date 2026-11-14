// Donnees de jeu anonymes (envoyees seulement si le joueur a accepte, cote client) : POST /donnees { k: "partie" | "incident" | "rec" | "note", ... }
//   partie   : chiffres d'une partie (version, plateforme generique, heros, joueurs, duree, etage, cause de fin, cartes, boutons, degats, reanimations)
//   incident : deconnexion | reprise | desync | nouvelle_version | fluidite (+ version et plateforme generique)
//   rec      : enregistrement rejouable de la fin de partie (format .afhrec, `rec` de src/rejeu.rs), borne, les 40 derniers gardes
//   note     : trois visages (bien / moyen / pas_bien) relies a l'identifiant de partie `gid`
// Lecture (en-tete x-admin-key ou Authorization: Bearer, JAMAIS dans l'URL ; sans ADMIN_KEY : 404) :
//   GET /donnees/liste?coll=partie|incident|note|rec[&since=ms&limit=n]   GET /donnees/rec?id=...   GET /donnees/resume[?format=txt|json&days=n]
// Aucune adresse, aucun pseudo, aucun identifiant personnel : `gid` est un identifiant de partie aleatoire cote client ; l'IP ne sert qu'a limiter
// le debit (memoire). Le stockage (memoire / disque / PostgreSQL, purge) est celui de stockage.js.
import crypto from 'node:crypto';
import { cleanRec } from './reports.js';
import { dropBody } from './overflow.js';

export const MAX_BODY = 300 * 1024;
export const MAX_REC_B64 = 250 * 1024;
export const RATE_PER_MIN = 40;
export const RATE_PER_HOUR = 400;
export const MAX_PER_GID = 16;
export const BUTTONS = ['poing', 'pied', 'saut', 'garde', 'esquive', 'saisie', 'special'];
export const INCIDENTS = ['deconnexion', 'reprise', 'desync', 'nouvelle_version', 'fluidite'];
export const FACES = ['bien', 'moyen', 'pas_bien'];
export const ENDS = ['mort', 'abandon', 'victoire'];

const clean = (v, n) => String(v ?? '').replace(/[\u0000-\u0008\u000b-\u001f\u007f-\u009f\u200b-\u200f\u2028-\u202e]/g, '').slice(0, n);
const cleanKey = (v, n = 40) => clean(v, n).replace(/[^\w.:@+\-/ ]/g, '_');
const num = (v, hi = 1e9) => (Number.isFinite(v) ? Math.max(0, Math.min(hi, Math.round(v * 100) / 100)) : null);
const oneOf = (v, list, d = '') => (list.includes(v) ? v : d);
const GID = /^[0-9a-f]{8,16}$/;
const PERSONAL = /^(name|nom|pseudo|ip|email|user|addr|token|pid)/i;

// { cle: nombre } : au plus 80 cles, cles personnelles ecartees, nombres bornes.
function counters(o, max = 80) {
  const out = {};
  if (!o || typeof o !== 'object' || Array.isArray(o)) return out;
  for (const [k, v] of Object.entries(o).slice(0, max)) { if (PERSONAL.test(k)) continue; const n = num(Number(v), 1e7); if (n !== null) out[cleanKey(k, 40)] = n; }
  return out;
}
// Plateforme generique : rien qui identifie un appareil (pas de modele, pas de resolution exacte).
function platform(p) {
  const o = p && typeof p === 'object' ? p : {};
  return { kind: oneOf(o.kind, ['mobile', 'desktop'], 'inconnu'), input: oneOf(o.input, ['tactile', 'manette', 'clavier'], 'inconnu'), orient: oneOf(o.orient, ['portrait', 'paysage'], 'inconnu') };
}

export function cleanPartie(m) {
  if (!m || typeof m !== 'object' || !GID.test(String(m.gid || ''))) return null;
  const e = m.end && typeof m.end === 'object' ? m.end : {};
  const c = m.cards && typeof m.cards === 'object' ? m.cards : {};
  const b = counters(m.buttons, 16);
  const buttons = {}; for (const k of BUTTONS) buttons[k] = b[k] ?? 0;
  return {
    gid: String(m.gid), build: clean(m.build, 48), platform: platform(m.platform), hero: cleanKey(m.hero, 40), mode: cleanKey(m.mode, 24),
    players: num(m.players, 8) ?? 1, dur: num(m.dur, 1e6) ?? 0, floor: num(m.floor, 9999) ?? 0,
    end: { type: oneOf(e.type, ENDS, 'abandon'), by: cleanKey(e.by, 40), weapon: cleanKey(e.weapon, 40) },
    cards: { offered: counters(c.offered), taken: counters(c.taken), refused: counters(c.refused) },
    buttons, dmg_dealt: num(m.dmg_dealt, 1e9) ?? 0, dmg_taken: num(m.dmg_taken, 1e9) ?? 0, revives: num(m.revives, 999) ?? 0,
  };
}
export function cleanIncident(m) {
  if (!m || typeof m !== 'object' || !INCIDENTS.includes(m.type)) return null;
  return { type: m.type, gid: GID.test(String(m.gid || '')) ? String(m.gid) : '', build: clean(m.build, 48), platform: platform(m.platform), ms: num(m.ms, 1e8), n: num(m.n, 1e6) ?? 1 };
}
export function cleanNote(m) {
  if (!m || typeof m !== 'object' || !GID.test(String(m.gid || '')) || !FACES.includes(m.face)) return null;
  return { gid: String(m.gid), face: m.face, build: clean(m.build, 48) };
}
export function cleanRecord(m) {
  if (!m || typeof m !== 'object' || !GID.test(String(m.gid || ''))) return null;
  const rec = cleanRec(m.rec);
  if (!rec) return null;
  const b64 = rec.epochs.reduce((a, e) => a + e.b64.length, 0);
  if (b64 > MAX_REC_B64) return null;
  return { gid: String(m.gid), build: clean(m.build, 48), platform: platform(m.platform), hero: cleanKey(m.hero, 40), floor: num(m.floor, 9999) ?? 0, ticks: rec.epochs.reduce((a, e) => a + (e.ticks || 0), 0), b64, rec };
}

// ------------------------------------------------------------------ agregats
const median = (a) => { if (!a.length) return null; const s = [...a].sort((x, y) => x - y); const h = s.length >> 1; return s.length % 2 ? s[h] : (s[h - 1] + s[h]) / 2; };
const day = (at) => new Date(at).toISOString().slice(0, 10);
const top = (obj, n) => Object.entries(obj).sort((a, b) => b[1] - a[1] || (a[0] < b[0] ? -1 : 1)).slice(0, n);

export function aggregate(stock, opts = {}) {
  const since = opts.days ? (opts.now ?? Date.now()) - opts.days * 86400e3 : 0;
  const parties = stock.list('partie', { since }), incidents = stock.list('incident', { since }), notes = stock.list('note', { since });
  const perDay = {}; for (const p of parties) perDay[day(p.at)] = (perDay[day(p.at)] || 0) + 1;
  const causes = {}; let deaths = 0;
  for (const p of parties) if (p.end.type === 'mort') { deaths++; const k = p.end.by || 'inconnu'; causes[k] = (causes[k] || 0) + 1; }
  const off = {}, tak = {}, ref = {};
  for (const p of parties) {
    for (const [k, v] of Object.entries(p.cards.offered)) off[k] = (off[k] || 0) + v;
    for (const [k, v] of Object.entries(p.cards.taken)) tak[k] = (tak[k] || 0) + v;
    for (const [k, v] of Object.entries(p.cards.refused)) ref[k] = (ref[k] || 0) + v;
  }
  const cards = Object.keys(off).map((id) => ({ id, offered: off[id], taken: tak[id] || 0, refused: ref[id] || 0, rate: (tak[id] || 0) / off[id] }));
  const rated = cards.filter((c) => c.offered >= 3).sort((a, b) => b.rate - a.rate || b.offered - a.offered);
  const btn = {}; for (const k of BUTTONS) btn[k] = { total: 0, games: 0 };
  for (const p of parties) for (const k of BUTTONS) { const v = p.buttons[k] || 0; btn[k].total += v; if (v > 0) btn[k].games++; }
  const inc = {}; for (const i of incidents) inc[i.type] = (inc[i.type] || 0) + (i.n || 1);
  const faces = {}; for (const n of notes) faces[n.face] = (faces[n.face] || 0) + 1;
  const ends = {}; for (const p of parties) ends[p.end.type] = (ends[p.end.type] || 0) + 1;
  const plat = {}; for (const p of parties) { const k = `${p.platform.kind}/${p.platform.input}`; plat[k] = (plat[k] || 0) + 1; }
  const builds = {}; for (const p of parties) builds[p.build] = (builds[p.build] || 0) + 1;
  const dmgD = parties.map((p) => p.dmg_dealt), dmgT = parties.map((p) => p.dmg_taken);
  return {
    parties: parties.length, par_jour: perDay, duree_mediane_s: median(parties.map((p) => p.dur)), etage_median: median(parties.map((p) => p.floor)),
    fins: ends, morts: deaths, causes_de_mort: top(causes, 8), cartes_plus_prises: rated.slice(0, 5), cartes_moins_prises: rated.slice(-5).reverse(),
    boutons: btn, boutons_jamais_utilises: BUTTONS.filter((k) => btn[k].total === 0 && parties.length > 0),
    incidents: inc, notes: faces, plateformes: plat, versions: builds, degats_inflig_med: median(dmgD), degats_subis_med: median(dmgT),
    reanimations: parties.reduce((a, p) => a + p.revives, 0), enregistrements: stock.count('rec'),
  };
}

export function resumeTexte(a, info = {}) {
  const f = (x, d = 0) => (x === null || x === undefined ? '?' : Number(x).toFixed(d));
  const L = [];
  L.push(`DONNEES DE JEU (${a.parties} parties, ${a.enregistrements} enregistrements${info.mode ? ', stockage : ' + info.mode : ''})`);
  const days = Object.entries(a.par_jour).sort().slice(-7);
  L.push('Parties par jour : ' + (days.length ? days.map(([d, n]) => `${d.slice(5)}=${n}`).join(' ') : 'aucune'));
  L.push(`Duree mediane ${f(a.duree_mediane_s !== null ? a.duree_mediane_s / 60 : null, 1)} min ; etage median ${f(a.etage_median, 1)} ; fins : ${Object.entries(a.fins).map(([k, v]) => `${k} ${v}`).join(', ') || '-'}`);
  L.push('Morts, top causes : ' + (a.causes_de_mort.length ? a.causes_de_mort.map(([k, v]) => `${k} ${v}`).join(', ') : 'aucune'));
  const cs = (l) => (l.length ? l.map((c) => `${c.id} ${Math.round(c.rate * 100)}% (${c.taken}/${c.offered})`).join(', ') : 'pas assez de donnees');
  L.push('Cartes les plus prises : ' + cs(a.cartes_plus_prises));
  L.push('Cartes les moins prises : ' + cs(a.cartes_moins_prises));
  const used = BUTTONS.map((k) => `${k} ${a.boutons[k].games}/${a.parties}`).join(' ');
  L.push('Boutons (parties qui les utilisent) : ' + used);
  L.push('Boutons jamais utilises : ' + (a.boutons_jamais_utilises.length ? a.boutons_jamais_utilises.join(', ') : 'aucun'));
  L.push('Incidents : ' + (Object.keys(a.incidents).length ? Object.entries(a.incidents).map(([k, v]) => `${k} ${v}`).join(', ') : 'aucun'));
  L.push('Notes : ' + (Object.keys(a.notes).length ? FACES.map((k) => `${k} ${a.notes[k] || 0}`).join(', ') : 'aucune'));
  L.push(`Degats medians : infliges ${f(a.degats_inflig_med)} / subis ${f(a.degats_subis_med)} ; reanimations ${a.reanimations}`);
  L.push('Plateformes : ' + (Object.entries(a.plateformes).map(([k, v]) => `${k} ${v}`).join(', ') || '-') + ' ; versions : ' + (Object.entries(a.versions).map(([k, v]) => `${k} ${v}`).join(', ') || '-'));
  return L.join('\n') + '\n';
}

// ------------------------------------------------------------------ module HTTP
export function createDonnees(stock, opts = {}) {
  const adminKey = () => opts.adminKey ?? process.env.ADMIN_KEY ?? '';
  const perMin = opts.perMin ?? RATE_PER_MIN, perHour = opts.perHour ?? RATE_PER_HOUR;
  const rate = new Map();
  const perGid = new Map(); // gid -> envois (borne : 20000 identifiants)
  const seenPartie = new Set();
  const stats = { posts: 0, rejected: 0, stored: 0 };
  const cors = { 'access-control-allow-origin': '*' };

  function rateOk(ip) {
    const t = Date.now(); const l = (rate.get(ip) || []).filter((x) => t - x < 3600000);
    if (l.length >= perHour || l.filter((x) => t - x < 60000).length >= perMin) { rate.set(ip, l); return false; }
    l.push(t); rate.set(ip, l); if (rate.size > 5000) rate.clear();
    return true;
  }
  // Un message decode : renvoie { id } ou { err }.
  function ingest(m) {
    stats.posts++;
    const bad = () => { stats.rejected++; return { err: 'bad' }; };
    if (!m || typeof m !== 'object') return bad();
    const gid = String(m.gid || '');
    if (GID.test(gid)) {
      const n = (perGid.get(gid) || 0) + 1;
      if (n > MAX_PER_GID) return bad();
      perGid.set(gid, n); if (perGid.size > 20000) perGid.clear();
    }
    let id = '';
    switch (m.k) {
      case 'partie': {
        const c = cleanPartie(m); if (!c) return bad();
        if (seenPartie.has(c.gid)) return { id: 'dup' };
        seenPartie.add(c.gid); if (seenPartie.size > 20000) seenPartie.clear();
        id = stock.add('partie', c); break;
      }
      case 'incident': { const c = cleanIncident(m); if (!c) return bad(); id = stock.add('incident', c); break; }
      case 'note': {
        const c = cleanNote(m); if (!c) return bad();
        if (stock.list('note', { where: (x) => x.gid === c.gid }).length) return { id: 'dup' };
        id = stock.add('note', c); break;
      }
      case 'rec': {
        const c = cleanRecord(m); if (!c) return bad();
        const { rec, ...meta } = c;
        id = stock.add('rec', meta, { blob: Buffer.from(JSON.stringify(rec)), ext: 'json' }); break;
      }
      default: return bad();
    }
    if (!id) return bad();
    stats.stored++;
    return { id };
  }
  function handlePost(req, res, ip = '') {
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
      let r; try { r = ingest(m); } catch { r = { err: 'bad' }; }
      if (r.err) { res.writeHead(400, cors); res.end(); return; }
      res.writeHead(201, { ...cors, 'content-type': 'application/json' }); res.end(JSON.stringify({ ok: true }));
    });
  }
  // En-tete seulement : jamais dans l'URL (elle finirait dans les journaux).
  function authed(req) {
    const k = adminKey();
    if (!k) return false;
    const got = String(req.headers['x-admin-key'] || (/^Bearer (.+)$/.exec(req.headers.authorization || '') || [])[1] || '');
    return crypto.timingSafeEqual(crypto.createHash('sha256').update(got).digest(), crypto.createHash('sha256').update(k).digest());
  }
  const json = (res, code, o) => { res.writeHead(code, { 'content-type': 'application/json', 'cache-control': 'no-store' }); res.end(JSON.stringify(o)); };
  async function handleGet(req, res, url) {
    if (!adminKey()) { res.writeHead(404); res.end('desactive'); return; }
    if (!authed(req)) { res.writeHead(403); res.end('cle invalide'); return; }
    const p = url.pathname, q = url.searchParams;
    try {
      if (p === '/donnees/resume') {
        const a = aggregate(stock, { days: Number(q.get('days')) || 0 });
        if (q.get('format') === 'json') return json(res, 200, a);
        res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8', 'cache-control': 'no-store' }); res.end(resumeTexte(a, { mode: stock.mode })); return;
      }
      if (p === '/donnees/liste') {
        const coll = q.get('coll') || 'partie';
        if (!['partie', 'incident', 'note', 'rec'].includes(coll)) { res.writeHead(400); res.end(); return; }
        const rows = stock.list(coll, { since: Number(q.get('since')) || 0, limit: Math.min(5000, Number(q.get('limit')) || 1000) });
        return json(res, 200, { coll, n: rows.length, rows });
      }
      if (p === '/donnees/rec') {
        const id = q.get('id') || '';
        const m = stock.get('rec', id);
        const b = m && (await stock.blob('rec', id));
        if (!b) { res.writeHead(404); res.end(); return; }
        res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' }); res.end(b); return;
      }
    } catch { /* jamais d'exception vers le relais */ }
    res.writeHead(404); res.end();
  }
  return { handlePost, handleGet, ingest, stats };
}
