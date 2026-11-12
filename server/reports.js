// Rapports d'erreurs des joueurs : POST /report (valide, borne, ecrit en JSON-lines) et GET /reports (regroupes par empreinte).
// Aucune donnee personnelle n'est stockee : pas d'adresse IP (seulement en memoire pour limiter le debit), pas de pseudo.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';

export const KINDS = ['panic', 'js', 'net', 'asset', 'audio', 'webgl', 'wasm', 'ws', 'desync', 'stall', 'user', 'other'];
const MAX_BODY = 512 * 1024; // le rapport sans enregistrement reste sous 64 Ko ; l'enregistrement de rejeu (`rec`) peut en ajouter ~250 Ko
const MAX_REC = 400 * 1024; // enregistrement de rejeu (base64, toutes epoques) : au-dela il est ecarte, le rapport est garde
const MAX_DIAG = 40 * 1024; // diagnostic de partie en ligne (serialise)
const MAX_CRUMBS = 50;

const clean = (v, n) => String(v ?? '').replace(/[\u0000-\u0008\u000b-\u001f\u007f-\u009f\u200b-\u200f\u2028-\u202e]/g, '').slice(0, n);
const cleanKey = (v, n) => clean(v, n).replace(/[^\w.:@+\-/ ]/g, '_');
const PERSONAL = /^(name|nom|pseudo|ip|email|user|addr|token)/i;

// Nettoyage recursif du diagnostic : profondeur, longueurs et nombres bornes, cles personnelles ecartees.
// Enregistrement de rejeu (src/rejeu.rs) : liste d'epoques {b64 (zlib+base64), ticks, mode, slots, me, trunc, build, proto, platform}.
// Opaque pour le serveur (aucune donnee perso : pas de pseudo, validé cote client) ; on ne garde que ce qui a la bonne forme.
export function cleanRec(r) {
  if (!r || typeof r !== 'object' || Array.isArray(r) || !Array.isArray(r.epochs)) return null;
  const n = (v, hi) => (Number.isFinite(v) && v >= 0 && v <= hi ? Math.round(v) : null);
  const epochs = [];
  let total = 0;
  for (const e of r.epochs.slice(0, 3)) {
    if (!e || typeof e !== 'object' || typeof e.b64 !== 'string' || !/^[A-Za-z0-9+/=]+$/.test(e.b64)) continue;
    total += e.b64.length;
    if (total > MAX_REC) break;
    epochs.push({ b64: e.b64, ticks: n(e.ticks, 1e9), mode: n(e.mode, 99), full: !!e.full, slots: n(e.slots, 8), me: n(e.me, 8), trunc: !!e.trunc, build: cleanKey(e.build, 48), proto: n(e.proto, 1000), platform: cleanKey(e.platform, 16) });
  }
  return epochs.length ? { v: 1, epochs } : null;
}

// Resume d'un enregistrement sans les donnees (liste des rapports) : { epochs: [{ticks, mode, slots, me, trunc, bytes}] }.
const recSummary = (rec) => (rec ? { epochs: rec.epochs.map((e) => ({ ticks: e.ticks, mode: e.mode, slots: e.slots, me: e.me, trunc: e.trunc, build: e.build, bytes: e.b64.length })) } : undefined);

// Identifiant court d'un rapport (calcule a la lecture : date + session + empreinte), pour tools/replay_report.sh.
export const idOf = (r) => crypto.createHash('sha1').update(`${r.at}|${r.sid}|${r.fp}`).digest('hex').slice(0, 10);

export function cleanDeep(v, depth = 0) {
  if (v === null || typeof v === 'boolean') return v;
  if (typeof v === 'number') return Number.isFinite(v) ? v : null;
  if (typeof v === 'string') return clean(v, 200);
  if (depth >= 6) return null;
  if (Array.isArray(v)) return v.slice(0, 160).map((x) => cleanDeep(x, depth + 1));
  if (typeof v === 'object') {
    const o = {};
    for (const [k, x] of Object.entries(v).slice(0, 48)) {
      if (PERSONAL.test(k)) continue;
      o[cleanKey(k, 32)] = cleanDeep(x, depth + 1);
    }
    return o;
  }
  return null;
}

// Normalise le rapport recu : types connus, tailles bornees. Renvoie null si inutilisable.
export function cleanReport(r) {
  if (!r || typeof r !== 'object' || Array.isArray(r)) return null;
  const msg = clean(r.msg, 600);
  if (!msg.trim()) return null;
  const kind = KINDS.includes(r.kind) ? r.kind : 'other';
  const loc = clean(r.loc, 200);
  const ctx = {};
  if (r.ctx && typeof r.ctx === 'object' && !Array.isArray(r.ctx)) {
    for (const [k, v] of Object.entries(r.ctx).slice(0, 40)) {
      // ni nom ni pseudo : on ecarte toute cle qui y ressemble
      if (PERSONAL.test(k)) continue;
      ctx[cleanKey(k, 32)] = clean(typeof v === 'object' ? JSON.stringify(v) : v, 120);
    }
  }
  const crumbs = (Array.isArray(r.crumbs) ? r.crumbs : []).slice(-MAX_CRUMBS).map((c) => ({
    t: Number.isFinite(c?.t) ? Math.round(c.t) : 0, k: cleanKey(c?.k, 16), m: clean(c?.m, 160),
  }));
  const env = {};
  if (r.env && typeof r.env === 'object' && !Array.isArray(r.env)) {
    for (const [k, v] of Object.entries(r.env).slice(0, 24)) env[cleanKey(k, 24)] = clean(typeof v === 'object' ? JSON.stringify(v) : v, 200);
  }
  // Diagnostic de partie en ligne : borne en taille ; au-dela, on retire d'abord le journal puis les entrees.
  let diag = null;
  if (r.diag && typeof r.diag === 'object' && !Array.isArray(r.diag)) {
    diag = cleanDeep(r.diag);
    for (const drop of ['console', 'log', 'inputs', 'player_events', 'frames']) {
      if (JSON.stringify(diag).length <= MAX_DIAG) break;
      diag[drop] = [];
    }
    if (JSON.stringify(diag).length > MAX_DIAG) diag = { truncated: true, first_div: diag.first_div ?? null };
  }
  const comment = clean(r.comment, 300).replace(/[\r\n]+/g, ' ');
  const num = (v, lo, hi) => (Number.isFinite(v) && v >= lo && v <= hi ? Math.round(v) : null);
  // Empreinte : celle du client si elle ressemble a une empreinte, sinon recalculee (type + message sans chiffres + lieu).
  const fp = /^[0-9a-f]{8,32}$/.test(r.fp) ? r.fp : crypto.createHash('sha1').update(kind + '\0' + msg.replace(/\d+/g, '') + '\0' + loc).digest('hex').slice(0, 16);
  return {
    kind, msg, loc, fp,
    build: cleanKey(r.build, 48), proto: num(r.proto, 0, 1000), platform: cleanKey(r.platform, 16),
    sid: cleanKey(r.sid, 24), ping: num(r.ping, 0, 600000), tick: num(r.tick, 0, 1e9),
    ctx, env, crumbs, diag, comment, ...(cleanRec(r.rec) ? { rec: cleanRec(r.rec) } : {}),
  };
}

export function createReports(opts = {}) {
  const file = opts.file ?? process.env.REPORTS_FILE ?? path.join('data', 'reports.jsonl');
  const maxBytes = opts.maxBytes ?? Number(process.env.REPORTS_MAX_BYTES || 20 * 1024 * 1024);
  const keep = opts.keep ?? 3; // fichiers de rotation conserves : reports.jsonl.1 .. .3
  const adminKey = () => opts.adminKey ?? process.env.ADMIN_KEY ?? process.env.VERDICT_KEY ?? '';
  const perIpPerMin = opts.perIpPerMin ?? 12;
  const perSidPer10Min = opts.perSidPer10Min ?? 20;
  const sproto = opts.protocol ?? null; // PROTOCOL_VERSION du serveur (HTTP, pas lockstep)
  const sidRate = new Map(); // sid -> [debut de fenetre, compte] (memoire seulement)
  const allowedSid = (sid) => {
    if (!sid) return true;
    const now = Date.now();
    let w = sidRate.get(sid);
    if (!w || now - w[0] > 600000) { w = [now, 0]; sidRate.set(sid, w); }
    if (sidRate.size > 5000) for (const [k, v] of sidRate) if (now - v[0] > 600000) sidRate.delete(k);
    return ++w[1] <= perSidPer10Min;
  };
  const globalPerMin = opts.globalPerMin ?? 300;
  const rate = new Map(); // ip -> [debut de fenetre, compte] (memoire seulement)
  let gWin = [0, 0];

  function allowed(ip) {
    const now = Date.now();
    if (now - gWin[0] > 60000) gWin = [now, 0];
    if (++gWin[1] > globalPerMin) return false;
    let w = rate.get(ip);
    if (!w || now - w[0] > 60000) { w = [now, 0]; rate.set(ip, w); }
    if (rate.size > 5000) for (const [k, v] of rate) if (now - v[0] > 60000) rate.delete(k);
    return ++w[1] <= perIpPerMin;
  }

  function rotate() {
    try {
      if (fs.statSync(file).size < maxBytes) return;
    } catch { return; }
    try {
      for (let i = keep; i >= 1; i--) {
        const from = i === 1 ? file : `${file}.${i - 1}`;
        if (!fs.existsSync(from)) continue;
        if (i === keep && fs.existsSync(`${file}.${i}`)) fs.rmSync(`${file}.${i}`);
        fs.renameSync(from, `${file}.${i}`);
      }
    } catch { /* disque en lecture seule */ }
  }

  function store(rep) {
    try {
      fs.mkdirSync(path.dirname(file), { recursive: true });
      rotate();
      fs.appendFileSync(file, JSON.stringify({ at: new Date().toISOString(), ...rep, ...(rep.sproto === undefined && sproto !== null ? { sproto } : {}) }) + '\n');
      return true;
    } catch { return false; }
  }

  const cors = { 'access-control-allow-origin': '*', 'access-control-allow-methods': 'POST, GET, OPTIONS', 'access-control-allow-headers': 'content-type' };

  // POST /report
  function handlePost(req, res) {
    if (req.method === 'OPTIONS') { res.writeHead(204, cors); res.end(); return; }
    if (req.method !== 'POST') { res.writeHead(405, cors); res.end(); return; }
    const len = Number(req.headers['content-length'] || 0);
    if (len > MAX_BODY) { res.writeHead(413, cors); res.end('trop gros'); req.resume(); return; }
    const ip = req.socket.remoteAddress || '?';
    if (!allowed(ip)) { res.writeHead(429, cors); res.end('trop vite'); req.resume(); return; }
    let body = ''; let over = false;
    req.setEncoding('utf8');
    req.on('data', (d) => { body += d; if (body.length > MAX_BODY) { over = true; body = ''; req.destroy(); } });
    req.on('error', () => {});
    req.on('end', () => {
      if (over) return;
      let rep = null;
      try { rep = cleanReport(JSON.parse(body)); } catch { /* ignore */ }
      if (!rep) { res.writeHead(400, cors); res.end('rapport invalide'); return; }
      if (!allowedSid(rep.sid)) { res.writeHead(429, cors); res.end('trop vite (session)'); return; }
      const ok = store(rep);
      res.writeHead(ok ? 204 : 503, cors); res.end();
    });
  }

  function readAll() {
    const out = [];
    for (let i = keep; i >= 0; i--) {
      const f = i === 0 ? file : `${file}.${i}`;
      let txt = '';
      try { txt = fs.readFileSync(f, 'utf8'); } catch { continue; }
      for (const line of txt.split('\n')) {
        if (!line) continue;
        try { out.push(JSON.parse(line)); } catch { /* ligne tronquee */ }
      }
    }
    return out;
  }

  // Regroupe par empreinte : compteur, premiere/derniere occurrence, versions, modes, plateformes, derniers exemples.
  // `kind`, `mode`, `build`, `since` filtrent avant le regroupement.
  function group(items, q = {}) {
    const since = q.since ? Date.parse(q.since) : 0;
    const map = new Map();
    for (const r of items) {
      if (q.kind && r.kind !== q.kind) continue;
      if (q.mode && (r.ctx?.mode || '') !== q.mode) continue;
      if (q.build && r.build !== q.build) continue;
      if (since && Date.parse(r.at) < since) continue;
      let g = map.get(r.fp);
      if (!g) { g = { fp: r.fp, kind: r.kind, msg: r.msg, loc: r.loc, count: 0, first: r.at, last: r.at, builds: new Set(), platforms: new Set(), modes: new Set(), sessions: new Set(), samples: [] }; map.set(r.fp, g); }
      g.count++; g.last = r.at; if (r.at < g.first) g.first = r.at;
      if (r.build) g.builds.add(r.build);
      if (r.platform) g.platforms.add(r.platform);
      if (r.ctx?.mode) g.modes.add(r.ctx.mode);
      if (r.sid) g.sessions.add(r.sid);
      g.samples.push({ id: idOf(r), rec: recSummary(r.rec), at: r.at, build: r.build, platform: r.platform, sid: r.sid, sproto: r.sproto, proto: r.proto, comment: r.comment, ctx: r.ctx, env: r.env, ping: r.ping, tick: r.tick, crumbs: r.crumbs, diag: r.diag });
      if (g.samples.length > 3) g.samples.shift();
    }
    const limit = Math.max(1, Math.min(500, Number(q.limit) || 100));
    return [...map.values()].sort((a, b) => (a.last < b.last ? 1 : -1)).slice(0, limit).map((g) => ({
      // Le diagnostic complet (images, entrees, journal) n'est garde que pour l'echantillon le plus recent (taille de la reponse).
      ...g, builds: [...g.builds], platforms: [...g.platforms], modes: [...g.modes], sessions: g.sessions.size, samples: g.samples.reverse().map((s, i) => (i === 0 ? s : { ...s, diag: s.diag ? { ...s.diag, frames: undefined, inputs: undefined, log: undefined, console: undefined } : undefined })),
    }));
  }

  // Divergence d'un echantillon : image de la 1re divergence, place locale, mode, versions, joueurs, gels (une ligne par rapport).
  function divergence(g, s) {
    const d = s.diag; const fd = d?.first_div;
    if (!fd || fd.frame === undefined) return null;
    const hs = Object.entries(fd.hashes || {}).map(([k, v]) => `p${k}=${String(v).slice(0, 8)}`).join(' ');
    const odd = Object.entries(fd.hashes || {}).filter(([k, v]) => v !== fd.hashes[Object.keys(fd.hashes)[0]]).map(([k]) => `p${k}`).join(',') || '?';
    const ev = (d.player_events || []).slice(-4).map((e) => `${e[1]}:${e[2]}`).join(',') || '-';
    const proto = s.proto !== undefined && s.proto !== null ? `client p${s.proto} / serveur p${s.sproto ?? d.server_proto ?? '?'}` : 'proto ?';
    return `DIVERGENCE | image ${fd.frame} | place locale ${fd.slot ?? d.me ?? '?'} | divergent(s) ${odd} | mode ${s.ctx?.mode || '?'} | ${proto} | ${d.div_count ?? 1}x | hashs ${hs} | derniers join/leave ${ev}`;
  }

  // Resume lisible (pour tools/fetch_reports.sh) : un bloc par groupe, avec fichier:ligne du panic et dernier fil d'Ariane.
  function markdown(groups, total) {
    const L = [`# Rapports d'erreurs AI Fighter 2`, '', `Genere le ${new Date().toISOString()} - ${total} rapports, ${groups.length} groupes (du plus recent au plus ancien).`, ''];
    if (!groups.length) L.push('Aucun rapport.');
    // Resume des divergences en tete : les plus precoces (image la plus basse) d'abord.
    const divs = groups.flatMap((g) => g.samples.map((s) => ({ g, s, line: divergence(g, s), f: s.diag?.first_div?.frame ?? Infinity }))).filter((x) => x.line).sort((a, b) => a.f - b.f);
    if (divs.length) { L.push('## Divergences (desynchro de simulation)', ''); divs.forEach((x) => L.push(`- ${x.line} | ${x.s.at}`)); L.push(''); }
    groups.forEach((g, i) => {
      L.push(`## ${i + 1}. [${g.kind}] ${g.msg.split('\n')[0].slice(0, 160)}`, '');
      L.push(`- empreinte : \`${g.fp}\` ; ${g.count} fois, ${g.sessions} session(s)`);
      if (g.loc) L.push(`- **lieu : \`${g.loc}\`**`);
      L.push(`- premiere : ${g.first} ; derniere : ${g.last}`);
      L.push(`- versions : ${g.builds.join(', ') || '?'} ; plateformes : ${g.platforms.join(', ') || '?'} ; modes : ${g.modes.join(', ') || '?'}`);
      const s = g.samples[0];
      if (s) {
        L.push(`- contexte : ${Object.entries(s.ctx || {}).map(([k, v]) => `${k}=${v}`).join(' ; ') || '-'}`);
        const e = Object.entries(s.env || {}).map(([k, v]) => `${k}=${v}`).join(' ; ');
        if (e) L.push(`- environnement : ${e}`);
        if (s.comment) L.push(`- commentaire du joueur : ${s.comment}`);
        L.push(`- rapport \`${s.id}\`${s.rec ? ` : enregistrement rejouable (${s.rec.epochs.map((e) => `${e.ticks} images${e.trunc ? ' TRONQUE' : ''}, ${Math.round(e.bytes / 1024)} Ko`).join(' + ')}) -> tools/replay_report.sh ${s.id}` : ' : sans enregistrement'}`);
        const d = s.diag;
        if (d) {
          L.push(`- versions : jeu ${d.game || '?'} ; build ${d.build || '?'} ; protocole client ${s.proto ?? d.proto ?? '?'} / serveur ${s.sproto ?? d.server_proto ?? '?'}${s.env?.sw ? ` ; service worker ${s.env.sw}` : ''}`);
          const dv = divergence(g, s); if (dv) L.push(`- ${dv}`);
          if (d.players?.length) L.push(`- joueurs (place : en ligne/IA) : ${d.players.map((p) => `${p.slot}:${p.online ? 'en ligne' : 'hors'}${p.ai ? '/IA' : ''}`).join(' ; ')} ; moi = ${d.me ?? '?'}`);
          if (d.player_events?.length) L.push(`- arrivees/departs : ${d.player_events.slice(-10).map((e) => `${e[1]}:${e[2]}@${e[0]}`).join(' ')}`);
          const lk = d.link;
          if (lk) { const last = (lk.recent || []).slice(-1)[0]; const rt = (lk.recent || []).map((x) => x[1]); L.push(`- liaison : dernier ${last ? `${last[1]} ms (gigue ${last[2]})` : '?'} ; max ${rt.length ? Math.max(...rt) : '?'} ms ; gels ${lk.stall_count ?? 0}${lk.stalls?.length ? ` (derniers : ${lk.stalls.slice(-4).map((x) => `image ${x[1]} x${x[2]}`).join(', ')})` : ''}`); }
          if (d.rl) L.push(`- roguelite : ${JSON.stringify(d.rl).slice(0, 400)}`);
          if (d.frames?.length) L.push(`- images (hash local ; ${d.frames.length} gardees) : ${d.frames.slice(-6).map((f) => `${f[0]}=${String(f[1]).slice(0, 8)}${f[2] ? `[${Object.entries(f[2]).map(([k, v]) => `p${k}:${String(v).slice(0, 8)}`).join(' ')}]` : ''}`).join(' ')}`);
          if (d.inputs?.length) L.push(`- dernieres entrees (image: bits par place, ffffffff = IA) : ${d.inputs.slice(-3).map((x) => `${x[0]}:[${(x[1] || []).map((b) => Number(b).toString(16)).join(',')}]`).join(' ')}`);
          if (d.log?.length) { L.push(`- journal (${d.log.length} lignes, 8 dernieres) :`); d.log.slice(-8).forEach((x) => L.push(`    - ${x[1]}`)); }
        }
        if (s.crumbs?.length) { L.push('- fil d\'Ariane (derniers evenements) :'); s.crumbs.slice(-12).forEach((c) => L.push(`    - ${c.k}: ${c.m}`)); }
      }
      L.push('');
    });
    return L.join('\n');
  }

  function authorized(key) {
    const k = adminKey();
    if (!k || typeof key !== 'string') return false;
    const a = Buffer.from(key), b = Buffer.from(k);
    return a.length === b.length && crypto.timingSafeEqual(a, b);
  }

  // GET /reports?key=...&kind=&mode=&build=&since=&limit=&format=json|md
  function handleGet(req, res, url) {
    if (req.method === 'OPTIONS') { res.writeHead(204, cors); res.end(); return; }
    if (!adminKey()) { res.writeHead(403, cors); res.end('rapports desactives : definir ADMIN_KEY'); return; }
    if (!authorized(url.searchParams.get('key'))) { res.writeHead(401, cors); res.end('cle invalide'); return; }
    const q = Object.fromEntries(url.searchParams);
    const items = readAll();
    // Enregistrement de rejeu d'UN rapport : `format=rec&id=<id>` (ou `id=latest`) -> JSON {v, epochs} = contenu d'un fichier .afhrec.
    if (q.format === 'rec') {
      const withRec = items.filter((r) => r.rec && (!q.kind || r.kind === q.kind));
      const hit = q.id === 'latest' || !q.id ? withRec[withRec.length - 1] : withRec.find((r) => idOf(r) === q.id);
      if (!hit) { res.writeHead(404, cors); res.end('aucun enregistrement pour ce rapport'); return; }
      res.writeHead(200, { ...cors, 'content-type': 'application/json', 'cache-control': 'no-store', 'x-report-id': idOf(hit) });
      res.end(JSON.stringify(hit.rec));
      return;
    }
    const groups = group(items, q);
    if (q.format === 'md') { res.writeHead(200, { ...cors, 'content-type': 'text/markdown; charset=utf-8', 'cache-control': 'no-store' }); res.end(markdown(groups, items.length)); return; }
    res.writeHead(200, { ...cors, 'content-type': 'application/json', 'cache-control': 'no-store' });
    res.end(JSON.stringify({ total: items.length, groups }));
  }

  return { handlePost, handleGet, store, readAll, group, markdown, file };
}
