// Rapports d'erreurs des joueurs : POST /report (valide, borne, ecrit en JSON-lines) et GET /reports (regroupes par empreinte).
// Aucune donnee personnelle n'est stockee : pas d'adresse IP (seulement en memoire pour limiter le debit), pas de pseudo.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';

export const KINDS = ['panic', 'js', 'net', 'asset', 'audio', 'webgl', 'wasm', 'ws', 'other'];
const MAX_BODY = 24 * 1024;
const MAX_CRUMBS = 50;

const clean = (v, n) => String(v ?? '').replace(/[\u0000-\u0008\u000b-\u001f\u007f-\u009f\u200b-\u200f\u2028-\u202e]/g, '').slice(0, n);
const cleanKey = (v, n) => clean(v, n).replace(/[^\w.:@+\-/ ]/g, '_');

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
      if (/^(name|nom|pseudo|ip|email|user)/i.test(k)) continue;
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
  const num = (v, lo, hi) => (Number.isFinite(v) && v >= lo && v <= hi ? Math.round(v) : null);
  // Empreinte : celle du client si elle ressemble a une empreinte, sinon recalculee (type + message sans chiffres + lieu).
  const fp = /^[0-9a-f]{8,32}$/.test(r.fp) ? r.fp : crypto.createHash('sha1').update(kind + '\0' + msg.replace(/\d+/g, '') + '\0' + loc).digest('hex').slice(0, 16);
  return {
    kind, msg, loc, fp,
    build: cleanKey(r.build, 48), proto: num(r.proto, 0, 1000), platform: cleanKey(r.platform, 16),
    sid: cleanKey(r.sid, 24), ping: num(r.ping, 0, 600000), tick: num(r.tick, 0, 1e9),
    ctx, env, crumbs,
  };
}

export function createReports(opts = {}) {
  const file = opts.file ?? process.env.REPORTS_FILE ?? path.join('data', 'reports.jsonl');
  const maxBytes = opts.maxBytes ?? Number(process.env.REPORTS_MAX_BYTES || 5 * 1024 * 1024);
  const keep = opts.keep ?? 3; // fichiers de rotation conserves : reports.jsonl.1 .. .3
  const adminKey = () => opts.adminKey ?? process.env.ADMIN_KEY ?? process.env.VERDICT_KEY ?? '';
  const perIpPerMin = opts.perIpPerMin ?? 12;
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
      fs.appendFileSync(file, JSON.stringify({ at: new Date().toISOString(), ...rep }) + '\n');
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
      g.samples.push({ at: r.at, build: r.build, platform: r.platform, ctx: r.ctx, env: r.env, ping: r.ping, tick: r.tick, crumbs: r.crumbs });
      if (g.samples.length > 3) g.samples.shift();
    }
    const limit = Math.max(1, Math.min(500, Number(q.limit) || 100));
    return [...map.values()].sort((a, b) => (a.last < b.last ? 1 : -1)).slice(0, limit).map((g) => ({
      ...g, builds: [...g.builds], platforms: [...g.platforms], modes: [...g.modes], sessions: g.sessions.size, samples: g.samples.reverse(),
    }));
  }

  // Resume lisible (pour tools/fetch_reports.sh) : un bloc par groupe, avec fichier:ligne du panic et dernier fil d'Ariane.
  function markdown(groups, total) {
    const L = [`# Rapports d'erreurs AI Fighter 2`, '', `Genere le ${new Date().toISOString()} - ${total} rapports, ${groups.length} groupes (du plus recent au plus ancien).`, ''];
    if (!groups.length) L.push('Aucun rapport.');
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
    const groups = group(items, q);
    if (q.format === 'md') { res.writeHead(200, { ...cors, 'content-type': 'text/markdown; charset=utf-8', 'cache-control': 'no-store' }); res.end(markdown(groups, items.length)); return; }
    res.writeHead(200, { ...cors, 'content-type': 'application/json', 'cache-control': 'no-store' });
    res.end(JSON.stringify({ total: items.length, groups }));
  }

  return { handlePost, handleGet, store, readAll, group, markdown, file };
}
