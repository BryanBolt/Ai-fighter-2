// Verdicts des pages de propositions (Valider / A refaire / commentaire), stockes cote serveur pour que les agents puissent les lire.
// Auth legere sans compte : une cle secrete VERDICT_KEY (variable d'environnement). Sans cle, ecriture ET lecture sont desactivees (503)
// et les pages restent sur leur localStorage. Aucune donnee personnelle : page, carte, verdict, commentaire libre, version, date.
// Persistance : fichier JSON (VERDICTS_FILE, ecriture atomique tmp + rename) ; sans fichier : memoire seule.
import fs from 'node:fs';
import crypto from 'node:crypto';

export const VLIMITS = { bodyMax: 8192, pageMax: 64, carteMax: 120, versionMax: 64, commentMax: 2000, entriesMax: 5000, ipWindowMs: 60000, ipWindowMax: 240, badKeyMax: 20 };
export const VERDICTS = ['ok', 'refaire', 'discuter', ''];
const PAGE_RE = /^[A-Za-z0-9_.-]+$/;
const CORS = { 'access-control-allow-origin': '*', 'access-control-allow-methods': 'GET, PUT, OPTIONS', 'access-control-allow-headers': 'content-type, x-verdict-key, authorization', 'access-control-max-age': '600' };
const clean = (s, max) => String(s ?? '').replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f‪-‮]/g, '').slice(0, max);

export function createVerdicts({ file = null, key = null, now = () => Date.now() } = {}) {
  const secret = key ? String(key) : null;
  let db = { entries: {} };
  if (file) { try { const d = JSON.parse(fs.readFileSync(file, 'utf8')); if (d && typeof d.entries === 'object') db = d; } catch { /* absent ou illisible */ } }
  let count = Object.keys(db.entries).length;
  let timer = null;
  function flush() {
    if (timer) { clearTimeout(timer); timer = null; }
    if (!file) return;
    try { fs.writeFileSync(file + '.tmp', JSON.stringify(db)); fs.renameSync(file + '.tmp', file); } catch { /* disque en lecture seule */ }
  }
  const later = () => { if (file && !timer) { timer = setTimeout(flush, 300); timer.unref?.(); } };

  const authOk = (req) => {
    if (!secret) return false;
    const h = String(req.headers['x-verdict-key'] || String(req.headers.authorization || '').replace(/^Bearer\s+/i, ''));
    const a = crypto.createHash('sha256').update(h).digest(), b = crypto.createHash('sha256').update(secret).digest();
    return crypto.timingSafeEqual(a, b);
  };

  // Debit par adresse (toutes requetes) + freinage des mauvaises cles.
  const hits = new Map(), bad = new Map();
  function limited(ip) {
    const t = now(); const w = hits.get(ip);
    if (!w || t - w.t0 > VLIMITS.ipWindowMs) { if (hits.size > 5000) hits.clear(); hits.set(ip, { t0: t, n: 1 }); return false; }
    return ++w.n > VLIMITS.ipWindowMax;
  }

  function put(body) {
    if (!body || typeof body !== 'object') return { code: 400, error: 'corps invalide' };
    const page = String(body.page ?? ''), carte = clean(body.carte, VLIMITS.carteMax).trim();
    if (!page || page.length > VLIMITS.pageMax || !PAGE_RE.test(page)) return { code: 400, error: 'page invalide' };
    if (!carte) return { code: 400, error: 'carte invalide' };
    const verdict = body.verdict == null ? '' : String(body.verdict);
    if (!VERDICTS.includes(verdict)) return { code: 400, error: 'verdict invalide' };
    const commentaire = clean(body.commentaire, VLIMITS.commentMax).trim();
    const version = clean(body.version, VLIMITS.versionMax).trim();
    const k = page + '\n' + carte;
    const at = now();
    const cat = Number.isFinite(body.at) ? Math.min(body.at, at) : at; // file hors ligne : date client, jamais dans le futur
    const prev = db.entries[k];
    if (prev && cat < prev.at) return { code: 200, ok: true, stale: true };
    if (!verdict && !commentaire) { if (prev) { delete db.entries[k]; count--; } later(); return { code: 200, ok: true, deleted: true }; }
    if (!prev && count >= VLIMITS.entriesMax) return { code: 413, error: 'trop de verdicts' };
    if (!prev) count++;
    db.entries[k] = { page, carte, verdict, commentaire, version, at: cat };
    later();
    return { code: 200, ok: true };
  }

  function list(page) {
    return Object.values(db.entries).filter((e) => !page || e.page === page).sort((a, b) => (a.page < b.page ? -1 : a.page > b.page ? 1 : a.at - b.at));
  }

  function handle(req, res, url, ip) {
    const p = url.pathname;
    if (p !== '/api/verdict' && p !== '/api/verdicts' && p !== '/api/verdict-status') return false;
    const send = (code, obj) => { res.writeHead(code, { 'content-type': 'application/json', 'cache-control': 'no-store', ...CORS }); res.end(JSON.stringify(obj)); };
    if (req.method === 'OPTIONS') { res.writeHead(204, CORS); res.end(); return true; }
    if (p === '/api/verdict-status') { send(200, { enabled: !!secret }); return true; }
    if (limited(ip)) { send(429, { error: 'trop de requetes' }); return true; }
    if (!secret) { send(503, { error: 'verdicts desactives (VERDICT_KEY absente)' }); return true; }
    if ((bad.get(ip) || 0) >= VLIMITS.badKeyMax) { send(429, { error: 'trop d\'essais' }); return true; }
    if (!authOk(req)) { bad.set(ip, (bad.get(ip) || 0) + 1); if (bad.size > 2000) bad.clear(); send(401, { error: 'cle invalide' }); return true; }
    bad.delete(ip);
    if (p === '/api/verdicts' && req.method === 'GET') {
      const page = url.searchParams.get('page');
      send(200, { verdicts: list(page && PAGE_RE.test(page) ? page : null), persisted: !!file }); return true;
    }
    if (p === '/api/verdict' && req.method === 'PUT') {
      let size = 0; const chunks = []; let dead = false;
      req.on('data', (c) => { size += c.length; if (size > VLIMITS.bodyMax) { if (!dead) { dead = true; send(413, { error: 'corps trop gros' }); req.destroy(); } return; } chunks.push(c); });
      req.on('end', () => {
        if (dead) return;
        let body; try { body = JSON.parse(Buffer.concat(chunks).toString('utf8')); } catch { send(400, { error: 'json invalide' }); return; }
        const { code, ...rest } = put(body); send(code, rest);
      });
      req.on('error', () => {});
      return true;
    }
    send(405, { error: 'methode non permise' }); return true;
  }
  return { handle, put, list, flush, enabled: !!secret };
}
