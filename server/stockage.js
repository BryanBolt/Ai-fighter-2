// Stockage des donnees recues (signalements, chiffres de partie, incidents, enregistrements, notes) derriere une interface unique :
//   add(coll, data, { blob, ext, at })  -> id          (synchrone : memoire d'abord, disque et base ensuite, jamais d'exception)
//   list(coll, { since, limit, where }) -> [meta]      (les plus anciens d'abord ; `where` = fonction ou { cle: valeur })
//   get(coll, id) / blob(coll, id) / count(coll) / purge() / stats() / init() / close()
// Trois niveaux, tous bornes :
//   - MEMOIRE : les `keep` derniers enregistrements de chaque collection (c'est ce que lisent les points d'acces GET) ;
//   - DISQUE (sauf noDisk) : un .json (+ un .<ext> pour la piece jointe) par enregistrement, rotation par nombre et par octets ;
//   - POSTGRESQL si DATABASE_URL est definie (dependance `pg`, TLS, requetes parametrees, pool de 3, delais courts) : ecriture en
//     arriere-plan, rechargement au demarrage. Base injoignable ou erreur = on reste en memoire (+ disque), on reessaie 60 s plus tard ;
//     aucune erreur ne remonte jamais au relais.
// Purge automatique : par age (maxAgeMs) et par taille (maxBytes par collection) ; le total reste sous ~300 Mo (voir DEFAULTS).
// Rien de personnel n'est stocke ici : les modules appelants ecartent pseudo, adresse et identifiants avant d'appeler add().
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';

const MB = 1024 * 1024;
// Plafonds par defaut (octets) : somme 270 Mo < 300 Mo. `keep` = enregistrements gardes en memoire ET conserves au plus.
export const DEFAULTS = {
  signalements: { keep: 60, maxFiles: 200, maxBytes: 40 * MB, maxAgeMs: 90 * 86400e3 },
  partie: { keep: 30000, maxFiles: 30000, maxBytes: 60 * MB, maxAgeMs: 180 * 86400e3 },
  incident: { keep: 10000, maxFiles: 10000, maxBytes: 20 * MB, maxAgeMs: 90 * 86400e3 },
  note: { keep: 10000, maxFiles: 10000, maxBytes: 5 * MB, maxAgeMs: 180 * 86400e3 },
  rec: { keep: 40, maxFiles: 40, maxBytes: 120 * MB, maxAgeMs: 30 * 86400e3 },
};
export const TOTAL_MAX_BYTES = 300 * MB;
const PG_RETRY_MS = 60_000;
const PG_QUEUE_MAX = 500;
const ID_RE = /^[0-9]{13}-[0-9a-f]{8}$/;

export const sizeOf = (meta, blob) => Buffer.byteLength(JSON.stringify(meta)) + (blob ? blob.length : 0);

// Option TLS de pg : verification du certificat ACTIVEE par defaut. PGSSL_INSECURE=1 la desactive (DECONSEILLE : ouvre
// la porte a l'interception) ; AFH_PG_NOSSL=1 coupe TLS (base locale de developpement uniquement).
export function sslOption(env = process.env) {
  if (env.AFH_PG_NOSSL === '1') return false;
  return { rejectUnauthorized: env.PGSSL_INSECURE !== '1' };
}

// Client pg par defaut (import dynamique : sans le paquet, ou sans DATABASE_URL, on reste en memoire).
async function defaultPgFactory(url, env) {
  const { default: pg } = await import('pg');
  const ssl = sslOption(env);
  const pool = new pg.Pool({ connectionString: url, ssl, max: 3, connectionTimeoutMillis: 3000, idleTimeoutMillis: 10_000, statement_timeout: 5000, query_timeout: 5000 });
  pool.on('error', () => { /* connexion perdue : la prochaine requete le dira */ });
  return pool;
}

export function createStockage(opts = {}) {
  const env = opts.env ?? process.env;
  const root = opts.dir ?? env.STOCKAGE_DIR ?? 'donnees';
  const noDisk = !!opts.noDisk;
  const databaseUrl = opts.databaseUrl === undefined ? env.DATABASE_URL || '' : opts.databaseUrl;
  const pgFactory = opts.pgFactory ?? (databaseUrl ? () => defaultPgFactory(databaseUrl, env) : null);
  const now = opts.now ?? Date.now;
  // Nom de la table PostgreSQL (defaut `afh_records` ; opts.table sert aux essais sur une vraie base).
  const T = opts.table ?? 'afh_records';
  if (!/^[a-z0-9_]{1,50}$/.test(T)) throw new Error('nom de table invalide');
  const conf = {};
  const colls = new Map(); // nom -> { name, cfg, dir, mem: Map(id -> { meta, img, ext }), bytes }
  const st = { added: 0, pgOk: 0, pgErr: 0, lastErr: '', dropped: 0, pgMode: pgFactory ? 'pending' : 'none' };

  function cfgOf(name) {
    if (!conf[name]) conf[name] = { ...(DEFAULTS[name] || { keep: 2000, maxFiles: 2000, maxBytes: 10 * MB, maxAgeMs: 90 * 86400e3 }), ...(opts.collections?.[name] || {}) };
    return conf[name];
  }
  function coll(name) {
    let c = colls.get(name);
    if (!c) {
      const cfg = cfgOf(name);
      c = { name, cfg, dir: cfg.dir ?? path.join(root, name), mem: new Map(), bytes: 0, noDisk: cfg.noDisk ?? noDisk };
      colls.set(name, c);
      if (!c.noDisk) loadDisk(c);
    }
    return c;
  }
  const entryBytes = (e) => sizeOf(e.meta, e.img);

  // ---------------------------------------------------------------- disque
  function loadDisk(c) {
    try {
      const ids = fs.readdirSync(c.dir).filter((n) => /^[0-9]{13}-[0-9a-f]{8}\.json$/.test(n)).map((n) => n.slice(0, -5)).sort().slice(-c.cfg.keep);
      for (const id of ids) {
        try {
          const meta = JSON.parse(fs.readFileSync(path.join(c.dir, id + '.json'), 'utf8'));
          const ext = meta.img_ext || meta.blob_ext || '';
          let img = null;
          if (ext) { try { img = fs.readFileSync(path.join(c.dir, id + '.' + ext)); } catch { /* piece jointe perdue */ } }
          const e = { meta, img, ext: img ? ext : '' };
          c.mem.set(id, e); c.bytes += entryBytes(e);
        } catch { /* fichier abime */ }
      }
    } catch { /* pas de dossier */ }
  }
  function rotateDisk(c) {
    if (c.noDisk) return;
    try {
      const names = fs.readdirSync(c.dir).filter((n) => /^[0-9]{13}-[0-9a-f]{8}\./.test(n));
      const ids = [...new Set(names.map((n) => n.split('.')[0]))].sort();
      const size = (id) => names.filter((n) => n.startsWith(id + '.')).reduce((a, n) => { try { return a + fs.statSync(path.join(c.dir, n)).size; } catch { return a; } }, 0);
      let total = ids.reduce((a, id) => a + size(id), 0);
      const cut = now() - c.cfg.maxAgeMs;
      while (ids.length > 1 && (ids.length > c.cfg.maxFiles || total > c.cfg.maxBytes || Number(ids[0].slice(0, 13)) < cut)) {
        const id = ids.shift(); total -= size(id);
        for (const n of names.filter((x) => x.startsWith(id + '.'))) { try { fs.unlinkSync(path.join(c.dir, n)); } catch { /* deja parti */ } }
      }
    } catch { /* dossier absent */ }
  }

  // ---------------------------------------------------------------- memoire
  function trim(c) {
    const cut = now() - c.cfg.maxAgeMs;
    for (const [id, e] of c.mem) {
      const over = c.mem.size > c.cfg.keep || (c.bytes > c.cfg.maxBytes && c.mem.size > 1) || e.meta.at < cut;
      if (!over) break;
      c.mem.delete(id); c.bytes -= entryBytes(e);
    }
  }

  // ---------------------------------------------------------------- PostgreSQL (arriere-plan)
  let pool = null, pgReady = false, pgRetryAt = 0, draining = false, initing = null;
  const queue = [];
  const tableSql = `CREATE TABLE IF NOT EXISTS ${T} (id text PRIMARY KEY, coll text NOT NULL, at bigint NOT NULL, data jsonb NOT NULL, blob bytea, ext text NOT NULL DEFAULT '', bytes integer NOT NULL)`;
  const idxSql = `CREATE INDEX IF NOT EXISTS ${T}_coll_at ON ${T} (coll, at DESC)`;
  // On ne garde que le code d'erreur (jamais le message : il pourrait citer la chaine de connexion).
  const pgFail = (err) => { st.lastErr = String(err?.code || (err?.message === 'delai' ? 'delai' : 'erreur')).slice(0, 40); st.pgErr++; pgReady = false; st.pgMode = 'degraded'; pgRetryAt = now() + PG_RETRY_MS; };
  async function pgQuery(text, params = []) {
    if (!pool) throw new Error('pas de pool');
    return Promise.race([pool.query(text, params), new Promise((_, rej) => setTimeout(() => rej(new Error('delai')), 6000).unref?.())]);
  }
  async function pgInit() {
    if (!pgFactory) return;
    try {
      if (!pool) pool = await pgFactory();
      await pgQuery(tableSql); await pgQuery(idxSql);
      for (const name of new Set([...Object.keys(DEFAULTS), ...Object.keys(opts.collections || {})])) {
        const c = coll(name);
        const r = await pgQuery(`SELECT id, at, data, ext, bytes FROM ${T} WHERE coll = $1 ORDER BY at DESC, id DESC LIMIT $2`, [name, c.cfg.keep]);
        for (const row of [...r.rows].reverse()) {
          if (c.mem.has(row.id)) continue;
          const meta = { id: row.id, at: Number(row.at), ...(typeof row.data === 'string' ? JSON.parse(row.data) : row.data) };
          // La piece jointe n'est relue qu'a la demande (blob()) : la memoire ne porte que les metadonnees.
          c.mem.set(row.id, { meta, img: null, ext: row.ext || '', fromPg: true }); c.bytes += Number(row.bytes) || 0;
        }
        c.mem = new Map([...c.mem].sort((a, b) => (a[0] < b[0] ? -1 : 1)));
        trim(c);
      }
      pgReady = true; st.pgMode = 'ok';
      await purgePg();
    } catch (err) { pgFail(err); }
  }
  async function purgePg() {
    if (!pgReady) return;
    try {
      for (const name of new Set([...Object.keys(DEFAULTS), ...colls.keys()])) {
        const cfg = cfgOf(name);
        await pgQuery(`DELETE FROM ${T} WHERE coll = $1 AND at < $2`, [name, now() - cfg.maxAgeMs]);
        await pgQuery(`DELETE FROM ${T} WHERE coll = $1 AND id IN (SELECT id FROM ${T} WHERE coll = $1 ORDER BY at DESC, id DESC OFFSET $2)`, [name, cfg.maxFiles]);
        await pgQuery(`DELETE FROM ${T} WHERE coll = $1 AND id IN (SELECT id FROM (SELECT id, SUM(bytes) OVER (ORDER BY at DESC, id DESC) AS run FROM ${T} WHERE coll = $1) t WHERE run > $2)`, [name, cfg.maxBytes]);
      }
    } catch (err) { pgFail(err); }
  }
  async function drain() {
    if (draining) return;
    draining = true;
    try {
      while (queue.length) {
        if (!pgReady) {
          if (!pgFactory || now() < pgRetryAt) break;
          await pgInit();
          if (!pgReady) break;
        }
        const r = queue[0];
        try {
          await pgQuery(`INSERT INTO ${T} (id, coll, at, data, blob, ext, bytes) VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7) ON CONFLICT (id) DO NOTHING`,
            [r.id, r.coll, r.at, JSON.stringify(r.data), r.blob || null, r.ext || '', r.bytes]);
          queue.shift(); st.pgOk++;
        } catch (err) { pgFail(err); break; }
      }
    } catch { /* jamais d'exception vers le relais */ }
    draining = false;
  }
  const kick = () => { if (pgFactory) drain().catch(() => {}); };

  // ---------------------------------------------------------------- interface
  function add(name, data, o = {}) {
    try {
      const c = coll(name);
      const at = o.at ?? now();
      const id = String(at).padStart(13, '0') + '-' + crypto.randomBytes(4).toString('hex');
      const blob = o.blob || null, ext = blob ? String(o.ext || 'bin').replace(/[^a-z0-9]/gi, '').slice(0, 8) : '';
      const meta = { id, at, ...data };
      const e = { meta, img: blob, ext };
      c.mem.set(id, e); c.bytes += entryBytes(e);
      trim(c);
      if (!c.noDisk) {
        try {
          fs.mkdirSync(c.dir, { recursive: true });
          fs.writeFileSync(path.join(c.dir, id + '.json'), JSON.stringify(meta));
          if (blob) fs.writeFileSync(path.join(c.dir, id + '.' + ext), blob);
          rotateDisk(c);
        } catch { /* disque en lecture seule : la memoire suffit */ }
      }
      if (pgFactory) {
        queue.push({ id, coll: name, at, data, blob, ext, bytes: entryBytes(e) });
        while (queue.length > PG_QUEUE_MAX) { queue.shift(); st.dropped++; }
        kick();
      }
      st.added++;
      return id;
    } catch { return ''; }
  }
  const match = (where) => (typeof where === 'function' ? where : where && typeof where === 'object' ? (m) => Object.entries(where).every(([k, v]) => m[k] === v) : () => true);
  function list(name, o = {}) {
    const c = coll(name), w = match(o.where), since = Number(o.since) || 0;
    const rows = [];
    for (const e of c.mem.values()) { if (e.meta.at > since && w(e.meta)) rows.push(e.meta); }
    return o.limit ? rows.slice(-o.limit) : rows;
  }
  const get = (name, id) => coll(name).mem.get(id)?.meta || null;
  const entry = (name, id) => coll(name).mem.get(id) || null;
  // Piece jointe d'un enregistrement (memoire, sinon relue en base pour ceux recharges au demarrage).
  async function blob(name, id) {
    const e = coll(name).mem.get(id);
    if (!e) return null;
    if (e.img) return e.img;
    if (e.fromPg && pgReady) {
      try { const r = await pgQuery(`SELECT blob FROM ${T} WHERE id = $1 AND coll = $2`, [id, name]); return r.rows[0]?.blob || null; } catch (err) { pgFail(err); }
    }
    return null;
  }
  const count = (name) => coll(name).mem.size;
  // Renvoie la promesse de la purge en base (les appelants du relais l'ignorent ; l'essai sur vraie base l'attend).
  function purge() { for (const c of colls.values()) { trim(c); rotateDisk(c); } return pgReady ? purgePg().catch(() => {}) : Promise.resolve(); }
  const stats = () => ({ ...st, queued: queue.length, collections: Object.fromEntries([...colls].map(([n, c]) => [n, { n: c.mem.size, bytes: c.bytes }])) });
  const init = () => (initing ||= pgFactory ? pgInit() : Promise.resolve());
  // Attend que la file d'ecriture en base soit vidée (essais) : renvoie true si tout est parti.
  async function flush() {
    for (let i = 0; i < 400 && (draining || queue.length); i++) {
      if (!draining) { if (!pgReady && (!pgFactory || now() < pgRetryAt)) break; await drain(); } else await new Promise((r) => setTimeout(r, 25));
    }
    return queue.length === 0;
  }
  async function close() { try { await drain(); } catch { /* rien */ } try { await pool?.end?.(); } catch { /* rien */ } pool = null; pgReady = false; }
  for (const n of Object.keys(DEFAULTS)) coll(n);
  return { add, list, get, entry, blob, count, purge, stats, init, close, flush, coll, ID_RE, get pgReady() { return pgReady; }, get mode() { return pgFactory ? (pgReady ? 'postgres' : 'memoire (base indisponible)') : noDisk ? 'memoire' : 'memoire+disque'; } };
}
