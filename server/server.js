// Relais en ligne d'AI Fighter 2 : salons, parties rapides, relais des entrees (lockstep deterministe),
// journal des entrees (reprise, arrivee en cours de partie), detection de desynchronisation, AFK, expulsion.
// Le serveur ne simule RIEN : chaque client fait tourner le jeu complet.
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import zlib from 'node:zlib';
import { fileURLToPath } from 'node:url';
import { WebSocketServer } from 'ws';
import { createPush } from './push.js';

export const PROTOCOL_VERSION = 10; // 10 : daemon de chaque joueur au salon (champ `daemon` des messages lobby et room) ; 9 : mode ROGUELITE en coop (mode 3 : run d equipe, cartes personnelles, etages) ; 8 : nunchaku (nouvelle arme ramassable, serie de six coups, Pied = coups de pied avec l'arme) et armes au sol du dojo/temple changees ; 7 : entree Saisie (nouveau bit d'entree) + coups renommes (SCHEMA_VERSION 8) ; 6 : table des coups changee (grammaire X/Y des 9 autres heros, SCHEMA_VERSION 7) ; 5 : jusqu'a 8 joueurs, vague/PV de depart (solo -> en ligne) ; 4 : table des coups changee (aeriens, realisme, grammaire X/Y ; tir allie, QA). 3 : parties hebergees (jeu complet : loadout, cmd sequences). 2 : join_any / host. Seuls les clients v8 sont acceptes.
const COMPAT_VERSIONS = [PROTOCOL_VERSION]; // derive de PROTOCOL_VERSION : ne peut plus diverger (v8 etait refuse par erreur, ancienne valeur [7]) ; // protocole 7 : les clients 1 a 6 n'ont pas la meme table de coups (et 1 a 4 ne savent pas lire des salons de 8 places), ils doivent se mettre a jour
const HOSTED_SIZE = 2; // taille par defaut d une partie hebergee (anciens clients) ; le client actuel demande 4 avec `size` (places vides dormantes)
const HOSTED_MIN = 2; // taille minimale demandable
const MAX_CONN_PER_IP = 8; // connexions simultanees par adresse
const MAX_CONNS = 600; // connexions totales
const MAX_ROOMS_PER_IP = 3; // salons ouverts par la meme adresse
const CHAT_GAP_MS = 600; // delai minimal entre deux messages de chat
const MAX_LOADOUTS = 200; // changements d'equipement par place et par partie
const RESYNC_GAP_MS = 2500; // delai minimal entre deux reprises forcees d'un meme joueur
const MAX_BANNED = 40; // adresses bannies par salon
const CMD_MARGIN = 30; // images d'avance d'une commande de jeu (vote, chapitre, dialogue)
const MAX_CMDS = 2000; // commandes par place et par partie (le journal de reprise reste borne)
const ROSTER = 10; // heros du roster en ligne
const STAGES = 12;
const FULL_STAGES = 16; // parties hebergees : tous les stages de la campagne
const MAX_SLOTS = 8;
const REJOIN_MIN = 15; // images d'avance minimales pour un evenement de controle (retour quasi immediat)
const REJOIN_MAX = 590; // plafond (le journal accepte au plus +600 images)
const HOLD_MS = 20 * 60 * 1000; // une place coupee reste reservee 20 min
const ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // sans caracteres ambigus (0/O, 1/I)
const ROOM_TTL_MS = 30 * 60 * 1000; // salon « permanent » (lien de reconnexion) : garde 30 min apres le dernier joueur
const MAX_MATCH_PER_MIN = 12; // demandes de partie (host / join_any / create / quick) par adresse et par minute
const REPORT_GAP_MS = 5000; // delai minimal entre deux signalements d'un meme joueur
const COLORS = 8; // palette de couleurs de joueurs (le client la definit : 8 teintes bien separees, sures pour le daltonisme)
const MAX_PEEK = 8; // salons verifies d'un coup (reprise : « mes dernieres parties »)

export function createServer(opts = {}) {
  const trustProxy = opts.trustProxy ?? process.env.TRUST_PROXY === '1';
  const afkMs = opts.afkMs ?? Number(process.env.AFK_MS || 60000);
  const holdMs = opts.holdMs ?? HOLD_MS;
  const idleMs = opts.idleMs ?? Number(process.env.IDLE_MS || 20 * 60 * 1000); // salon sans aucune action humaine
  const root = opts.root ?? process.env.WEB_ROOT ?? path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'web');
  const rooms = new Map(); // code -> salon
  const banned = new Map(); // code -> Set(ip)
  const ideasRate = new Map();
  const maxRooms = opts.maxRooms ?? Number(process.env.MAX_ROOMS || 60);
  const autoHoldMs = opts.autoHoldMs ?? 10 * 60 * 1000; // place d'un joueur parti reservee pour son retour (le heros s'efface au prochain boss, cote clients)
  const graceMs = opts.graceMs ?? 30000; // partie vide apres une simple coupure : gardee un instant
  const restartMs = opts.restartMs ?? 7000; // ecran de resultats avant la nouvelle partie
  const joinRate = new Map(); // ip -> derniere demande host/join_any
  const roomTtl = opts.roomTtlMs ?? ROOM_TTL_MS;
  const matchLog = new Map(); // ip -> horodatages des demandes de partie (fenetre glissante d'une minute)
  const reports = new Map(); // code -> Map(slot cible -> Set(jetons des signaleurs))
  const push = createPush({ sender: opts.pushSender ?? null, env: opts.pushEnv ?? process.env, roomExists: (c) => rooms.has(c) });

  const rnd = (n) => crypto.randomInt(n);
  const newCode = () => {
    for (;;) {
      let c = '';
      for (let i = 0; i < 5; i++) c += ALPHABET[rnd(ALPHABET.length)];
      if (!rooms.has(c)) return c;
    }
  };
  const newToken = () => crypto.randomBytes(12).toString('hex');
    // Equipement d'un joueur : le relais ne l'interprete pas, il borne seulement la forme et les tailles
  // (le monde de chaque client re-valide les ids et applique les plafonds de build existants).
  const int = (v, lo, hi, d = lo) => (Number.isInteger(v) && v >= lo && v <= hi ? v : d);
  const intList = (a, n, hi) => (Array.isArray(a) ? a.slice(0, n).filter((x) => Number.isInteger(x) && x >= 0 && x <= hi) : []);
  function cleanLoadout(l) {
    if (!l || typeof l !== 'object' || Array.isArray(l)) return null;
    let size = 0; try { size = JSON.stringify(l).length; } catch { return null; }
    if (size > 2000) return null;
    const gear = (Array.isArray(l.gear) ? l.gear : []).slice(0, 5).map((g) => (typeof g === 'string' && /^[a-z0-9_]{1,24}$/.test(g) ? g : ''));
    while (gear.length < 5) gear.push('');
    const combos = (Array.isArray(l.combos) ? l.combos : []).slice(0, 2).filter((c) => c && Array.isArray(c.steps)).map((c) => ({
      slot: int(c.slot, 0, 1), steps: c.steps.slice(0, 5).filter((st) => Array.isArray(st) && Number.isInteger(st[0]) && st[0] >= 0 && st[0] < 256).map((st) => [st[0], st[1] ? 1 : 0]),
    }));
    return { hero: int(l.hero, 0, ROSTER - 1), gear, moves: intList(l.moves, 24, 255), combos, special: int(l.special, 0, 40, null), bonuses: intList(l.bonuses, 48, 255), pet: int(l.pet, 0, 3), pet_level: int(l.pet_level, 1, 5, 1), daemon: int(l.daemon, 0, 9, null) };
  }
  const send = (ws, msg) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(msg)); };
  // Texte venu d'un joueur : sans caracteres de controle, espaces nettoyes, longueur bornee.
  const cleanText = (v, max) => String(v ?? '').replace(/[\u0000-\u001f\u007f-\u009f\u200b-\u200f\u2028-\u202e]/g, '').replace(/\s+/g, ' ').trim().slice(0, max);
  // Pseudo assaini : pas de lien ni d'adresse mail (ni de donnees personnelles collees par erreur), au moins une lettre ou un chiffre.
  const cleanName = (v) => {
    let n = cleanText(v, 120).replace(/[a-z][a-z0-9+.-]*:\/\/\S*/gi, ' ').replace(/\S+@\S+\.\S+/g, ' ').replace(/www\.\S+/gi, ' ').replace(/\d{6,}/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 14).trim();
    return /[\p{L}\p{N}]/u.test(n) ? n : 'Joueur';
  };
  const cleanCode = (v) => String(v ?? '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 8);
  const ipCount = new Map(); // ip -> connexions ouvertes
  const bornAt = Date.now();

  // ---------------------------------------------------------------- HTTP
  const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.wasm': 'application/wasm', '.json': 'application/json', '.png': 'image/png', '.webmanifest': 'application/manifest+json', '.svg': 'image/svg+xml', '.ico': 'image/x-icon', '.css': 'text/css; charset=utf-8', '.ogg': 'audio/ogg', '.webp': 'image/webp', '.gif': 'image/gif', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.wav': 'audio/wav', '.mp4': 'video/mp4', '.map': 'application/json', '.txt': 'text/plain; charset=utf-8' };
  const server = http.createServer((req, res) => {
    let url;
    try { url = new URL(req.url, 'http://x'); } catch { res.writeHead(400, { 'content-type': 'text/plain' }); res.end('requete invalide'); return; }
    if (url.pathname === '/health') { res.writeHead(200, { 'content-type': 'text/plain' }); res.end('ok'); return; }
    if (url.pathname === '/api/rooms') {
      res.writeHead(200, { 'content-type': 'application/json', 'access-control-allow-origin': '*' });
      res.end(JSON.stringify(publicList())); return;
    }
    if (url.pathname === '/api/status') {
      res.writeHead(200, { 'content-type': 'application/json', 'access-control-allow-origin': '*' });
      res.end(JSON.stringify({ v: PROTOCOL_VERSION, rooms: rooms.size, players: [...rooms.values()].reduce((n, r) => n + humans(r).length, 0), hosted: [...rooms.values()].filter((r) => r.full).length, conns: clients.size, up: Math.round((Date.now() - bornAt) / 1000) })); return;
    }
    if (url.pathname.startsWith('/api/room/')) {
      const code = cleanCode(url.pathname.slice('/api/room/'.length));
      res.writeHead(200, { 'content-type': 'application/json', 'access-control-allow-origin': '*', 'cache-control': 'no-store' });
      res.end(JSON.stringify(peekRoom(code))); return;
    }
    if (url.pathname === '/api/ideas' && req.method === 'POST') { handleIdea(req, res); return; }
    if (push.handle(req, res, url.pathname)) return;
    // Chemin mal encode (« %E0%A4 », « % ») : 400 au lieu d'une exception non rattrapee qui arretait tout le serveur.
    let p;
    try { p = decodeURIComponent(url.pathname); } catch { res.writeHead(400, { 'content-type': 'text/plain' }); res.end('chemin invalide'); return; }
    // Propositions hebergees ailleurs (PROPS_ORIGIN, ex. un 2e site Render statique) : redirection, liens inchanges.
    if (process.env.PROPS_ORIGIN && p.startsWith('/propositions/')) { res.writeHead(302, { location: process.env.PROPS_ORIGIN.replace(/\/+$/, '') + p + url.search, 'cache-control': 'public, max-age=300' }); res.end(); return; }
    if (p === '/') p = '/index.html';
    else if (p.endsWith('/')) p += 'index.html'; // dossiers : /combat_v2/ sert son index.html
    // Dossier de la racine EXACTEMENT (avec separateur) : « web2 » n'est pas dans « web ».
    const rootDir = path.resolve(root);
    const file = path.resolve(rootDir, '.' + path.sep + p.replace(/^[\\/]+/, ''));
    if (p.includes('\0') || !(file === rootDir || file.startsWith(rootDir + path.sep)) || !fs.existsSync(file) || fs.statSync(file).isDirectory()) {
      res.writeHead(404, { 'content-type': 'text/plain' }); res.end('introuvable'); return;
    }
    serveStatic(req, res, file);
  });

  // Fichiers du jeu : ETag + Last-Modified (revalidation : 304 sans retelecharger les 10+ Mo du wasm), compression gzip/brotli
  // des types compressibles gardee en memoire (calculee une fois par version de fichier), erreurs de lecture rattrapees
  // (un flux sans gestionnaire d'erreur arretait tout le relais), seulement GET/HEAD.
  const COMPRESSIBLE = new Set(['.html', '.js', '.json', '.wasm', '.webmanifest', '.svg', '.txt', '.css', '.map']);
  const MEDIA_LONG = new Set(['.gif', '.webp', '.jpg', '.jpeg', '.png', '.mp4', '.ogg', '.wav']); // medias des propositions : cache long (les pages, elles, restent revalidees)
  const zcache = new Map(); // "fichier|mtime|taille|codage" -> Promise<Buffer>
  const ZCACHE_MAX = 12;
  const BROTLI_MAX = 3 * 1024 * 1024; // au-dela (le wasm) : gzip, bien plus rapide a calculer sur un petit serveur
  function compressed(file, st, enc) {
    const key = `${file}|${st.mtimeMs}|${st.size}|${enc}`;
    let hit = zcache.get(key);
    if (!hit) {
      hit = fs.promises.readFile(file).then((buf) => new Promise((ok, ko) => {
        const done = (e, out) => (e ? ko(e) : ok(out));
        if (enc === 'br') zlib.brotliCompress(buf, { params: { [zlib.constants.BROTLI_PARAM_QUALITY]: 5, [zlib.constants.BROTLI_PARAM_SIZE_HINT]: buf.length } }, done);
        else zlib.gzip(buf, { level: 6 }, done);
      }));
      hit.catch(() => zcache.delete(key));
      zcache.set(key, hit);
      while (zcache.size > ZCACHE_MAX) zcache.delete(zcache.keys().next().value);
    }
    return hit;
  }
  function serveStatic(req, res, file) {
    if (req.method !== 'GET' && req.method !== 'HEAD') { res.writeHead(405, { 'content-type': 'text/plain', allow: 'GET, HEAD' }); res.end('methode refusee'); return; }
    let st;
    try { st = fs.statSync(file); } catch { res.writeHead(404, { 'content-type': 'text/plain' }); res.end('introuvable'); return; }
    const ext = path.extname(file);
    const accept = String(req.headers['accept-encoding'] || '');
    const enc = COMPRESSIBLE.has(ext) && st.size > 1024 ? (/\bbr\b/.test(accept) && st.size <= BROTLI_MAX ? 'br' : /\bgzip\b/.test(accept) ? 'gzip' : '') : '';
    const etag = `"${st.size.toString(16)}-${Math.round(st.mtimeMs).toString(16)}${enc ? '-' + enc : ''}"`;
    const head = {
      'content-type': MIME[ext] || 'application/octet-stream', 'cache-control': MEDIA_LONG.has(ext) && /[\\/]propositions[\\/]/.test(file) ? 'public, max-age=604800, stale-while-revalidate=86400' : 'no-cache', etag, 'last-modified': st.mtime.toUTCString(),
      'x-content-type-options': 'nosniff', vary: 'Accept-Encoding',
    };
    if (ext === '.js' && path.basename(file) === 'sw.js') head['service-worker-allowed'] = '/';
    const inm = req.headers['if-none-match'];
    if (inm && inm.split(',').some((t) => t.trim() === etag || t.trim() === 'W/' + etag)) { res.writeHead(304, head); res.end(); return; }
    const fail = () => { if (!res.headersSent) { try { res.writeHead(500, { 'content-type': 'text/plain' }); } catch { /* deja ferme */ } } res.end(); };
    if (enc) {
      compressed(file, st, enc).then((buf) => {
        res.writeHead(200, { ...head, 'content-encoding': enc, 'content-length': buf.length });
        res.end(req.method === 'HEAD' ? undefined : buf);
      }).catch(fail);
      return;
    }
    res.writeHead(200, { ...head, 'content-length': st.size });
    if (req.method === 'HEAD') { res.end(); return; }
    const rs = fs.createReadStream(file);
    rs.on('error', () => { res.destroy(); });
    res.on('close', () => rs.destroy());
    rs.pipe(res);
  }

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
  const emptySlot = () => ({ color: 0, ws: null, name: '', char: 0, ready: false, token: null, mode: 'free', hi: -1, journal: [], lastMove: Date.now(), lastBits: 0, heldUntil: 0, ip: '', loadout: null, cmds: 0, lastLoadout: 0, loadouts: 0, lastChat: 0, lastResync: 0, afkWarned: false, dormantDue: false, daemon: 255 });
  function makeRoom(isPublic) {
    const room = {
      code: newCode(), public: !!isPublic, started: false, created: Date.now(), lastAct: Date.now(), auto: false, run: 0, wave: 0, emptySince: 0, restartTimer: null, stageBase: rnd(STAGES),
      keep: false, ownerIp: '', // keep : salon a lien permanent (reste ouvert `roomTtl` apres le dernier joueur) ; ownerIp : qui l'a ouvert (limite par adresse)
      ff: true, // tir allie (reglage de l'hote, annonce a tous dans la config de partie)
      options: { size: 2, stage: 0 }, // size = nombre de heros en partie (les places vides sont tenues par l'IA)
      slots: Array.from({ length: MAX_SLOTS }, emptySlot),
      seed: 0, config: null, ctl: [], hashes: new Map(),
    };
    rooms.set(room.code, room);
    return room;
  }
  const humans = (room) => room.slots.map((s, i) => [s, i]).filter(([s]) => s.ws);
  const lobbyView = (room) => ({
    t: 'room', code: room.code, public: room.public, started: room.started, auto: room.auto, run: room.run, options: { ...room.options, ff: room.ff !== false, ffm: room.ffm, pal: room.pal !== false }, host: hostSlot(room), full: !!room.full, private: !room.public, keep: !!room.keep, ttl: Math.round(roomTtl / 60000),
    players: room.slots.map((s, i) => ({ slot: i, name: s.name, char: s.char, color: s.color, daemon: s.daemon, ready: s.ready, online: !!s.ws, mode: s.mode })),
  });
  const broadcast = (room, msg, except = null) => { for (const [s] of humans(room)) if (s.ws !== except) send(s.ws, msg); };
  // Liste publique : seulement les salons habites, les plus remplis (puis les plus recents) d'abord, bornee.
  const publicList = () => [...rooms.values()].filter((r) => r.public && humans(r).length > 0)
    .sort((a, b) => (humans(b).length / b.options.size - humans(a).length / a.options.size) || (b.created - a.created)).slice(0, 50)
    .map((r) => ({ code: r.code, auto: r.auto, stage: r.options.stage, run: r.run, wave: r.wave, n: humans(r).length, max: r.options.size, started: r.started, host: r.slots.find((s) => s.ws)?.name || '', full: !!r.full, mode: r.mode | 0, chapter: r.chapter | 0, open: humans(r).length < r.options.size, age: Math.round((Date.now() - r.created) / 1000), players: humans(r).map(([s]) => s.name) }));
  function hostSlot(room) { return room.slots.findIndex((s) => s.ws); }
  // Etat public d'un salon connu par son code : AUCUNE donnee personnelle (pseudos et heros seulement, jamais d'adresse).
  function peekRoom(code) {
    const room = rooms.get(code);
    if (!room) return { code, alive: false };
    const hs = humans(room);
    const size = room.started ? room.config.size : room.options.size;
    return {
      code, alive: true, public: room.public, started: room.started, auto: room.auto, full: !!room.full, n: hs.length, max: room.started ? Math.max(size, room.options.size) : room.options.size,
      open: hs.length < (room.started ? Math.max(size, room.options.size) : room.options.size), names: hs.map(([s]) => s.name).slice(0, 8),
      idle: Math.round((Date.now() - (hs.length ? room.lastAct : (room.emptySince || room.lastAct))) / 1000), ttl: room.keep ? Math.round(roomTtl / 1000) : 0,
    };
  }

  function attach(room, ws, c, name) {
    const now = Date.now();
    const limit = room.started ? room.config.size : MAX_SLOTS;
    const idx = room.slots.findIndex((s, i) => i < limit && !s.ws && s.heldUntil < now && (!room.started || s.mode === 'ai'));
    if (idx < 0) return -1;
    const s = room.slots[idx];
    s.ws = ws; s.name = uniqueName(room, cleanName(name), idx); s.token = c.token; s.ip = c.ip; s.heldUntil = 0; s.color = pickColor(room, idx, c.color); s.daemon = 255;
    s.ready = false; s.lastMove = now; s.afkWarned = false;
    c.room = room; c.slot = idx;
    if (!room.started) s.mode = 'human';
    return idx;
  }
  // Deux amis avec le meme pseudo : le second devient « Nom 2 » (on sait qui parle dans le chat).
  function uniqueName(room, name, idx) {
    const taken = new Set(room.slots.filter((s, i) => i !== idx && s.ws).map((s) => s.name));
    if (!taken.has(name)) return name;
    for (let k = 2; k < 10; k++) { const n = `${name.slice(0, 12)} ${k}`; if (!taken.has(n)) return n; }
    return name;
  }

  // Couleur unique dans un salon : si la couleur voulue est prise par un autre joueur present, on propose la plus proche libre.
  function pickColor(room, idx, want) {
    const taken = new Set(room.slots.filter((x, i) => i !== idx && x.ws).map((x) => x.color));
    const w = ((Number.isInteger(want) ? want : idx) % COLORS + COLORS) % COLORS;
    for (let d = 0; d < COLORS; d++) for (const k of d === 0 ? [0] : [d, -d]) { const c = (w + k + COLORS) % COLORS; if (!taken.has(c)) return c; }
    return w;
  }
  function startGame(room) {
    const taken = room.slots.filter((s) => s.ws).length;
    const size = Math.max(room.options.size, taken, 1);
    const used = new Set(room.slots.filter((s) => s.ws).map((s) => s.char));
    room.slots.forEach((s, i) => {
      if (i >= size) { s.mode = 'free'; return; }
      if (s.ws) s.mode = 'human';
      else { // place vide : heros IA, personnage different des autres
        let ch = room.auto ? rnd(ROSTER) : 0;
        if (!room.auto) while (used.has(ch)) ch++;
        else for (let k = 0; used.has(ch) && k < ROSTER; k++) ch = (ch + 1) % ROSTER;
        used.add(ch); s.char = ch; s.mode = 'ai';
        // Place vide : pas d'IA (salon sans IA, parties automatiques et hebergees) -> heros absent, pseudo vide, jusqu'a l'arrivee d'un ami.
        s.name = (room.options.noai || room.auto || room.full) ? '' : 'IA';
      }
      s.hi = -1; s.journal = []; s.lastMove = Date.now(); s.lastBits = 0; s.cmds = 0; s.loadouts = 0; s.afkWarned = false;
    });
    room.started = true; room.ctl = []; room.hashes = new Map();
    // Revanche apres une defaite : MEME graine (meme terrain, memes vagues) ; nouvelle graine seulement apres une victoire.
    room.seed = (room.keepSeed && room.seed) ? room.seed : ((rnd(0x7fffffff) >>> 0) || 1);
    room.keepSeed = false;
    room.config = { seed: room.seed, size, run: room.run, stage: room.options.stage | 0, full: !!room.full, mode: room.mode | 0, chapter: room.chapter | 0, wave: room.run === 0 ? (room.startWave | 0) : 0, hp: room.run === 0 ? (room.startHp | 0) : 0, ff: room.ff !== false, ffm: room.ffm, pal: room.pal !== false, slots: room.slots.slice(0, size).map((s, i) => ({ slot: i, char: s.char, name: s.name, ai: s.mode === 'ai' })) };
    broadcast(room, { t: 'start', ...room.config });
  }

  // Avance d'un evenement de controle : le temps de rejeu du journal chez l'arrivant (~ 1/25 des images deja jouees) pour ne pas figer les autres, au moins 15 images.
  const rejoinMargin = (room) => Math.max(REJOIN_MIN, Math.min(REJOIN_MAX, Math.round(highest(room) / 25) + 12));
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
  // `dormant` : le heros disparait avec son joueur (parties sans IA) au lieu de continuer a se battre tout seul.
  const noAiRoom = (room) => !!(room.full || room.auto || room.options.noai);
  // Parties sans IA (hebergees, automatiques) : l'IA tient la place (`bossDrop`) jusqu'au prochain boss, puis le heros s'efface cote clients ;
  // si le joueur revient avant (meme jeton), il reprend CE MEME heros immediatement.
  function dropToAi(room, slot) {
    const s = room.slots[slot];
    if (s.mode !== 'human') return;
    s.mode = 'ai';
    s.dormantDue = false;
    control(room, slot, s.hi + 1, noAiRoom(room) ? { ai: true, bossDrop: true } : { ai: true });
  }

  function closeRoom(room) {
    if (room.restartTimer) clearTimeout(room.restartTimer);
    reports.delete(room.code);
    banned.delete(room.code); // la liste des expulses disparait avec le salon (sinon elle fuit en memoire)
    rooms.delete(room.code);
  }

  // ---- Parties toujours en cours : host / join_any (pas de salon d'attente, rejoignable a tout moment)
  function startAuto(room) {
    room.options = { size: room.full ? (room.hostedSize || HOSTED_SIZE) : MAX_SLOTS, stage: room.full ? Math.max(0, Math.min(FULL_STAGES - 1, room.fixedStage ?? 0)) : Math.max(0, Math.min(STAGES - 1, room.fixedStage ?? (room.stageBase + (room.wins | 0)) % STAGES)) };
    startGame(room);
  }
  function restartRun(room) {
    room.restartTimer = null;
    if (!rooms.has(room.code) || !room.started || humans(room).length === 0) return;
    room.run++;
    room.wave = 0;
    room.slots.forEach((s) => { if (!s.ws && s.mode === 'human') s.mode = 'ai'; });
    startAuto(room);
  }
  function rateOk(ws, c) {
    const now = Date.now();
    if (now - (joinRate.get(c.ip) || 0) < 300) { send(ws, { t: 'error', msg: 'Trop de demandes, patiente un instant' }); return false; }
    joinRate.set(c.ip, now);
    if (joinRate.size > 5000) joinRate.clear();
    return true;
  }
  function takeAi(room, ws, c, char, now) {
    const cands = room.slots.map((s, i) => [s, i]).filter(([s, i]) => i < room.config.size && !s.ws && s.mode === 'ai');
    if (!cands.length) return -1;
    cands.sort((a, b) => ((a[0].heldUntil > now) - (b[0].heldUntil > now)) || ((b[0].char === char) - (a[0].char === char)));
    const [s, idx] = cands[0];
    s.ws = ws; s.name = uniqueName(room, cleanName(c.name), idx); s.token = c.token; s.ip = c.ip; s.heldUntil = 0; s.color = pickColor(room, idx, c.color); s.ready = false; s.lastMove = now; s.afkWarned = false;
    c.room = room; c.slot = idx;
    return idx;
  }
  // Limites par adresse : la boucle locale (tests, developpement) en est exemptee sauf si l'option est donnee explicitement.
  const isLocal = (ip) => opts.ipLimits === undefined && /^(127\.0\.0\.1|::1|::ffff:127\.0\.0\.1)$/.test(ip);
  const roomsOfIp = (ip) => [...rooms.values()].filter((r) => r.slots.some((s) => s.ws && s.ip === ip) || (r.keep && r.ownerIp === ip)).length;
  // Fenetre glissante : au plus MAX_MATCH_PER_MIN demandes de partie par minute et par adresse (hors boucle locale).
  function matchOk(ws, c) {
    if (isLocal(c.ip)) return true;
    const now = Date.now();
    const l = (matchLog.get(c.ip) || []).filter((t) => now - t < 60000);
    if (l.length >= MAX_MATCH_PER_MIN) { send(ws, { t: 'error', msg: 'Trop de demandes de partie : patiente une minute', code: 'rate' }); return false; }
    l.push(now); matchLog.set(c.ip, l);
    if (matchLog.size > 5000) matchLog.clear();
    return true;
  }
  // Une adresse qui a deja ses salons permanents vides : le plus ancien cede la place au nouveau (jamais un salon habite).
  function evictOwned(ip) {
    if (isLocal(ip)) return;
    const mine = [...rooms.values()].filter((r) => r.keep && r.ownerIp === ip && humans(r).length === 0).sort((a, b) => a.emptySince - b.emptySince);
    while (roomsOfIp(ip) >= MAX_ROOMS_PER_IP && mine.length) closeRoom(mine.shift());
  }
  function hostNew(ws, c, char, priv, stage, full, ff, keep) {
    if (rooms.size >= maxRooms) return send(ws, { t: 'error', msg: 'Serveur plein, reessaie dans un instant', code: 'full' });
    evictOwned(c.ip);
    if (roomsOfIp(c.ip) >= MAX_ROOMS_PER_IP && !isLocal(c.ip)) return send(ws, { t: 'error', msg: 'Trop de parties ouvertes depuis cette connexion', code: 'limit' });
    const room = makeRoom(!priv);
    room.auto = true; room.keep = !!keep; room.ownerIp = c.ip;
    if (typeof ff === 'boolean') room.ff = ff;
    if (full) {
      room.full = true; room.mode = int(full.mode, 0, 4); room.chapter = int(full.chapter, 0, 64);
      room.hostedSize = int(full.size, HOSTED_MIN, MAX_SLOTS, HOSTED_SIZE); // 2 a 8 joueurs (places vides dormantes)
      room.startWave = int(full.wave, 0, 64, 0); room.startHp = int(full.hp, 0, 100, 0); // solo transforme : on reprend a la vague et aux PV de l'hote
    }
    if (Number.isInteger(stage) && stage >= 0 && stage < (room.full ? FULL_STAGES : STAGES)) room.fixedStage = stage;
    const idx = attach(room, ws, c, c.name);
    room.slots[idx].char = char;
    send(ws, { ...lobbyView(room), you: idx, token: c.token });
    startAuto(room);
    // L'equipement de l'hote s'applique des la premiere image (identique chez tous).
    const lo = full && cleanLoadout(full.loadout);
    if (lo) { room.slots[idx].loadout = lo; control(room, idx, 0, { kind: 'loadout', data: lo }); }
  }
  function joinAny(ws, c, m) {
    if (!rateOk(ws, c) || !matchOk(ws, c)) return;
    if (c.room) leaveRoom(c, ws, true);
    const char = Math.max(0, Math.min(ROSTER - 1, m.char | 0));
    if (m.name) c.name = cleanName(m.name);
    if (Number.isInteger(m.color)) c.color = m.color;
    const now = Date.now();
    // La partie publique en cours la plus remplie qui a encore une place (a egalite : la plus ancienne).
    const open = [...rooms.values()].filter((r) => r.auto && r.public && r.started && humans(r).length > 0 && humans(r).length < r.options.size && (!r.full || c.v >= 3) && !banned.get(r.code)?.has(c.ip))
      .sort((a, b) => humans(b).length - humans(a).length)[0];
    if (open) {
      const idx = takeAi(open, ws, c, char, now);
      if (idx >= 0) { if (open.full) open.slots[idx].loadout = cleanLoadout(m.loadout); finishJoin(open, ws, c, idx); return send(ws, { t: 'matched', how: 'joined', code: open.code, n: humans(open).length, max: open.options.size }); }
    }
    hostNew(ws, c, char, false, undefined, null, typeof m.ff === 'boolean' ? m.ff : undefined, m.keep === true);
  }
  function hostGame(ws, c, m) {
    if (!rateOk(ws, c) || !matchOk(ws, c)) return;
    if (c.room) leaveRoom(c, ws, true);
    if (m.name) c.name = cleanName(m.name);
    if (Number.isInteger(m.color)) c.color = m.color;
    const full = m.full && c.v >= 3 ? m : null;
    hostNew(ws, c, Math.max(0, Math.min(ROSTER - 1, m.char | 0)), !!m.private, m.stage === undefined ? undefined : m.stage | 0, full, typeof m.ff === 'boolean' ? m.ff : undefined, m.keep === true);
  }

  function leaveRoom(c, ws, permanent) {
    const room = c.room;
    if (!room) return;
    const s = room.slots[c.slot];
    if (s && s.ws === ws) {
      s.ws = null; s.ready = false;
      if (room.started) {
        // Depart volontaire ou coupure : meme traitement. L'IA tient la place jusqu'au prochain boss ; le meme jeton reprend le meme heros.
        dropToAi(room, c.slot);
        s.heldUntil = (permanent && !noAiRoom(room)) ? 0 : Date.now() + (room.auto ? autoHoldMs : holdMs);
        if (room.auto) broadcast(room, { t: 'presence', ev: 'leave', name: s.name, slot: c.slot });
      } else { s.mode = 'free'; s.name = ''; s.token = null; }
    }
    c.room = null;
    if (humans(room).length === 0) {
      // Partie en cours : plus personne -> elle se termine ; une simple coupure laisse un court delai de reprise.
      if (room.keep) room.emptySince = Date.now(); // lien permanent : le salon reste ouvert (expiration `roomTtl` apres le dernier joueur)
      else if (!room.auto || permanent) closeRoom(room); else room.emptySince = Date.now();
      return;
    }
    if (!room.started || room.auto) broadcast(room, lobbyView(room));
  }

  // ---------------------------------------------------------------- WebSocket
  const wss = new WebSocketServer({ server, maxPayload: 16 * 1024 });
  const clients = new Map(); // ws -> etat
  wss.on('connection', (ws, req) => {
    // x-forwarded-for n'est cru que derriere un proxy declare (TRUST_PROXY=1 ou opts.trustProxy) : sinon un client l'invente et contourne la limite par adresse.
    // Derriere un proxy, on prend la DERNIERE entree (celle ajoutee par le proxy), jamais la premiere (fournie par le client).
    const fwd = trustProxy ? String(req.headers['x-forwarded-for'] || '').split(',').pop().trim() : '';
    const ip = (fwd || req.socket.remoteAddress || '').toString();
    if (clients.size >= (opts.maxConns ?? MAX_CONNS) || ((ipCount.get(ip) || 0) >= (opts.connPerIp ?? MAX_CONN_PER_IP) && !isLocal(ip))) {
      send(ws, { t: 'error', msg: clients.size >= (opts.maxConns ?? MAX_CONNS) ? 'Serveur tres occupe, reessaie dans un instant' : 'Trop de connexions depuis cette adresse', fatal: true, code: 'limit' });
      ws.close(); return;
    }
    ipCount.set(ip, (ipCount.get(ip) || 0) + 1);
    const c = { id: crypto.randomBytes(4).toString('hex'), v: 1, token: newToken(), room: null, slot: -1, ip, name: 'Joueur', color: -1, msgs: 0, since: Date.now(), hello: false, alive: true };
    clients.set(ws, c);
    ws.on('pong', () => { c.alive = true; });
    ws.on('message', (data) => {
      c.alive = true;
      let m;
      try { m = JSON.parse(data.toString()); } catch { return; }
      if (!m || typeof m.t !== 'string') return;
      if (!c.hello && m.t !== 'hello') return send(ws, { t: 'error', msg: 'Connexion mal initialisee, relance le jeu', code: 'nohello' });
      const now = Date.now(); // limite de debit grossiere (les entrees arrivent a 60/s)
      if (now - c.since > 1000) { c.since = now; c.msgs = 0; }
      if (++c.msgs > 240) return;
      try { handle(ws, c, m); } catch (e) { send(ws, { t: 'error', msg: 'message invalide' }); }
    });
    ws.on('close', () => {
      leaveRoom(c, ws, false); clients.delete(ws);
      const n = (ipCount.get(ip) || 1) - 1; if (n <= 0) ipCount.delete(ip); else ipCount.set(ip, n);
    });
    ws.on('error', () => {});
  });

  function joinRoom(ws, c, code, m = {}) {
    const room = rooms.get(cleanCode(code));
    if (!room) return send(ws, { t: 'error', msg: 'Salon introuvable : la partie est peut-etre terminee', code: 'gone' });
    if (banned.get(room.code)?.has(c.ip)) return send(ws, { t: 'error', msg: 'Tu as ete expulse de ce salon' });
    if (room.full && c.v < 3) return send(ws, { t: 'error', msg: 'Partie complete : mets le jeu a jour pour la rejoindre', fatal: true });
    if (c.room) leaveRoom(c, ws, true);
    if (Number.isInteger(m.color)) c.color = m.color;
    if (room.full && room.started) {
      // Partie hebergee en cours : on prend la place IA (comme join_any), l'equipement est transmis dans le message.
      const idx2 = takeAi(room, ws, c, int(m.char, 0, ROSTER - 1), Date.now());
      if (idx2 < 0) return send(ws, { t: 'error', msg: `Partie pleine (${room.options.size} joueurs)`, code: 'full' });
      room.slots[idx2].loadout = cleanLoadout(m.loadout);
      return finishJoin(room, ws, c, idx2);
    }
    const idx = attach(room, ws, c, c.name);
    if (idx < 0) return send(ws, { t: 'error', msg: room.started ? 'Plus de place libre en cours de partie' : 'Salon complet' });
    finishJoin(room, ws, c, idx);
  }
  function finishJoin(room, ws, c, idx) {
    send(ws, { ...lobbyView(room), you: idx, token: c.token });
    if (room.started) {
      // Arrivee en cours de partie : rejeu du journal cote client, la place est reprise a une image future identique pour tous.
      const from = highest(room) + rejoinMargin(room);
      const s = room.slots[idx];
      s.mode = 'human'; s.lastMove = Date.now();
      // Place jamais jouee par un humain : sans cela ses premieres entrees (image >> 600) seraient refusees (f > hi + 600).
      if (s.hi < from - 1) s.hi = from - 1;
      send(ws, { ...resyncPayload(room), you: idx });
      control(room, idx, from, { ai: false, name: s.name });
      if (room.full && s.loadout) control(room, idx, from, { kind: 'loadout', data: s.loadout });
      if (room.auto) { broadcast(room, { t: 'presence', ev: 'join', name: s.name, slot: idx }, ws); broadcast(room, lobbyView(room), ws); }
    } else broadcast(room, lobbyView(room), ws);
  }

  function handle(ws, c, m) {
    switch (m.t) {
      case 'hello':
        if (!COMPAT_VERSIONS.includes(m.v)) return send(ws, { t: 'error', msg: 'Version incompatible : mets le jeu a jour', fatal: true });
        c.v = m.v; c.name = cleanName(m.name); c.hello = true; if (Number.isInteger(m.color)) c.color = m.color;
        if (m.token && /^[0-9a-f]{24}$/.test(m.token)) c.token = m.token;
        return send(ws, { t: 'welcome', v: PROTOCOL_VERSION, id: c.id, token: c.token });
      case 'join_any': return joinAny(ws, c, m);
      case 'host': return hostGame(ws, c, m);
      case 'list': return send(ws, { t: 'rooms', rooms: publicList() });
      case 'create': {
        if (!matchOk(ws, c)) return;
        if (c.room) leaveRoom(c, ws, true);
        evictOwned(c.ip);
        if (roomsOfIp(c.ip) >= MAX_ROOMS_PER_IP && !isLocal(c.ip)) return send(ws, { t: 'error', msg: 'Trop de parties ouvertes depuis cette connexion', code: 'limit' });
        if (rooms.size >= maxRooms) return send(ws, { t: 'error', msg: 'Serveur plein, reessaie dans un instant', code: 'full' });
        const room = makeRoom(!!m.public); // salon d'attente : PRIVE par defaut (lien), public seulement si demande
        room.keep = m.keep === true; room.ownerIp = c.ip;
        if (m.size !== undefined) room.options.size = int(m.size, 1, MAX_SLOTS, 2);
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
      case 'join': return joinRoom(ws, c, m.code, m);
      case 'peek': { // etat de salons connus (reprise de la derniere partie) : existe-t-il encore, qui est la, reste-t-il de la place
        const codes = (Array.isArray(m.codes) ? m.codes : [m.code]).slice(0, MAX_PEEK).map(cleanCode).filter((x) => x.length >= 4);
        return send(ws, { t: 'peeked', rooms: codes.map((code) => peekRoom(code)) });
      }
      case 'rejoin': {
        const room = rooms.get(cleanCode(m.code));
        if (!room) return send(ws, { t: 'error', msg: 'Reprise impossible', detail: 'Cette partie est terminee.', rejoin: true, code: 'gone' });
        if (typeof m.token !== 'string' || !/^[0-9a-f]{24}$/.test(m.token)) return send(ws, { t: 'error', msg: 'Reprise impossible', detail: 'Jeton de reprise invalide.', rejoin: true, code: 'token' });
        const idx = room.slots.findIndex((s) => s.token && s.token === m.token);
        if (idx < 0) return send(ws, { t: 'error', msg: 'Reprise impossible', detail: 'Ta place a ete reprise par un autre joueur.', rejoin: true, code: 'taken' });
        if (banned.get(room.code)?.has(c.ip)) return send(ws, { t: 'error', msg: 'Reprise impossible', detail: 'Tu as ete expulse de ce salon.', rejoin: true, code: 'banned' });
        const s = room.slots[idx];
        // Ce client occupait deja un autre salon : il le quitte (sinon sa place y reste « occupee » par une connexion fantome).
        if (c.room && c.room !== room) leaveRoom(c, ws, true);
        // Ancienne connexion encore ouverte (coupure que le serveur n'a pas encore vue, frequent sur telephone) : on la remplace.
        if (s.ws && s.ws !== ws) {
          const old = s.ws; const oc = clients.get(old);
          s.ws = null; if (oc) { oc.room = null; oc.slot = -1; }
          send(old, { t: 'error', msg: 'Connexion reprise depuis un autre appareil', code: 'replaced' });
          try { old.close(); } catch { /* deja fermee */ }
        }
        s.ws = ws; s.heldUntil = 0; s.afkWarned = false; c.room = room; c.slot = idx; c.token = m.token;
        return finishJoin(room, ws, c, idx);
      }
      case 'leave': leaveRoom(c, ws, true); return send(ws, { t: 'left' });
      case 'ping': return send(ws, { t: 'pong', n: typeof m.n === 'number' ? m.n : undefined });
      default: break;
    }
    const room = c.room;
    if (!room) return;
    const s = room.slots[c.slot];
    if (m.t !== "input" && m.t !== "hash" && m.t !== "wave" && m.t !== "end") room.lastAct = Date.now(); // activite du salon (menage des salons abandonnes)
    switch (m.t) {
      case 'lobby':
        if (room.started) return;
        s.char = Math.max(0, Math.min(99, m.char | 0));
        s.ready = !!m.ready;
        if (Number.isInteger(m.color)) { c.color = m.color; s.color = pickColor(room, c.slot, m.color); }
        s.daemon = Number.isInteger(m.daemon) && m.daemon >= 0 && m.daemon < 64 ? m.daemon : 255; // indice d'espece : borne, jamais de valeur libre
        if (m.name) { s.name = uniqueName(room, cleanName(m.name), c.slot); c.name = s.name; }
        return broadcast(room, lobbyView(room));
      case 'options':
        if (room.started || c.slot !== hostSlot(room)) return;
        room.options = { size: Math.max(1, Math.min(MAX_SLOTS, (m.size | 0) || 2)), stage: Math.max(0, Math.min(11, m.stage | 0)), noai: m.noai === true };
        if (typeof m.ff === 'boolean') room.ff = m.ff;
        if (Number.isInteger(m.ffm)) { room.ffm = Math.max(0, Math.min(2, m.ffm)); room.ff = room.ffm > 0; }
        if (typeof m.pal === 'boolean') room.pal = m.pal;
        if (typeof m.public === 'boolean') room.public = m.public;
        return broadcast(room, lobbyView(room));
      case 'start': // l'hote lance quand il veut, sans attendre les autres (ceux qui arrivent ensuite prennent une place IA en cours de partie)
        if (room.started || c.slot !== hostSlot(room)) return;
        if (typeof m.ff === 'boolean') room.ff = m.ff;
        if (Number.isInteger(m.ffm)) { room.ffm = Math.max(0, Math.min(2, m.ffm)); room.ff = room.ffm > 0; }
        if (typeof m.pal === 'boolean') room.pal = m.pal;
        return startGame(room);
      case 'report': { // signaler un joueur : assez de signalements distincts et il quitte le salon (public ou prive)
        const t = room.slots[m.slot | 0];
        const nowR = Date.now();
        if (!t || !t.ws || t === s) return;
        if (nowR - (s.lastReport || 0) < REPORT_GAP_MS) return send(ws, { t: 'error', msg: 'Doucement, un signalement a la fois', code: 'report_rate' });
        s.lastReport = nowR;
        if (!reports.has(room.code)) reports.set(room.code, new Map());
        const byTarget = reports.get(room.code);
        const key = m.slot | 0;
        if (!byTarget.has(key)) byTarget.set(key, new Set());
        const who = byTarget.get(key); who.add(c.token);
        send(ws, { t: 'reported', slot: key });
        const others = humans(room).length - 1; // electeurs possibles (la cible exclue)
        if (who.size >= Math.max(2, Math.ceil(others / 2)) || (others === 1 && who.size >= 1 && room.public)) {
          if (!banned.has(room.code)) banned.set(room.code, new Set());
          if (banned.get(room.code).size < MAX_BANNED) banned.get(room.code).add(t.ip);
          const kws = t.ws; const kc = clients.get(kws);
          send(kws, { t: 'kicked', reason: 'reports' });
          byTarget.delete(key);
          if (kc) leaveRoom(kc, kws, true);
        }
        return;
      }
      case 'kick': {
        if (c.slot !== hostSlot(room)) return;
        const k = room.slots[m.slot | 0];
        if (!k || !k.ws || k === s) return;
        if (!banned.has(room.code)) banned.set(room.code, new Set());
        if (banned.get(room.code).size < MAX_BANNED) banned.get(room.code).add(k.ip);
        const kws = k.ws; const kc = clients.get(kws);
        send(kws, { t: 'kicked' });
        if (kc) leaveRoom(kc, kws, true);
        return;
      }
      case 'input': {
        if (!room.started || !Array.isArray(m.bits) || m.bits.length > 8 || !Number.isInteger(m.f0)) return;
        if (s.mode !== 'human') return;
        // Avance du journal limitee dans le temps (~240 images/s, salve 1500) : un client ne gonfle pas `hi` (et le journal RLE de reprise) a volonte.
        const nowI = Date.now();
        s.budget = Math.min(1500, (s.budget ?? 1500) + (nowI - (s.budgetAt ?? nowI)) * 0.24); s.budgetAt = nowI;
        m.bits.forEach((b, k) => {
          const f = m.f0 + k; b = b | 0;
          if (f < 0 || f > s.hi + 600) return;
          if (f > s.hi) { const cost = f - s.hi; if (cost > s.budget) return; s.budget -= cost; s.journal[f] = b; s.hi = f; }
          if (b !== s.lastBits) { s.lastBits = b; s.lastMove = room.lastAct = Date.now(); }
        });
        return broadcast(room, { t: 'input', slot: c.slot, f0: m.f0, bits: m.bits.map((b) => b | 0) }, ws);
      }
      case 'hash': {
        if (!room.started) return;
        const f = m.f | 0;
        // Empreinte = 16 chiffres hexadecimaux : tout le reste est ignore (memoire du salon bornee, pas de 16 Ko par message).
        if (f < 0 || typeof m.h !== 'string' || !/^[0-9a-f]{1,32}$/.test(m.h)) return;
        const h = room.hashes.get(f) || new Map();
        h.set(c.slot, m.h);
        room.hashes.set(f, h);
        while (room.hashes.size > 64) room.hashes.delete(room.hashes.keys().next().value);
        const need = room.slots.filter((x) => x.ws && x.mode === 'human').length;
        if (h.size >= need) {
          const ref = h.get(Math.min(...h.keys()));
          for (const [k, v] of h) {
            if (v === ref) continue;
            const sk = room.slots[k]; const nowH = Date.now();
            if (nowH - sk.lastResync < RESYNC_GAP_MS) continue; // pas de tempete de reprises si un client reste en desaccord
            sk.lastResync = nowH;
            send(sk.ws, { t: 'desync', f });
            send(sk.ws, { ...resyncPayload(room), you: k, forced: true });
          }
          room.hashes.delete(f);
        }
        for (const k of room.hashes.keys()) if (k < f - 600) room.hashes.delete(k);
        return;
      }
      case 'resume': { // un joueur remplace par l'IA (AFK) reprend la main
        if (!room.started || s.mode !== 'ai' || s.ws !== ws) return;
        s.mode = 'human'; s.lastMove = Date.now(); s.lastBits = 0;
        return control(room, c.slot, highest(room) + rejoinMargin(room), { ai: false, name: s.name });
      }
      case 'color': { // changement de couleur en cours de partie : annonce a tous (marqueur au-dessus du heros)
        if (!Number.isInteger(m.color) || Date.now() - (s.lastColor || 0) < 500) return;
        s.lastColor = Date.now(); c.color = m.color; s.color = pickColor(room, c.slot, m.color);
        return broadcast(room, lobbyView(room));
      }
      case 'loadout': { // equipement change en cours de partie (partie complete) : applique a une image future identique pour tous
        if (!room.full || !room.started || s.mode !== 'human') return;
        const lo = cleanLoadout(m.data);
        const now = Date.now();
        if (!lo || now - s.lastLoadout < 1000 || s.loadouts >= MAX_LOADOUTS) return;
        s.lastLoadout = now; s.loadout = lo; s.loadouts++;
        return control(room, c.slot, highest(room) + rejoinMargin(room), { kind: 'loadout', data: lo });
      }
      case 'cmd': { // vote de recompense, chapitre suivant, fin de dialogue : sequence comme une entree
        if (!room.full || !room.started || s.mode !== 'human') return;
        if (!['reward_pick', 'chapter_next', 'skip_dialog', 'arena'].includes(m.cmd) || s.cmds >= MAX_CMDS) return;
        const arg = int(m.arg, 0, 200);
        s.cmds++;
        return control(room, c.slot, highest(room) + CMD_MARGIN, { kind: 'cmd', cmd: m.cmd, arg });
      }
      case 'chat': {
        const text = cleanText(m.text, 120);
        const nowC = Date.now();
        if (!text) return;
        if (nowC - s.lastChat < CHAT_GAP_MS) return send(ws, { t: 'error', msg: 'Doucement, un message a la fois', code: 'chat_rate' });
        s.lastChat = nowC;
        broadcast(room, { t: 'chat', slot: c.slot, name: s.name, text });
        return;
      }
      case 'wave': // progression affichee dans la liste des parties en cours (seul un joueur actif la fait avancer)
        if (!room.started || s.mode !== 'human') return;
        room.wave = Math.max(0, Math.min(999, m.wave | 0));
        return;
      case 'end': // fin de partie : retour au salon (partie en cours : nouvelle partie apres un court ecran de resultats)
        if (!room.started) return;
        if (room.full) return; // partie hebergee : pas de relance automatique, les joueurs decident (chapitre suivant ou quitter)
        if (room.auto) {
          if (m.run !== undefined && (m.run | 0) !== room.run) return;
          // Coherence de progression : le terrain n'avance qu'apres une victoire ; une defaite rejoue le meme stage et la meme graine.
          if (!room.restartTimer) { if (m.won === true) room.wins = (room.wins | 0) + 1; else room.keepSeed = true; }
          if (!room.restartTimer) { room.restartTimer = setTimeout(() => restartRun(room), restartMs); room.restartTimer.unref?.(); }
          return;
        }
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
          if (s.ws && s.mode === 'human' && now - s.lastMove > afkMs) { dropToAi(room, i); send(s.ws, { t: 'afk' }); if (room.auto) broadcast(room, { t: 'presence', ev: 'afk', name: s.name, slot: i }, s.ws); s.afkWarned = false; }
          else if (s.ws && s.mode === 'human' && !s.afkWarned && now - s.lastMove > afkMs * 0.7) { s.afkWarned = true; send(s.ws, { t: 'afk_warn', in: Math.max(1, Math.round((afkMs - (now - s.lastMove)) / 1000)) }); }
          else if (s.afkWarned && now - s.lastMove < afkMs * 0.5) s.afkWarned = false;
        }
      }
      // Salon abandonne (joueurs connectes mais plus aucune action depuis longtemps, ex. telephone pose) : ferme, la place est liberee.
      if (humans(room).length > 0 && now - room.lastAct > idleMs) {
        for (const [x] of humans(room)) {
          const w = x.ws; const cc = clients.get(w);
          send(w, { t: 'error', msg: 'Partie fermee : plus personne n\'a joue depuis longtemps', code: 'idle' });
          if (cc) leaveRoom(cc, w, true);
        }
        continue;
      }
      if (humans(room).length === 0 && (room.keep ? now - (room.emptySince || room.created) > roomTtl : room.auto ? now - room.emptySince > graceMs : now - room.created > holdMs)) closeRoom(room);
    }
  }, 1000);
  timer.unref?.();
  // Battement de coeur : une connexion silencieuse (telephone sorti du reseau) est coupee, sa place passe a l'IA/dormante
  // et peut etre reprise ensuite. Les clients navigateur repondent aux pings WebSocket automatiquement.
  const beat = setInterval(() => {
    for (const [ws, c] of clients) {
      if (!c.alive) { try { ws.terminate(); } catch { /* deja fermee */ } continue; }
      c.alive = false;
      try { ws.ping(); } catch { /* ignore */ }
    }
    const now = Date.now();
    for (const [ip, at] of joinRate) if (now - at > 60000) joinRate.delete(ip);
    for (const [ip, at] of ideasRate) if (now - at > 600000) ideasRate.delete(ip);
  }, opts.beatMs ?? 20000);
  beat.unref?.();

  return { server, wss, rooms, pushCircles: push.circles, close: () => new Promise((r) => { clearInterval(timer); clearInterval(beat); for (const w of wss.clients) w.terminate(); wss.close(() => server.close(() => r())); }) };
}

// Lancement direct.
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  // Une erreur imprevue dans un salon ne doit pas couper tous les autres : on la journalise et le relais continue.
  process.on('uncaughtException', (e) => console.error('exception non rattrapee :', e && e.stack || e));
  process.on('unhandledRejection', (e) => console.error('promesse rejetee :', e && e.stack || e));
  const port = Number(process.env.PORT || 8787);
  createServer().server.listen(port, () => console.log(`AI Fighter 2 : serveur en ligne sur le port ${port}`));
}
