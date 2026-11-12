// Amis qui se SUIVENT (sans compte) : identite anonyme stable (`pid` + secret `sec`), presence RECIPROQUE seulement, invitations directes.
// Tout est en memoire (aucune ecriture disque, aucune donnee personnelle) : une liaison pid -> hash(secret) vit tant que la personne est
// connectee, plus FREE_MS apres sa derniere activite ; l'etat d'un joueur en salon est LU dans le salon (jamais declare par le client).
// Reciprocite sans graphe stocke : A voit B seulement si la derniere liste `fsub` de B contient A. Sinon : etat « inconnu », rien d'autre.
import crypto from 'node:crypto';

const PID_RE = /^[0-9a-f]{16}$/;
const SEC_RE = /^[0-9a-f]{32}$/;
export const LIM = { subMax: 50, lobbyTtlMs: 40000, freeMs: 10 * 60 * 1000, subGapMs: 1500, inviteGapMs: 8000, askMax: 10, bindMax: 20000 };

export function createFriends({ clients, humans, send, now = Date.now, cleanName }) {
  const bind = new Map(); // pid -> { h, last, subs:Set(pid), vis, name }
  const byPid = new Map(); // pid -> Set(ws)
  const lastInvite = new Map(); // from|to -> t
  const hash = (s) => crypto.createHash('sha256').update('afh-friends:' + s).digest('hex');
  const live = (pid) => [...(byPid.get(pid) || [])].filter((w) => w.readyState === 1);

  function hello(ws, c, m) {
    const pid = String(m.pid ?? ''), sec = String(m.sec ?? '');
    if (!PID_RE.test(pid) || !SEC_RE.test(sec)) return send(ws, { t: 'fhello_ack', ok: false, code: 'bad' });
    const t = now(), h = hash(sec), cur = bind.get(pid);
    const fresh = cur && (live(pid).length > 0 || t - cur.last < LIM.freeMs);
    if (fresh && cur.h !== h) return send(ws, { t: 'fhello_ack', ok: false, code: 'taken' }); // usurpation : refusee
    if (!cur && bind.size >= LIM.bindMax) {
      for (const [k, b] of bind) if (t - b.last > LIM.freeMs && !live(k).length) bind.delete(k);
      if (bind.size >= LIM.bindMax) return send(ws, { t: 'fhello_ack', ok: false, code: 'full' });
    }
    const b = fresh ? cur : { h, subs: new Set(), vis: true, name: '', last: t };
    b.h = h; b.last = t; b.vis = m.vis !== false; b.name = cleanName(m.name ?? c.name);
    bind.set(pid, b);
    if (c.fpid && c.fpid !== pid) unbind(ws, c);
    c.fpid = pid;
    if (!byPid.has(pid)) byPid.set(pid, new Set());
    byPid.get(pid).add(ws);
    return send(ws, { t: 'fhello_ack', ok: true });
  }
  function unbind(ws, c) {
    const s = c.fpid && byPid.get(c.fpid);
    if (s) { s.delete(ws); if (!s.size) byPid.delete(c.fpid); }
    c.fpid = null;
  }
  // Etat visible d'un pid : { st: 'off'|'on'|'room', ... } ; 'off' si masque (« apparaitre hors ligne »).
  function stateOf(pid) {
    const b = bind.get(pid); const t = now();
    if (!b || !b.vis) return { st: 'off' };
    let lobby = false;
    for (const w of live(pid)) {
      const c = clients.get(w); if (!c) continue;
      const room = c.room;
      if (room) {
        const hs = humans(room);
        const size = room.started ? Math.max(room.config?.size || 0, room.options.size) : room.options.size;
        return { st: 'room', code: room.code, mode: room.mode | 0, n: hs.length, max: size, started: !!room.started, pub: !!room.public, name: b.name };
      }
      lobby = true;
    }
    // Hors partie : la socket de titre est courte (elle se referme apres chaque `fsub`) ; la presence tient donc LIM.lobbyTtlMs apres le dernier battement.
    if ((lobby || !live(pid).length) && t - b.last < LIM.lobbyTtlMs) return { st: 'on', name: b.name };
    return { st: 'off' };
  }
  // `fsub` : rafraichit la presence (« en ligne hors partie ») et la liste voulue ; repond avec l'etat des amis RECIPROQUES + les demandes entrantes.
  function sub(ws, c, m) {
    const me = c.fpid;
    if (!me || !bind.has(me)) return send(ws, { t: 'error', msg: 'identite requise', code: 'fid' });
    const t = now(), b = bind.get(me);
    if (c.fsubAt && t - c.fsubAt < LIM.subGapMs) return; // limite de debit
    c.fsubAt = t; b.last = t;
    b.subs = new Set((Array.isArray(m.pids) ? m.pids : []).slice(0, LIM.subMax).map(String).filter((p) => PID_RE.test(p) && p !== me));
    const friends = [];
    for (const p of b.subs) {
      const o = bind.get(p);
      if (o && o.subs.has(me)) friends.push({ pid: p, mutual: true, ...stateOf(p) });
      else friends.push({ pid: p, mutual: false, st: 'unk' });
    }
    // demandes entrantes : ceux qui m'ont dans leur liste (et en ligne) sans que je les aie dans la mienne
    const ask = [];
    for (const [p, o] of bind) {
      if (ask.length >= LIM.askMax) break;
      if (o.subs.has(me) && !b.subs.has(p) && live(p).length && o.vis) ask.push({ pid: p, name: o.name });
    }
    return send(ws, { t: 'fstate', friends, ask });
  }
  // Invitation directe a un ami reciproque en ligne : l'etat du salon est lu cote serveur (le client ne peut pas mentir sur le code).
  function invite(ws, c, m) {
    const me = c.fpid, to = String(m.pid ?? '');
    if (!me || !PID_RE.test(to)) return send(ws, { t: 'finvite_ack', ok: false, code: 'bad' });
    const o = bind.get(to), b = bind.get(me);
    if (!o || !b || !o.subs.has(me) || !b.subs.has(to)) return send(ws, { t: 'finvite_ack', ok: false, code: 'notfriend', pid: to });
    const mine = stateOf(me);
    if (mine.st !== 'room') return send(ws, { t: 'finvite_ack', ok: false, code: 'noroom', pid: to });
    const t = now(), k = me + '|' + to;
    if (t - (lastInvite.get(k) || 0) < LIM.inviteGapMs) return send(ws, { t: 'finvite_ack', ok: false, code: 'rate', pid: to });
    const targets = live(to);
    if (!targets.length || !o.vis) return send(ws, { t: 'finvite_ack', ok: false, code: 'offline', pid: to });
    lastInvite.set(k, t);
    if (lastInvite.size > 5000) lastInvite.clear();
    for (const w of targets) send(w, { t: 'finvite', from: me, name: b.name, code: mine.code, mode: mine.mode, n: mine.n, max: mine.max });
    return send(ws, { t: 'finvite_ack', ok: true, pid: to });
  }
  function leave(ws, c) { unbind(ws, c); }
  // `fbye` : depart volontaire (le joueur ferme l'ecran des amis / quitte le jeu) : « hors ligne » tout de suite, sans attendre le TTL.
  function bye(ws, c) { const b = c.fpid && bind.get(c.fpid); if (b) { b.last = 0; b.subs = new Set(); } }
  return { hello, sub, invite, bye, leave, stateOf, size: () => bind.size };
}
