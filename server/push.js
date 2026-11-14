// Notifications push d'invitation (Web Push). Sans compte : un « cercle » = code court partage entre amis.
// Rien de sensible n'est garde : seulement l'adresse d'abonnement du navigateur (en memoire) et un pseudo.
// Sans cles VAPID (VAPID_PUBLIC / VAPID_PRIVATE / VAPID_SUBJECT), les routes repondent « push indisponible » et le jeu continue.
import * as webpush from 'web-push';
import { dropBody } from './overflow.js';

const CIRCLE_RE = /^[A-Z0-9]{5,8}$/;
const MAX_CIRCLES = 2000;
const MAX_PER_CIRCLE = 12;
const MAX_BODY = 4096;
const INVITE_GAP_MS = 30000;
const SUB_GAP_MS = 2000;
const cleanText = (v, max) => [...String(v ?? '')].filter((c) => { const k = c.charCodeAt(0); return k >= 32 && k !== 127 && !(k >= 128 && k <= 159) && !(k >= 0x200b && k <= 0x200f) && !(k >= 0x2028 && k <= 0x202e); }).join('').split(' ').filter(Boolean).join(' ').slice(0, max);

// Abonnement valide : adresse https bornee + cles p256dh / auth en base64url.
export function cleanSubscription(s) {
  if (!s || typeof s !== 'object' || Array.isArray(s)) return null;
  const ep = typeof s.endpoint === 'string' ? s.endpoint : '';
  if (ep.length < 12 || ep.length > 600) return null;
  let u; try { u = new URL(ep); } catch { return null; }
  if (u.protocol !== 'https:') return null;
  const k = s.keys;
  const b64 = /^[A-Za-z0-9_-]{8,200}={0,2}$/;
  if (!k || typeof k.p256dh !== 'string' || typeof k.auth !== 'string' || !b64.test(k.p256dh) || !b64.test(k.auth)) return null;
  return { endpoint: ep, keys: { p256dh: k.p256dh, auth: k.auth } };
}

// sender(subscription, payloadString) -> Promise : injectable (tests) ; par defaut web-push.
export function createPush({ env = process.env, sender = null, roomExists = () => true, now = () => Date.now() } = {}) {
  const pub = env.VAPID_PUBLIC, priv = env.VAPID_PRIVATE, subject = env.VAPID_SUBJECT;
  let enabled = !!(sender || (pub && priv && subject));
  let send = sender;
  if (!sender && enabled) {
    try {
      webpush.setVapidDetails(subject, pub, priv);
      send = (sub, payload) => webpush.sendNotification(sub, payload, { TTL: 120, urgency: 'high' });
    } catch { enabled = false; }
  }
  const circles = new Map(); // cercle -> Map(endpoint -> { sub, name, at })
  const lastInvite = new Map(); // cercle -> date
  const lastSub = new Map(); // ip -> date

  const reply = (res, code, obj) => { res.writeHead(code, { 'content-type': 'application/json', 'cache-control': 'no-store' }); res.end(JSON.stringify(obj)); };
  function readJson(req, res, cb) {
    let body = '', dead = false;
    req.on('data', (d) => { body += d; if (body.length > MAX_BODY && !dead) { dead = true; reply(res, 413, { ok: false, err: 'trop gros' }); dropBody(req); } });
    req.on('end', () => {
      if (dead) return;
      let j; try { j = JSON.parse(body); } catch { return reply(res, 400, { ok: false, err: 'json invalide' }); }
      if (!j || typeof j !== 'object' || Array.isArray(j)) return reply(res, 400, { ok: false, err: 'json invalide' });
      cb(j);
    });
  }
  const unavailable = (res) => { reply(res, 503, { ok: false, err: 'push indisponible' }); return true; };

  // Retourne true si la route a ete traitee.
  function handle(req, res, pathname) {
    if (!pathname.startsWith('/push/')) return false;
    if (pathname === '/push/key' && req.method === 'GET') {
      if (!enabled) return unavailable(res);
      reply(res, 200, { ok: true, key: pub || '' }); return true;
    }
    if (pathname === '/push/subscribe' && req.method === 'POST') {
      if (!enabled) return unavailable(res);
      const ip = req.socket.remoteAddress || '?';
      const t = now();
      if (t - (lastSub.get(ip) || 0) < SUB_GAP_MS) { reply(res, 429, { ok: false, err: 'trop vite' }); return true; }
      lastSub.set(ip, t);
      if (lastSub.size > 5000) lastSub.clear();
      readJson(req, res, (j) => {
        const circle = String(j.circle ?? '').toUpperCase();
        const sub = cleanSubscription(j.subscription);
        if (!CIRCLE_RE.test(circle) || !sub) return reply(res, 400, { ok: false, err: 'entree invalide' });
        let m = circles.get(circle);
        if (!m) {
          if (circles.size >= MAX_CIRCLES) return reply(res, 503, { ok: false, err: 'complet' });
          m = new Map(); circles.set(circle, m);
        }
        if (!m.has(sub.endpoint) && m.size >= MAX_PER_CIRCLE) return reply(res, 409, { ok: false, err: 'cercle plein' });
        m.set(sub.endpoint, { sub, name: cleanText(j.name, 14) || 'Un ami', at: t });
        reply(res, 200, { ok: true, members: m.size });
      });
      return true;
    }
    if (pathname === '/push/unsubscribe' && req.method === 'POST') {
      readJson(req, res, (j) => {
        const circle = String(j.circle ?? '').toUpperCase();
        const m = circles.get(circle);
        if (CIRCLE_RE.test(circle) && m && typeof j.endpoint === 'string') { m.delete(j.endpoint); if (!m.size) circles.delete(circle); }
        reply(res, 200, { ok: true });
      });
      return true;
    }
    if (pathname === '/push/invite' && req.method === 'POST') {
      if (!enabled) return unavailable(res);
      readJson(req, res, async (j) => {
        const circle = String(j.circle ?? '').toUpperCase();
        const code = String(j.code ?? '').toUpperCase();
        if (!CIRCLE_RE.test(circle) || !/^[A-Z0-9]{4,8}$/.test(code)) return reply(res, 400, { ok: false, err: 'entree invalide' });
        if (!roomExists(code)) return reply(res, 404, { ok: false, err: 'partie introuvable' });
        const m = circles.get(circle);
        if (!m) return reply(res, 200, { ok: true, sent: 0 });
        const t = now();
        if (t - (lastInvite.get(circle) || 0) < INVITE_GAP_MS) return reply(res, 429, { ok: false, err: 'trop vite' });
        lastInvite.set(circle, t);
        if (lastInvite.size > 5000) lastInvite.clear();
        const from = typeof j.endpoint === 'string' ? j.endpoint : '';
        const who = cleanText(j.name, 14);
        const payload = JSON.stringify({ title: 'AI Fighter 2', body: (who || 'Un ami') + ' t\'invite a jouer !', url: '/?salon=' + code });
        let sent = 0;
        for (const [ep, e] of [...m]) {
          if (ep === from) continue;
          try { await send(e.sub, payload); sent++; }
          catch (err) { const sc = err && err.statusCode; if (sc === 404 || sc === 410) m.delete(ep); }
        }
        if (!m.size) circles.delete(circle);
        reply(res, 200, { ok: true, sent });
      });
      return true;
    }
    reply(res, 404, { ok: false, err: 'introuvable' });
    return true;
  }
  return { handle, enabled: () => enabled, circles };
}
