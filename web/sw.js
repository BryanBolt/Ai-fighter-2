// Service worker d'AI Fighter 2 : le jeu s'installe comme une appli (raccourci web) et reste TOUJOURS a jour.
// Strategie : reseau d'abord (version la plus recente), cache seulement en secours hors ligne ; jamais de version perimee
// tant que le reseau repond. Hors ligne : la derniere copie vue ; une page (navigation) retombe sur index.html, mais une
// ressource manquante (wasm, son) reste une vraie erreur : lui servir du HTML faisait planter l'instanciation du wasm.
const CACHE = 'afh-shell-v8';
// Pre-cache du jeu a l'installation (hors ligne des la 1re visite) : au mieux, fichier par fichier (un echec ne bloque jamais
// l'installation ; le wasm de ~12 Mo ne retient pas le demarrage : la page le telecharge deja en parallele).
// Mise a jour sans casser une partie : skipWaiting + claim ne rechargent JAMAIS la page ouverte ; la nouvelle version n'est
// utilisee qu'a la prochaine ouverture ou au prochain rechargement.
const CORE = ['/', '/index.html', '/mq_js_bundle.js', '/manifest.webmanifest', '/icon-192.png', '/icon-512.png', '/icon-maskable-512.png', '/favicon.svg', '/apple-touch-icon.png'];
// Le wasm (~12 Mo) et la musique du titre ne sont PAS pre-caches a l'installation : la page les telecharge deja, les prendre deux fois
// doublait les octets de la 1re visite (lenteur sur mobile). La page envoie {t:'warm'} une fois le wasm charge : copie alors depuis le
// cache HTTP (revalidation 304), sans 2e telechargement.
const WARM = ['/ai_fighter_2.wasm', '/music/final_escalation_title.ogg'];
self.addEventListener('message', (e) => {
  if (!e.data || e.data.t !== 'warm') return;
  e.waitUntil((async () => { try { const c = await caches.open(CACHE); for (const u of WARM) { try { const r = await fetch(u); if (cacheable(r)) await c.put(u, r); } catch (_) {} } } catch (_) {} })());
});
self.addEventListener('install', (e) => {
  self.skipWaiting();
  e.waitUntil((async () => {
    try { const c = await caches.open(CACHE); await Promise.all(CORE.map((u) => c.add(new Request(u, { cache: 'reload' })).catch(() => {}))); } catch (_) {}
  })());
});
self.addEventListener('activate', (e) => {
  e.waitUntil((async () => {
    // Anciennes versions du cache (nom change) : supprimees, sinon elles occupent le quota du telephone pour rien.
    try { const keys = await caches.keys(); await Promise.all(keys.filter((k) => k !== CACHE && k.startsWith('afh-')).map((k) => caches.delete(k))); } catch (_) {}
    await self.clients.claim();
  })());
});
// Une reponse n'est gardee que si elle est complete (200) : un 206 partiel (Range) ou une reponse opaque feraient
// echouer cache.put, ou pire, serviraient plus tard un morceau de fichier.
function cacheable(res) { return res && res.status === 200 && res.type === 'basic'; }
async function wasmCached(req) {
  try {
    const old = await caches.match(req, { ignoreSearch: true });
    const tag = old && old.headers.get('etag');
    if (!tag) return null;
    const ctl = new AbortController(); const t = setTimeout(() => ctl.abort(), 6000);
    try {
      const res = await fetch(req.url, { headers: { 'If-None-Match': tag }, cache: 'no-cache', signal: ctl.signal });
      clearTimeout(t);
      if (res.status === 304) return old;
      if (cacheable(res)) { const copy = res.clone(); caches.open(CACHE).then((c) => c.put(req, copy)).catch(() => {}); return res; }
      return old;
    } catch (_) { clearTimeout(t); return old; }
  } catch (_) { return null; }
}
self.addEventListener('fetch', (e) => {
  const req = e.request;
  if (req.method !== 'GET' || req.headers.has('range')) return;
  let url;
  try { url = new URL(req.url); } catch (_) { return; }
  if (url.origin !== location.origin) return;
  const path = url.pathname;
  if (path.startsWith('/propositions/') || path === '/health' || path.startsWith('/api') || path.startsWith('/push')) return; // pas de cache pour les pages de travail et l'API
  e.respondWith((async () => {
    // Wasm deja en cache : validation par ETag (requete conditionnelle de quelques octets, 304) puis copie locale. Sur 4G ou page
    // en arriere-plan, plus de re-telechargement de 6,7 Mo ; la copie venant du Cache API garde aussi le code compile de V8.
    if (path.endsWith('.wasm')) { const w = await wasmCached(req); if (w) return w; }
    try {
      const res = await fetch(req);
      // 5xx (Render qui se reveille : 502/503/504) : si on a une copie, on la sert plutot que l'erreur ; sinon l'erreur passe (la page reessaie).
      if (res.status >= 500) {
        const old = await caches.match(req, { ignoreSearch: true });
        if (old) return old;
        if (req.mode === 'navigate') { const sh = await caches.match('/index.html') || await caches.match('/'); if (sh) return sh; }
        return res;
      }
      if (cacheable(res)) {
        const copy = res.clone();
        e.waitUntil(caches.open(CACHE).then((c) => c.put(req, copy)).catch(() => {}));
      }
      return res;
    } catch (err) {
      const hit = await caches.match(req, { ignoreSearch: true });
      if (hit) return hit;
      if (req.mode === 'navigate') {
        const shell = await caches.match('/index.html') || await caches.match('/');
        if (shell) return shell;
      }
      return Response.error();
    }
  })());
});
// Notifications push (invitation d'un ami) : affichage et ouverture du lien d'invitation.
self.addEventListener('push', (e) => {
  let d = {}; try { d = e.data ? e.data.json() : {}; } catch (_) {}
  const title = typeof d.title === 'string' ? d.title.slice(0, 80) : 'AI Fighter 2';
  const body = typeof d.body === 'string' ? d.body.slice(0, 200) : 'Un ami t\'invite a jouer !';
  // Seul un lien du meme site est ouvert (un push ne doit jamais emmener ailleurs).
  let target = '/';
  try { const u = new URL(typeof d.url === 'string' ? d.url : '/', location.origin); if (u.origin === location.origin) target = u.pathname + u.search; } catch (_) {}
  e.waitUntil(self.registration.showNotification(title, { body, icon: '/icon-192.png', badge: '/icon-192.png', data: { url: target } }));
});
self.addEventListener('notificationclick', (e) => {
  e.notification.close();
  const url = (e.notification.data && e.notification.data.url) || '/';
  e.waitUntil(self.clients.matchAll({ type: 'window' }).then((l) => { for (const c of l) { if ('focus' in c) { try { c.navigate(url); } catch (_) {} return c.focus(); } } return self.clients.openWindow(url); }));
});
