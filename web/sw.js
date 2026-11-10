// Service worker d'AI Fighter 2 : le jeu s'installe comme une appli (raccourci web) et reste TOUJOURS a jour.
// Strategie : reseau d'abord (version la plus recente), cache seulement en secours hors ligne ; jamais de version perimee
// tant que le reseau repond. Hors ligne : la derniere copie vue ; une page (navigation) retombe sur index.html, mais une
// ressource manquante (wasm, son) reste une vraie erreur : lui servir du HTML faisait planter l'instanciation du wasm.
const CACHE = 'afh-shell-v3';
// Pre-cache du jeu a l'installation (hors ligne des la 1re visite) : au mieux, fichier par fichier (un echec ne bloque jamais
// l'installation ; le wasm de ~12 Mo ne retient pas le demarrage : la page le telecharge deja en parallele).
// Mise a jour sans casser une partie : skipWaiting + claim ne rechargent JAMAIS la page ouverte ; la nouvelle version n'est
// utilisee qu'a la prochaine ouverture ou au prochain rechargement.
const CORE = ['/', '/index.html', '/mq_js_bundle.js', '/manifest.webmanifest', '/icon-192.png', '/icon-512.png', '/icon-maskable-512.png', '/ai_fighter_2.wasm', '/music/final_escalation_title.ogg', '/music/level_1.ogg'];
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
self.addEventListener('fetch', (e) => {
  const req = e.request;
  if (req.method !== 'GET' || req.headers.has('range')) return;
  let url;
  try { url = new URL(req.url); } catch (_) { return; }
  if (url.origin !== location.origin) return;
  const path = url.pathname;
  if (path.startsWith('/propositions/') || path === '/health' || path.startsWith('/api') || path.startsWith('/push')) return; // pas de cache pour les pages de travail et l'API
  e.respondWith((async () => {
    try {
      const res = await fetch(req);
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
