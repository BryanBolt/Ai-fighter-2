// Service worker d'AI Fighter 2 : le jeu s'installe comme une appli (raccourci web) et reste TOUJOURS a jour.
// Strategie : reseau d'abord (version la plus recente), cache seulement en secours hors ligne ; jamais de version perimee.
const CACHE = 'afh-shell';
self.addEventListener('install', (e) => { self.skipWaiting(); });
self.addEventListener('activate', (e) => { e.waitUntil(self.clients.claim()); });
self.addEventListener('fetch', (e) => {
  const req = e.request;
  if (req.method !== 'GET' || new URL(req.url).origin !== location.origin) return;
  const path = new URL(req.url).pathname;
  if (path.startsWith('/propositions/') || path === '/health' || path.startsWith('/api')) return; // pas de cache pour les pages de travail et l'API
  e.respondWith(
    fetch(req).then((res) => {
      if (res && res.ok) { const copy = res.clone(); caches.open(CACHE).then((c) => c.put(req, copy)); }
      return res;
    }).catch(() => caches.match(req).then((r) => r || caches.match('/index.html')))
  );
});
// Notifications push (invitation d'un ami) : affichage et ouverture du lien d'invitation.
self.addEventListener('push', (e) => {
  let d = {}; try { d = e.data ? e.data.json() : {}; } catch (_) {}
  e.waitUntil(self.registration.showNotification(d.title || 'AI Fighter 2', { body: d.body || 'Un ami t\'invite a jouer !', icon: '/icon-192.png', badge: '/icon-192.png', data: { url: d.url || '/' } }));
});
self.addEventListener('notificationclick', (e) => {
  e.notification.close();
  const url = (e.notification.data && e.notification.data.url) || '/';
  e.waitUntil(self.clients.matchAll({ type: 'window' }).then((l) => { for (const c of l) { if ('focus' in c) { c.navigate(url); return c.focus(); } } return self.clients.openWindow(url); }));
});
