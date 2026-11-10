/* Synchronisation des verdicts (Valider / A refaire / commentaire) vers le serveur. Inclus par TOUTES les pages de propositions
   (tools/inject_verdicts.py). Aucun changement du JS des pages : on observe localStorage.setItem. Sans cle (saisie une fois dans
   decisions.html), rien n'est envoye et les pages restent sur localStorage seul. Voir tools/README_VERDICTS.md. */
(function () {
  'use strict';
  if (window.AFV) return;
  var KEY = 'afv_key', BASE = 'afv_base', QUEUE = 'afv_queue', SEEN = 'afv_seen';
  var ls = null, rawSet = null;
  try { ls = window.localStorage; rawSet = Storage.prototype.setItem; } catch (e) { return; }
  function get(k) { try { return ls.getItem(k); } catch (e) { return null; } }
  function put(k, v) { try { rawSet.call(ls, k, v); } catch (e) {} }
  function json(k, d) { try { var v = JSON.parse(get(k) || ''); return v && typeof v === 'object' ? v : d; } catch (e) { return d; } }
  var script = document.currentScript, hinted = ((script && script.getAttribute('data-keys')) || '').split(',').filter(Boolean);
  var page = (location.pathname.split('/').pop() || 'index').replace(/\.html?$/i, '').replace(/[^A-Za-z0-9_.-]/g, '_').slice(0, 64) || 'index';
  var queue = json(QUEUE, []), seen = json(SEEN, {}), status = 'off', busy = false, timer = null, badge = null;
  if (!Array.isArray(queue)) queue = [];

  function norm(v) {
    if (v === 1 || v === true || v === 'ok' || v === 'yes') return 'ok';
    if (v === -1 || v === 'ko' || v === 'no' || v === 'redo' || v === 'refaire') return 'refaire';
    if (v === 'mid' || v === 'discuter') return 'discuter';
    return '';
  }
  var FIELDS = { v: 1, c: 1, t: 1, s: 1, verdict: 1, comment: 1, commentaire: 1, note: 1 };
  // Une page stocke { idCarte: { v|s|verdict: ..., c|t|comment: ... } } : on reconnait cette forme, rien d'autre.
  function entries(raw) {
    var o; try { o = JSON.parse(raw); } catch (e) { return null; }
    if (!o || typeof o !== 'object' || Array.isArray(o)) return null;
    var out = [], ids = Object.keys(o);
    for (var i = 0; i < ids.length; i++) {
      var e = o[ids[i]];
      if (!e || typeof e !== 'object' || Array.isArray(e)) return null;
      var ks = Object.keys(e), hit = false;
      for (var j = 0; j < ks.length; j++) { if (!FIELDS[ks[j]]) return null; hit = true; }
      if (!hit) continue;
      var v = norm(e.v !== undefined ? e.v : e.s !== undefined ? e.s : e.verdict);
      var c = String(e.c != null ? e.c : e.t != null ? e.t : e.comment != null ? e.comment : e.commentaire != null ? e.commentaire : '');
      out.push({ carte: ids[i], verdict: v, commentaire: c });
    }
    return out.length || ids.length === 0 ? out : null;
  }
  function scan(storageKey, raw, force) {
    if (storageKey.indexOf('afv_') === 0) return;
    var list = entries(raw); if (!list) return;
    var changed = false;
    list.forEach(function (e) {
      var id = page + '|' + storageKey + '|' + e.carte, sig = e.verdict + '\u0001' + e.commentaire;
      if (seen[id] === sig) return;
      if (!e.verdict && !e.commentaire && seen[id] === undefined) return; // vide et jamais envoye : rien a dire
      queue = queue.filter(function (q) { return q.id !== id; });
      queue.push({ id: id, page: page, carte: e.carte, verdict: e.verdict, commentaire: e.commentaire, version: storageKey, at: Date.now(), sig: sig });
      changed = true;
    });
    if (changed) { put(QUEUE, JSON.stringify(queue)); schedule(force ? 50 : 800); }
  }

  function base() { return (get(BASE) || '').replace(/\/+$/, ''); }
  function api(method, path, body) {
    return fetch(base() + path, { method: method, headers: { 'content-type': 'application/json', 'x-verdict-key': get(KEY) || '' }, body: body ? JSON.stringify(body) : undefined, cache: 'no-store' });
  }
  function setStatus(s) { status = s; paint(); try { window.dispatchEvent(new CustomEvent('afv-status', { detail: s })); } catch (e) {} }
  function schedule(ms) { if (timer) clearTimeout(timer); timer = setTimeout(flush, ms); }
  function flush() {
    timer = null;
    if (!get(KEY)) { setStatus('off'); return; }
    if (busy) return;
    if (!queue.length) { setStatus('ok'); return; }
    if (navigator.onLine === false) { setStatus('offline'); return; }
    busy = true; setStatus('pending');
    var q = queue[0];
    api('PUT', '/api/verdict', { page: q.page, carte: q.carte, verdict: q.verdict, commentaire: q.commentaire, version: q.version, at: q.at }).then(function (r) {
      busy = false;
      if (r.status === 401) { setStatus('badkey'); return; }
      if (r.status === 503) { setStatus('off'); return; }
      if (r.status === 429) { setStatus('pending'); schedule(30000); return; }
      if (r.status >= 500) { setStatus('offline'); schedule(20000); return; }
      // 200 = pris ; autre 4xx = refuse pour de bon : on retire de la file (sinon elle resterait bloquee).
      queue.shift(); seen[q.id] = q.sig; put(QUEUE, JSON.stringify(queue)); put(SEEN, JSON.stringify(seen));
      flush();
    }).catch(function () { busy = false; setStatus('offline'); schedule(20000); });
  }

  function paint() {
    if (!document.body) return;
    if (status === 'off') { if (badge) badge.style.display = 'none'; return; }
    if (!badge) {
      badge = document.createElement('div');
      badge.style.cssText = 'position:fixed;left:8px;bottom:8px;z-index:99999;font:11px/1.2 Silkscreen,monospace,sans-serif;padding:4px 8px;border-radius:8px;border:2px solid #221834;pointer-events:none;opacity:.92';
      badge.setAttribute('aria-live', 'polite');
      document.body.appendChild(badge);
    }
    var t = { ok: 'synchronise', pending: 'envoi... (' + queue.length + ')', offline: 'hors ligne (' + queue.length + ' en attente)', badkey: 'cle refusee' }[status] || '';
    badge.style.display = ''; badge.textContent = t;
    badge.style.background = status === 'ok' ? '#fff3e0' : status === 'badkey' ? '#d8456b' : '#221834';
    badge.style.color = status === 'ok' ? '#221834' : '#fff3e0';
  }

  // Observation des ecritures de localStorage : aucune modification du JS des pages.
  try {
    Storage.prototype.setItem = function (k, v) {
      rawSet.apply(this, arguments);
      try { if (this === ls) scan(String(k), String(v), false); } catch (e) {}
    };
  } catch (e) {}

  function init() {
    hinted.forEach(function (k) { var raw = get(k); if (raw) scan(k, raw, true); });
    if (get(KEY)) schedule(300); else setStatus('off');
  }
  window.AFV = {
    getKey: function () { return get(KEY) || ''; },
    setKey: function (k) { k = String(k || '').trim(); if (k) put(KEY, k); else { try { ls.removeItem(KEY); } catch (e) {} } seen = {}; put(SEEN, '{}'); init(); },
    getBase: base,
    setBase: function (b) { put(BASE, String(b || '').trim()); },
    status: function () { return status; },
    pending: function () { return queue.length; },
    // Tous les verdicts du serveur (rejet si cle absente ou refusee).
    fetchAll: function () {
      if (!get(KEY)) return Promise.reject(new Error('cle absente'));
      return api('GET', '/api/verdicts').then(function (r) { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); });
    },
    // Re-envoie les verdicts locaux connus de la page.
    resync: function () { seen = {}; put(SEEN, '{}'); init(); },
    norm: norm, entries: entries
  };
  window.addEventListener('online', function () { schedule(200); });
  setInterval(function () { if (queue.length && get(KEY)) flush(); }, 30000);
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', function () { paint(); init(); }); else init();
})();
