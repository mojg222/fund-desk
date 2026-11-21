/* 운용데스크 서비스워커: 앱 셸은 캐시 우선(백그라운드 갱신), data/ 는 네트워크 우선 */
const CACHE = 'fund-desk-v1';
const SHELL = ['./', 'index.html', 'manifest.webmanifest', 'icons/icon-192.png', 'icons/icon-512.png', 'icons/apple-touch-icon.png'];
self.addEventListener('install', (e) => { e.waitUntil(caches.open(CACHE).then((c) => c.addAll(SHELL)).then(() => self.skipWaiting())); });
self.addEventListener('activate', (e) => { e.waitUntil(caches.keys().then((ks) => Promise.all(ks.filter((k) => k !== CACHE).map((k) => caches.delete(k)))).then(() => self.clients.claim())); });
const put = (req, res) => { if (res && res.ok) { const c = res.clone(); caches.open(CACHE).then((x) => x.put(req, c)); } return res; };
self.addEventListener('fetch', (e) => {
  const r = e.request; if (r.method !== 'GET') return;
  const u = new URL(r.url);
  if (u.origin === location.origin && u.pathname.includes('/data/')) {
    e.respondWith(fetch(r).then((res) => put(r, res)).catch(() => caches.match(r)));
  } else if (u.origin === location.origin) {
    e.respondWith(caches.match(r).then((m) => { const net = fetch(r).then((res) => put(r, res)).catch(() => m); return m || net; }));
  } else if (/fonts\.(googleapis|gstatic)\.com$/.test(u.hostname)) {
    e.respondWith(caches.match(r).then((m) => m || fetch(r).then((res) => put(r, res)).catch(() => m)));
  }
});
