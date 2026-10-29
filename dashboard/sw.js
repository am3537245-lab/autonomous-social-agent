// Production Service Worker for Sovereign Mobile Control Plane (PWA)
// Resilient offline cache and Web Push Notifications (RFC 8291)
const CACHE_NAME = 'autonomous-agent-v3-fluid-cache';
const STATIC_ASSETS = [
  '/',
  '/manifest.json',
  '/icons/icon-192.png',
  '/icons/icon-512.png'
];

// 1. Install & Cache Shell
self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => {
      return cache.addAll(STATIC_ASSETS);
    }).then(() => self.skipWaiting())
  );
});

// 2. Activate & Clean Old Caches
self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) => {
      return Promise.all(
        keys.filter((key) => key !== CACHE_NAME).map((key) => caches.delete(key))
      );
    }).then(() => self.clients.claim())
  );
});

// 3. Network-First for APIs, Cache-First for Static Shell
self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url);

  // Skip caching for API, WebSockets, or non-GET requests
  if (url.pathname.startsWith('/api/') || url.pathname.startsWith('/ws/') || event.request.method !== 'GET') {
    return;
  }

  event.respondWith(
    caches.match(event.request).then((cachedResponse) => {
      if (cachedResponse) {
        // Fetch background update
        fetch(event.request).then((networkResponse) => {
          if (networkResponse && networkResponse.status === 200) {
            caches.open(CACHE_NAME).then((cache) => cache.put(event.request, networkResponse));
          }
        }).catch(() => {});
        return cachedResponse;
      }
      return fetch(event.request).then((networkResponse) => {
        if (!networkResponse || networkResponse.status !== 200 || networkResponse.type !== 'basic') {
          return networkResponse;
        }
        const responseToCache = networkResponse.clone();
        caches.open(CACHE_NAME).then((cache) => cache.put(event.request, responseToCache));
        return networkResponse;
      });
    })
  );
});

// 4. Web Push Listener (Android Lock Screen, iOS Web Clip, Desktop)
self.addEventListener('push', (event) => {
  let data = {
    title: 'تنبيه من الموجّه الآلي',
    body: 'هناك تفاعل جديد يتطلب اهتمامك.',
    icon: '/icons/icon-192.png',
    badge: '/icons/icon-192.png',
    vibrate: [120, 60, 120, 60, 240],
    data: { url: '/' }
  };

  if (event.data) {
    try {
      data = Object.assign(data, event.data.json());
    } catch (e) {
      data.body = event.data.text();
    }
  }

  const options = {
    body: data.body,
    icon: data.icon || '/icons/icon-192.png',
    badge: data.badge || '/icons/icon-192.png',
    vibrate: data.vibrate || [120, 60, 120, 60, 240],
    tag: 'agent-notification',
    renotify: true,
    data: data.data || { url: '/' },
    actions: data.actions || [
      { action: 'open_dashboard', title: 'فتح لوحة التحكم' },
      { action: 'dismiss', title: 'تجاهل' }
    ]
  };

  event.waitUntil(
    self.registration.showNotification(data.title, options)
  );
});

// 5. Push Notification Click Handler
self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  if (event.action === 'dismiss') {
    return;
  }

  // Handle interactive lock-screen price resolution actions
  if (event.action && event.action.startsWith('set_price:')) {
    event.waitUntil(
      fetch('/api/push/action', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: event.action, data: event.notification.data })
      }).catch((e) => console.error('Push action resolution error:', e))
    );
    return;
  }

  const targetUrl = (event.notification.data && event.notification.data.url) ? event.notification.data.url : '/';

  event.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true }).then((clientList) => {
      for (const client of clientList) {
        if (client.url.includes(targetUrl) && 'focus' in client) {
          return client.focus();
        }
      }
      if (clients.openWindow) {
        return clients.openWindow(targetUrl);
      }
    })
  );
});
