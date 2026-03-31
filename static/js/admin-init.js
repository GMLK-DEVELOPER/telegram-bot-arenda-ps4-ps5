// ── Load All Data ─────────────────────────────────────────────────────────────
async function loadAll() {
    const [c, u, r, req, d] = await Promise.all([
        api('GET', '/api/consoles'),
        api('GET', '/api/users'),
        api('GET', '/api/rentals'),
        api('GET', '/api/rental-requests'),
        api('GET', '/api/discounts'),
    ]);

    consolesData  = c.consoles   || {};
    usersData     = u.users      || {};
    rentalsData   = r.rentals    || {};
    requestsData  = req.requests || {};
    discountsData = d.discounts  || {};

    // Update last refresh time
    lastUpdateTime = Date.now();
    updateLastUpdateDisplay();

    // Badge + notification for new requests
    const pendingCount = Object.values(requestsData)
        .filter(r => r.status === 'pending' || r.status === 'pending_approval').length;
    // Update both desktop and mobile badges
    ['requests-badge', 'requests-badge-mobile'].forEach(id => {
        const badge = document.getElementById(id);
        if (!badge) return;
        if (pendingCount > 0) {
            badge.textContent   = pendingCount;
            badge.style.display = 'inline';
        } else {
            badge.style.display = 'none';
        }
    });
    if (pendingCount > previousPendingCount && previousPendingCount > 0) {
        showNotification('Новая заявка на аренду!');
        playNotificationSound();
    }
    previousPendingCount = pendingCount;

    // Refresh visible section
    const active = document.querySelector('.section.active');
    if (active) {
        const name = active.id.replace('section-', '');
        switch(name) {
            case 'dashboard':      loadDashboard();   break;
            case 'rentals':        renderRentals();   break;
            case 'requests':       renderRequests();  break;
            case 'users':          renderUsers();     break;
            case 'consoles':       renderConsoles();  break;
            case 'discounts':      renderDiscounts(); break;
        }
    } else {
        loadDashboard();
    }
}

// ── Lightweight Request Poller ────────────────────────────────────────────────
async function pollRequests() {
    try {
        const req = await api('GET', '/api/rental-requests');
        const data = req.requests || {};
        const pendingCount = Object.values(data)
            .filter(r => r.status === 'pending' || r.status === 'pending_approval').length;

        ['requests-badge', 'requests-badge-mobile'].forEach(id => {
            const badge = document.getElementById(id);
            if (!badge) return;
            if (pendingCount > 0) {
                badge.textContent   = pendingCount;
                badge.style.display = 'inline';
            } else {
                badge.style.display = 'none';
            }
        });

        if (pendingCount > previousPendingCount && previousPendingCount >= 0) {
            showNotification('Новая заявка на аренду!');
            playNotificationSound();
            // Flash the requests badge
            ['requests-badge', 'requests-badge-mobile'].forEach(id => {
                const badge = document.getElementById(id);
                if (badge) {
                    badge.classList.add('badge-pulse');
                    setTimeout(() => badge.classList.remove('badge-pulse'), 3000);
                }
            });
        }
        previousPendingCount = pendingCount;
    } catch (_) {}
}

// ── Online / Offline Indicator ────────────────────────────────────────────────
function updateOnlineStatus() {
    const dot = document.getElementById('online-dot');
    if (!dot) return;
    if (navigator.onLine) {
        dot.className = 'online-dot online';
        dot.title     = 'Сервер онлайн';
    } else {
        dot.className = 'online-dot offline';
        dot.title     = 'Нет соединения';
    }
}

// ── Keyboard Shortcuts ────────────────────────────────────────────────────────
const SECTION_KEYS = ['dashboard','consoles','rentals','requests','users','discounts','ratings','calendar','settings'];

document.addEventListener('keydown', (e) => {
    if (['INPUT','TEXTAREA','SELECT'].includes(e.target.tagName)) return;
    if (e.ctrlKey || e.altKey || e.metaKey) return;

    const num = parseInt(e.key);
    if (num >= 1 && num <= SECTION_KEYS.length) {
        showSection(SECTION_KEYS[num - 1]);
        return;
    }
    if (e.key === 'r' || e.key === 'R') {
        loadAll();
        toast('Данные обновлены', 'info');
    }
});

// ── Logout Confirmation ───────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
    const logoutLink = document.getElementById('logout-link');
    if (logoutLink) {
        logoutLink.addEventListener('click', (e) => {
            e.preventDefault();
            if (confirm('Выйти из панели управления?')) {
                window.location.href = '/logout';
            }
        });
    }

    // Notification permission
    if ('Notification' in window && Notification.permission === 'default') {
        Notification.requestPermission();
    }

    // Online/offline
    updateOnlineStatus();
    window.addEventListener('online',  updateOnlineStatus);
    window.addEventListener('offline', updateOnlineStatus);

    // Init
    loadAll();
    // Poll for new requests every 10 seconds (lightweight)
    setInterval(pollRequests, 10000);
});
