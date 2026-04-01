// ── Shared State ─────────────────────────────────────────────────────────────
let consolesData  = {};
let usersData     = {};
let rentalsData   = {};
let requestsData  = {};
let discountsData = {};
let settingsData  = {};
let previousPendingCount = 0;
let lastUpdateTime = null;

// ── API Helper ───────────────────────────────────────────────────────────────
async function api(method, url, body) {
    const opts = { method, headers: { 'Content-Type': 'application/json' } };
    if (body) opts.body = JSON.stringify(body);
    try {
        const r = await fetch(url, opts);
        return r.json();
    } catch (e) {
        return { status: 'error', message: 'Ошибка сети' };
    }
}

// ── Navigation ───────────────────────────────────────────────────────────────
const SECTIONS = ['dashboard','consoles','rentals','requests','users','discounts','ratings','calendar','settings','telegram-admins','reports','audit'];

function showSection(name) {
    document.querySelectorAll('.section').forEach(s => s.classList.remove('active'));
    document.querySelectorAll('.nav-link[data-section]').forEach(a => a.classList.remove('active'));
    const sec = document.getElementById('section-' + name);
    if (sec) sec.classList.add('active');
    document.querySelectorAll(`[data-section="${name}"]`).forEach(a => a.classList.add('active'));

    // Close mobile offcanvas if open
    const oc = document.getElementById('mobileSidebar');
    if (oc) { const inst = bootstrap.Offcanvas.getInstance(oc); if (inst) inst.hide(); }

    switch(name) {
        case 'dashboard':      loadDashboard();       break;
        case 'consoles':       renderConsoles();      break;
        case 'rentals':        renderRentals();       break;
        case 'requests':       renderRequests();      break;
        case 'users':          renderUsers();         break;
        case 'discounts':      renderDiscounts();     break;
        case 'ratings':        loadRatings();         break;
        case 'calendar':       loadCalendar();        break;
        case 'settings':       loadSettings();        break;
        case 'telegram-admins':loadTelegramAdmins();  break;
        case 'reports':        loadTopClients();      break;
        case 'audit':          loadAuditLogs();       break;
    }
}

// ── Toast ─────────────────────────────────────────────────────────────────────
function toast(msg, type = 'success') {
    const el = document.getElementById('mainToast');
    const title = { success: '✅ Успешно', error: '❌ Ошибка', info: 'ℹ️ Информация' }[type] || '📢';
    el.className = `toast toast-${type}`;
    document.getElementById('toast-title').textContent = title;
    document.getElementById('toast-body').textContent  = msg;
    new bootstrap.Toast(el, { delay: 3500 }).show();
}

// ── Utilities ────────────────────────────────────────────────────────────────
function timeAgo(iso) {
    if (!iso) return '—';
    const diff = Date.now() - new Date(iso).getTime();
    const h = Math.floor(diff / 3600000);
    const m = Math.floor((diff % 3600000) / 60000);
    return h > 0 ? `${h}ч ${m}м` : `${m}м`;
}

function formatDate(iso) {
    if (!iso) return '—';
    try { return new Date(iso).toLocaleString('ru-RU'); } catch { return iso.slice(0, 16); }
}

function formatMoney(n) {
    return Number(n || 0).toLocaleString('ru-RU') + ' лей';
}

function filterTable(inputId, tbodyId) {
    const q = document.getElementById(inputId)?.value?.toLowerCase() || '';
    document.querySelectorAll(`#${tbodyId} tr`).forEach(row => {
        row.style.display = row.textContent.toLowerCase().includes(q) ? '' : 'none';
    });
}

function exportCSV(rows, filename) {
    if (!rows.length) { toast('Нет данных для экспорта', 'error'); return; }
    const headers = Object.keys(rows[0]);
    const lines = [
        headers.join(','),
        ...rows.map(obj => headers.map(h => `"${(obj[h] ?? '').toString().replace(/"/g, '""')}"`).join(','))
    ];
    const blob = new Blob(['\ufeff' + lines.join('\n')], { type: 'text/csv;charset=utf-8' });
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement('a');
    a.href = url; a.download = filename; a.click();
    URL.revokeObjectURL(url);
}

// ── Last Update Display ──────────────────────────────────────────────────────
function updateLastUpdateDisplay() {
    if (!lastUpdateTime) return;
    const t = new Date(lastUpdateTime).toLocaleTimeString('ru-RU');
    const a = document.getElementById('last-update-time');
    const b = document.getElementById('last-update-top');
    if (a) a.textContent = t;
    if (b) b.textContent = t;
}

// ── Web Notifications ────────────────────────────────────────────────────────
function showNotification(message) {
    toast(message, 'info');
    if ('Notification' in window && Notification.permission === 'granted') {
        new Notification('PS4 Аренда', { body: message });
    }
}

function playNotificationSound() {
    try {
        const ctx  = new (window.AudioContext || window.webkitAudioContext)();
        const osc  = ctx.createOscillator();
        const gain = ctx.createGain();
        osc.connect(gain); gain.connect(ctx.destination);
        osc.type = 'sine';
        osc.frequency.setValueAtTime(880, ctx.currentTime);
        osc.frequency.setValueAtTime(660, ctx.currentTime + 0.15);
        gain.gain.setValueAtTime(0.3, ctx.currentTime);
        gain.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + 0.5);
        osc.start(ctx.currentTime);
        osc.stop(ctx.currentTime + 0.5);
    } catch(e) {}
}
