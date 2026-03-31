// ── Revenue Calculation ───────────────────────────────────────────────────────
function calcRevenue(period) {
    const now   = new Date();
    let   start;
    if (period === 'today') {
        start = new Date(now.getFullYear(), now.getMonth(), now.getDate());
    } else {
        start = new Date(now.getFullYear(), now.getMonth(), 1);
    }
    return Object.values(rentalsData)
        .filter(r => (r.status === 'completed' || r.status === 'active') && r.end_time && new Date(r.end_time) >= start)
        .reduce((sum, r) => sum + (Number(r.total_cost) || 0), 0);
}

// ── Revenue Chart ─────────────────────────────────────────────────────────────
let revenueChart = null;

function renderRevenueChart() {
    const canvas = document.getElementById('revenue-chart');
    if (!canvas || typeof Chart === 'undefined') return;

    const labels = [];
    const data   = [];

    for (let i = 6; i >= 0; i--) {
        const d        = new Date();
        d.setDate(d.getDate() - i);
        const dayStart = new Date(d.getFullYear(), d.getMonth(), d.getDate());
        const dayEnd   = new Date(dayStart.getTime() + 86400000);

        labels.push(d.toLocaleDateString('ru-RU', { day: 'numeric', month: 'short' }));

        const rev = Object.values(rentalsData)
            .filter(r => r.end_time)
            .filter(r => { const t = new Date(r.end_time); return t >= dayStart && t < dayEnd; })
            .reduce((sum, r) => sum + (Number(r.total_cost) || 0), 0);
        data.push(rev);
    }

    if (revenueChart) {
        revenueChart.data.labels        = labels;
        revenueChart.data.datasets[0].data = data;
        revenueChart.update();
        return;
    }

    revenueChart = new Chart(canvas, {
        type: 'bar',
        data: {
            labels,
            datasets: [{
                label: 'Доход (лей)',
                data,
                backgroundColor: 'rgba(13,110,253,0.7)',
                borderRadius: 8,
                borderSkipped: false,
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    callbacks: { label: ctx => `${ctx.parsed.y.toLocaleString('ru-RU')} лей` }
                }
            },
            scales: {
                y: { beginAtZero: true, grid: { color: '#f0f0f0' }, ticks: { callback: v => v + ' л' } },
                x: { grid: { display: false } }
            }
        }
    });
}

// ── Live Countdown ────────────────────────────────────────────────────────────
let countdownTimer = null;

function startCountdowns() {
    if (countdownTimer) clearInterval(countdownTimer);
    countdownTimer = setInterval(tickCountdowns, 1000);
    tickCountdowns();
}

function tickCountdowns() {
    document.querySelectorAll('[data-countdown]').forEach(el => {
        const endMs    = parseInt(el.dataset.countdown);
        const remaining = endMs - Date.now();
        if (remaining <= 0) {
            el.textContent = 'Истекло';
            el.classList.add('text-danger', 'blink');
        } else {
            const h = Math.floor(remaining / 3600000);
            const m = Math.floor((remaining % 3600000) / 60000);
            const s = Math.floor((remaining % 60000) / 1000);
            el.textContent = `${h}:${String(m).padStart(2,'0')}:${String(s).padStart(2,'0')}`;
            el.classList.remove('text-danger', 'blink');
            if (remaining < 1800000) el.classList.add('text-warning'); // < 30 min
            if (remaining < 600000)  el.classList.replace('text-warning', 'text-danger'); // < 10 min
        }
    });
}

// ── Dashboard ─────────────────────────────────────────────────────────────────
function loadDashboard() {
    const available = Object.values(consolesData).filter(c => c.status === 'available').length;
    const rented    = Object.values(consolesData).filter(c => c.status === 'rented').length;
    const pending   = Object.values(requestsData).filter(r => r.status === 'pending' || r.status === 'pending_approval').length;
    const today     = calcRevenue('today');
    const month     = calcRevenue('month');

    document.getElementById('stat-available').textContent = available;
    document.getElementById('stat-rented').textContent    = rented;
    document.getElementById('stat-users').textContent     = Object.keys(usersData).length;
    document.getElementById('stat-pending').textContent   = pending;
    document.getElementById('stat-revenue-today').textContent = today.toLocaleString('ru-RU') + ' лей';
    document.getElementById('stat-revenue-month').textContent = month.toLocaleString('ru-RU') + ' лей';

    // ── Active rentals table ──────────────────────────────────────────────────
    const tbody = document.getElementById('active-rentals-body');
    tbody.innerHTML = '';
    const activeRentals = Object.entries(rentalsData).filter(([, r]) => r.status === 'active');

    if (!activeRentals.length) {
        tbody.innerHTML = '<tr><td colspan="5" class="text-center text-muted py-3">Нет активных аренд</td></tr>';
    } else {
        activeRentals.forEach(([id, r]) => {
            const c    = consolesData[r.console_id] || {};
            const u    = usersData[r.user_id] || {};
            const name = u.full_name || u.first_name || r.user_id;

            // Calculate end time for countdown
            const startMs  = r.start_time ? new Date(r.start_time).getTime() : Date.now();
            const hours    = r.selected_hours || r.duration_hours || 1;
            const endMs    = startMs + hours * 3600000;

            tbody.innerHTML += `<tr class="row-active">
                <td>${c.name || '—'}</td>
                <td>${name}</td>
                <td>${timeAgo(r.start_time)}</td>
                <td><span class="countdown" data-countdown="${endMs}">…</span></td>
                <td><button class="btn btn-xs btn-danger btn-sm" onclick="endRental('${id}')">Завершить</button></td>
            </tr>`;
        });
        startCountdowns();
    }

    // ── Pending requests table ────────────────────────────────────────────────
    const rtbody = document.getElementById('pending-requests-body');
    rtbody.innerHTML = '';
    const pendingList = Object.entries(requestsData)
        .filter(([, r]) => r.status === 'pending' || r.status === 'pending_approval');

    if (!pendingList.length) {
        rtbody.innerHTML = '<tr><td colspan="4" class="text-center text-muted py-3">Нет ожидающих заявок</td></tr>';
    } else {
        pendingList.forEach(([id, r]) => {
            const c    = consolesData[r.console_id] || {};
            const u    = usersData[r.user_id] || {};
            const name = u.full_name || u.first_name || r.user_id;
            rtbody.innerHTML += `<tr class="row-pending">
                <td>${name}</td>
                <td>${c.name || '—'}</td>
                <td>${r.total_cost || r.expected_cost || 0} лей</td>
                <td>
                    <button class="btn btn-sm btn-success me-1" onclick="approveRequest('${id}')"><i class="fas fa-check"></i></button>
                    <button class="btn btn-sm btn-danger" onclick="rejectRequest('${id}')"><i class="fas fa-times"></i></button>
                </td>
            </tr>`;
        });
    }

    renderRevenueChart();
}
