// ── Reports & Analytics ──────────────────────────────────────────────────────

async function generateReport() {
    const startDate = document.getElementById('report-start-date').value;
    const endDate = document.getElementById('report-end-date').value;

    if (!startDate || !endDate) {
        toast('Укажите даты', 'error');
        return;
    }

    try {
        const res = await fetch(`/api/report?start_date=${startDate}&end_date=${endDate}`);
        const data = await res.json();

        const html = `
            <div class="row g-3">
                <div class="col-md-3">
                    <div class="card text-center">
                        <div class="card-body">
                            <h3 class="text-primary">${data.total_rentals}</h3>
                            <small class="text-muted">Всего аренд</small>
                        </div>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="card text-center">
                        <div class="card-body">
                            <h3 class="text-success">${data.completed_rentals}</h3>
                            <small class="text-muted">Завершено</small>
                        </div>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="card text-center">
                        <div class="card-body">
                            <h3 class="text-warning">${(data.total_revenue || 0).toFixed(0)} лей</h3>
                            <small class="text-muted">Доход</small>
                        </div>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="card text-center">
                        <div class="card-body">
                            <h3 class="text-info">${data.new_users}</h3>
                            <small class="text-muted">Новых клиентов</small>
                        </div>
                    </div>
                </div>
            </div>
            <div class="mt-3">
                <p><strong>Топ консоль:</strong> ${data.top_console && data.top_console.name ? data.top_console.name + ' (' + (data.top_console.count || 0) + ' аренд, ' + (data.top_console.revenue || 0).toFixed(0) + ' лей)' : 'Нет данных'}</p>
                <p><strong>Средняя длительность:</strong> ${(data.avg_duration || 0).toFixed(1)} часов</p>
            </div>
        `;

        document.getElementById('report-result').innerHTML = html;
        toast('Отчёт сформирован', 'success');
    } catch (err) {
        toast('Ошибка загрузки отчёта', 'error');
    }
}

async function loadTopClients() {
    // Set default dates if empty
    const startEl = document.getElementById('report-start-date');
    const endEl = document.getElementById('report-end-date');
    if (startEl && !startEl.value) {
        const end = new Date();
        const start = new Date();
        start.setDate(start.getDate() - 7);
        startEl.value = start.toISOString().split('T')[0];
        endEl.value = end.toISOString().split('T')[0];
    }

    try {
        const res = await fetch('/api/top-clients');
        const data = await res.json();
        const tbody = document.getElementById('top-clients-body');

        if (!data.clients || data.clients.length === 0) {
            tbody.innerHTML = '<tr><td colspan="2" class="text-center text-muted">Нет данных</td></tr>';
            return;
        }

        tbody.innerHTML = data.clients.map((c, i) => `
            <tr>
                <td>
                    <span class="badge bg-secondary me-2">${i + 1}</span>
                    ${c.full_name || c.username || c.user_id}
                </td>
                <td><strong>${c.total_spent.toFixed(0)} лей</strong></td>
            </tr>
        `).join('');
    } catch (err) {
        console.error('Error loading top clients:', err);
    }
}

async function sendBroadcast() {
    const filter = document.getElementById('broadcast-filter').value;
    const message = document.getElementById('broadcast-message').value.trim();

    if (!message) {
        toast('Введите сообщение', 'error');
        return;
    }

    if (!confirm(`Отправить сообщение всем пользователям (${filter})?`)) {
        return;
    }

    try {
        const res = await fetch('/api/broadcast', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ message, filter })
        });
        const data = await res.json();

        if (data.status === 'success') {
            toast(`Отправлено ${data.sent} из ${data.total} пользователей`, 'success');
            document.getElementById('broadcast-message').value = '';
        } else {
            toast(data.error || 'Ошибка отправки', 'error');
        }
    } catch (err) {
        toast('Ошибка отправки', 'error');
    }
}

async function loadAuditLogs() {
    try {
        const res = await fetch('/api/audit-logs?limit=50');
        const data = await res.json();
        const tbody = document.getElementById('audit-logs-body');

        if (!data.logs || data.logs.length === 0) {
            tbody.innerHTML = '<tr><td colspan="5" class="text-center text-muted">Нет логов</td></tr>';
            return;
        }

        tbody.innerHTML = data.logs.map(log => {
            const date = new Date(log.CreatedAt);
            return `
                <tr>
                    <td><small>${date.toLocaleString('ru-RU')}</small></td>
                    <td><span class="badge bg-secondary">${log.AdminUser}</span></td>
                    <td>${log.Action}</td>
                    <td><small>${log.Target}</small></td>
                    <td><small>${log.IPAddress}</small></td>
                </tr>
            `;
        }).join('');
    } catch (err) {
        console.error('Error loading audit logs:', err);
    }
}

