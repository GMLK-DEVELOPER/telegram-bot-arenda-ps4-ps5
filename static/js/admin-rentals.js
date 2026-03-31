function renderRentals() {
    const q     = document.getElementById('search-rentals')?.value?.toLowerCase() || '';
    const tbody = document.getElementById('rentals-table-body');
    tbody.innerHTML = '';

    const entries = Object.entries(rentalsData)
        .sort((a, b) => new Date(b[1].start_time || 0) - new Date(a[1].start_time || 0));

    entries.forEach(([id, r]) => {
        const c    = consolesData[r.console_id] || {};
        const u    = usersData[r.user_id] || {};
        const name = u.full_name || u.first_name || r.user_id;

        if (q && !`${name} ${c.name || ''} ${id}`.toLowerCase().includes(q)) return;

        const isActive = r.status === 'active';
        const status = isActive
            ? '<span class="badge bg-success">Активна</span>'
            : '<span class="badge bg-secondary">Завершена</span>';
        const action = isActive
            ? `<button class="btn btn-sm btn-danger" onclick="endRental('${id}')"><i class="fas fa-stop"></i> Завершить</button>`
            : '<span class="text-muted">—</span>';

        tbody.innerHTML += `<tr class="${isActive ? 'row-active' : 'row-ended'}">
            <td><code class="text-muted" style="font-size:.8em">${id.slice(0,8)}</code></td>
            <td>${name}</td>
            <td>${c.name || '—'}</td>
            <td>${formatDate(r.start_time)}</td>
            <td>${r.end_time ? formatDate(r.end_time) : '—'}</td>
            <td class="fw-bold">${r.total_cost || 0} лей</td>
            <td>${status}</td>
            <td>${action}</td>
        </tr>`;
    });

    if (!tbody.innerHTML) {
        tbody.innerHTML = '<tr><td colspan="8" class="text-center text-muted py-3">Нет аренд</td></tr>';
    }
}

async function endRental(id) {
    if (!confirm('Завершить аренду?')) return;
    const res = await api('POST', '/api/rentals/end', { rental_id: id });
    if (res.status === 'success') {
        rentalsData[id].status     = 'completed';
        rentalsData[id].total_cost = res.total_cost;
        rentalsData[id].end_time   = new Date().toISOString();
        if (rentalsData[id].console_id && consolesData[rentalsData[id].console_id]) {
            consolesData[rentalsData[id].console_id].status = 'available';
        }
        renderRentals(); loadDashboard();
        toast(`Аренда завершена. Стоимость: ${res.total_cost} лей`);
    } else toast(res.message, 'error');
}

function exportRentalsCSV() {
    const rows = Object.entries(rentalsData).map(([id, r]) => {
        const c    = consolesData[r.console_id] || {};
        const u    = usersData[r.user_id] || {};
        const name = u.full_name || u.first_name || r.user_id;
        return {
            'ID':         id.slice(0,8),
            'Пользователь': name,
            'Телефон':    u.phone_number || '',
            'Консоль':    c.name || '',
            'Начало':     (r.start_time || '').slice(0,16),
            'Конец':      (r.end_time || '').slice(0,16),
            'Стоимость':  r.total_cost || 0,
            'Статус':     r.status,
        };
    });
    const d = new Date().toISOString().slice(0,10);
    exportCSV(rows, `rentals_${d}.csv`);
}
