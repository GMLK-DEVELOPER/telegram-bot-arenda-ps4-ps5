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

        const isActive   = r.status === 'active';
        const isBooked   = r.status === 'booked';
        const isAwaiting = r.status === 'awaiting_location';
        const status = isActive
            ? '<span class="badge bg-success">▶️ Активна</span>'
            : isBooked
            ? '<span class="badge bg-warning text-dark">📌 Забронировано</span>'
            : isAwaiting
            ? '<span class="badge bg-info text-dark">📍 Ждём геолокацию</span>'
            : '<span class="badge bg-secondary">Завершена</span>';
        const endTime = r.expected_end_time ? `<br><small class="text-muted">До: ${formatDate(r.expected_end_time)}</small>` : '';
        const action = isActive
            ? `<div class="d-flex gap-1">
                <button class="btn btn-sm btn-warning" onclick="extendRental('${id}')" title="Продлить"><i class="fas fa-plus-circle"></i></button>
                <button class="btn btn-sm btn-danger" onclick="endRental('${id}')"><i class="fas fa-stop"></i></button>
               </div>`
            : isBooked
            ? `<button class="btn btn-sm btn-success" onclick="startRental('${id}')"><i class="fas fa-play me-1"></i>Начать аренду</button>`
            : isAwaiting
            ? `<span class="text-info"><i class="fas fa-spinner fa-spin me-1"></i>Ждём геолокацию...</span>`
            : '<span class="text-muted">—</span>';

        const loc = r.location;
        const mapBtn = loc && loc.latitude && loc.longitude
            ? `<button class="btn btn-sm btn-outline-info ms-1" onclick="showUserMap(${loc.latitude},${loc.longitude},'${name}')" title="Посмотреть на карте"><i class="fas fa-map-marker-alt"></i></button>`
            : (isActive
            ? `<button class="btn btn-sm btn-outline-secondary ms-1" onclick="requestLocationFromUser('${id}','${r.user_id}')" title="Запросить геолокацию у клиента"><i class="fas fa-location-arrow"></i></button>`
            : '');

        tbody.innerHTML += `<tr class="${isActive ? 'row-active' : 'row-ended'}">
            <td><code class="text-muted" style="font-size:.8em">${id.slice(0,8)}</code></td>
            <td>${name}${mapBtn}</td>
            <td>${c.name || '—'}</td>
            <td>${formatDate(r.start_time)}${endTime}</td>
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

async function startRental(id) {
    const r = rentalsData[id];
    const c = consolesData[r?.console_id] || {};
    if (!confirm(`Начать аренду "${c.name || id.slice(0,8)}"? Клиент забрал консоль.`)) return;
    const res = await api('POST', '/api/rentals/start', { rental_id: id });
    if (res.status === 'success') {
        rentalsData[id].status = 'active';
        rentalsData[id].start_time = new Date().toISOString();
        renderRentals(); loadDashboard();
        toast('Аренда началась! Клиент уведомлён.');
    } else toast(res.message, 'error');
}

async function extendRental(id) {
    const r = rentalsData[id];
    const c = consolesData[r?.console_id] || {};
    const hours = prompt(`Продлить аренду "${c.name || id.slice(0,8)}" на сколько часов?`, '2');
    if (!hours || isNaN(hours) || hours < 1) return;
    const res = await api('POST', '/api/rentals/extend', { rental_id: id, hours: parseInt(hours) });
    if (res.status === 'success') {
        rentalsData[id].expected_end_time = new Date(res.new_end).toISOString();
        rentalsData[id].expected_cost = (rentalsData[id].expected_cost || 0) + res.extra_cost;
        renderRentals();
        toast(`Продлено до ${res.new_end}. Доп. стоимость: ${res.extra_cost} лей`);
    } else toast(res.message, 'error');
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

async function requestLocationFromUser(rentalId, userId) {
    const res = await api('POST', '/api/users/request-location', { user_id: userId });
    if (res.status === 'success') {
        toast('Запрос геолокации отправлен клиенту');
    } else {
        toast(res.message || 'Ошибка', 'error');
    }
}

function showUserMap(lat, lng, name) {
    document.getElementById('map-modal-title').textContent = '📍 ' + name;
    const modal = new bootstrap.Modal(document.getElementById('mapModal'));
    modal.show();
    // Wait for modal to render, then init map
    setTimeout(() => {
        const container = document.getElementById('map-container');
        container.innerHTML = '';
        const iframe = document.createElement('iframe');
        iframe.style.width = '100%';
        iframe.style.height = '400px';
        iframe.style.border = 'none';
        iframe.style.borderRadius = '8px';
        iframe.src = `https://www.openstreetmap.org/export/embed.html?bbox=${lng-0.01},${lat-0.01},${lng+0.01},${lat+0.01}&layer=mapnik&marker=${lat},${lng}`;
        container.appendChild(iframe);
        document.getElementById('map-open-link').href = `https://www.google.com/maps?q=${lat},${lng}`;
        document.getElementById('map-open-link').style.display = 'inline-block';
    }, 300);
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
