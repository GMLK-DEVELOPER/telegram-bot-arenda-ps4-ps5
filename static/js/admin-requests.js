function renderRequests() {
    const q     = document.getElementById('search-requests')?.value?.toLowerCase() || '';
    const tbody = document.getElementById('requests-table-body');
    tbody.innerHTML = '';

    const entries = Object.entries(requestsData)
        .sort((a, b) => new Date(b[1].request_time || b[1].created_at || 0) - new Date(a[1].request_time || a[1].created_at || 0));

    entries.forEach(([id, r]) => {
        const c    = consolesData[r.console_id] || {};
        const u    = usersData[r.user_id] || {};
        const name = u.full_name || u.first_name || r.user_id;

        if (q && !`${name} ${c.name || ''} ${u.phone_number || ''}`.toLowerCase().includes(q)) return;

        const isPending = r.status === 'pending' || r.status === 'pending_approval';
        const statusBadge = {
            pending:          '<span class="badge bg-warning text-dark">Ожидает</span>',
            pending_approval: '<span class="badge bg-warning text-dark">Ожидает</span>',
            approved:         '<span class="badge bg-success">Одобрена</span>',
            rejected:         '<span class="badge bg-danger">Отклонена</span>',
        }[r.status] || `<span class="badge bg-secondary">${r.status}</span>`;

        const actions = isPending
            ? `<button class="btn btn-sm btn-success me-1" onclick="approveRequest('${id}')"><i class="fas fa-check"></i> Одобрить</button>
               <button class="btn btn-sm btn-danger" onclick="rejectRequest('${id}')"><i class="fas fa-times"></i> Отклонить</button>`
            : '<span class="text-muted">—</span>';

        tbody.innerHTML += `<tr class="${isPending ? 'row-pending' : ''}">
            <td>${name}</td>
            <td>${u.phone_number || '—'}</td>
            <td>${c.name || '—'}</td>
            <td class="fw-bold">${r.total_cost || r.expected_cost || 0} лей</td>
            <td>${formatDate(r.request_time || r.created_at || '')}</td>
            <td>${statusBadge}</td>
            <td>${actions}</td>
        </tr>`;
    });

    if (!tbody.innerHTML) {
        tbody.innerHTML = '<tr><td colspan="7" class="text-center text-muted py-3">Нет заявок</td></tr>';
    }
}

async function approveRequest(id) {
    const res = await api('POST', '/api/rental-requests/approve', { request_id: id });
    if (res.status === 'success') {
        requestsData[id].status = 'approved';
        renderRequests(); loadDashboard();
        toast('Заявка одобрена');
    } else toast(res.message, 'error');
}

async function rejectRequest(id) {
    const res = await api('POST', '/api/rental-requests/reject', { request_id: id });
    if (res.status === 'success') {
        requestsData[id].status = 'rejected';
        renderRequests(); loadDashboard();
        toast('Заявка отклонена');
    } else toast(res.message, 'error');
}
