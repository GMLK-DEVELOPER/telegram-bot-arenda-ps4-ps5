function renderUsers() {
    const q     = document.getElementById('search-users')?.value?.toLowerCase() || '';
    const tbody = document.getElementById('users-table-body');
    tbody.innerHTML = '';

    Object.entries(usersData).forEach(([id, u]) => {
        const name     = u.full_name || u.first_name || 'Неизвестный';
        const joinDate = (u.joined_at || '').slice(0, 10);
        const rentals  = Object.values(rentalsData).filter(r => r.user_id === id).length;

        if (q && !`${name} ${u.phone_number || ''} ${id}`.toLowerCase().includes(q)) return;

        const status = u.is_banned
            ? '<span class="badge bg-danger">Заблокирован</span>'
            : '<span class="badge bg-success">Активен</span>';
        const action = u.is_banned
            ? `<button class="btn btn-sm btn-outline-success" onclick="unbanUser('${id}')"><i class="fas fa-unlock"></i> Разблокировать</button>`
            : `<button class="btn btn-sm btn-outline-danger"  onclick="banUser('${id}')"><i class="fas fa-ban"></i> Блок</button>`;

        tbody.innerHTML += `<tr class="${u.is_banned ? 'row-banned' : ''}">
            <td><code class="text-muted" style="font-size:.8em">${id}</code></td>
            <td>${name}</td>
            <td>${u.phone_number || '—'}</td>
            <td class="text-center"><span class="badge bg-light text-dark border">${rentals}</span></td>
            <td class="fw-bold">${u.total_spent || 0} лей</td>
            <td class="text-muted">${joinDate}</td>
            <td>${status}</td>
            <td>${action}</td>
        </tr>`;
    });

    if (!tbody.innerHTML) {
        tbody.innerHTML = '<tr><td colspan="8" class="text-center text-muted py-3">Нет пользователей</td></tr>';
    }
}

async function banUser(id) {
    if (!confirm('Заблокировать пользователя?')) return;
    const res = await api('POST', '/api/users/ban', { user_id: id });
    if (res.status === 'success') { usersData[id].is_banned = true; renderUsers(); toast('Пользователь заблокирован'); }
    else toast(res.message, 'error');
}

async function unbanUser(id) {
    const res = await api('POST', '/api/users/unban', { user_id: id });
    if (res.status === 'success') { usersData[id].is_banned = false; renderUsers(); toast('Пользователь разблокирован'); }
    else toast(res.message, 'error');
}

function exportUsersCSV() {
    const rows = Object.entries(usersData).map(([id, u]) => ({
        'ID':           id,
        'Имя':          u.full_name || u.first_name || '',
        'Телефон':      u.phone_number || '',
        'Аренд':        Object.values(rentalsData).filter(r => r.user_id === id).length,
        'Потрачено':    u.total_spent || 0,
        'Дата':         (u.joined_at || '').slice(0,10),
        'Статус':       u.is_banned ? 'Заблокирован' : 'Активен',
    }));
    const d = new Date().toISOString().slice(0,10);
    exportCSV(rows, `users_${d}.csv`);
}
