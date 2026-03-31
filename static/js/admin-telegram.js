async function loadTelegramAdmins() {
    const res = await api('GET', '/api/telegram-admins');
    if (res.status !== 'success') { toast('Ошибка загрузки Telegram админов', 'error'); return; }

    const admins = res.admins || {};
    const tbody  = document.getElementById('telegram-admins-list');
    tbody.innerHTML = '';

    if (!Object.keys(admins).length) {
        tbody.innerHTML = '<tr><td colspan="6" class="text-center text-muted py-3">Нет Telegram админов. Добавьте первого!</td></tr>';
        return;
    }

    Object.values(admins).forEach(admin => {
        const tr = document.createElement('tr');
        tr.innerHTML = `
            <td><code>${admin.telegram_id}</code></td>
            <td>${admin.username ? `<span class="text-primary">${admin.username}</span>` : '—'}</td>
            <td>${admin.full_name || '—'}</td>
            <td class="text-muted">${formatDate(admin.added_at)}</td>
            <td>${admin.added_by || '—'}</td>
            <td>
                <button class="btn btn-sm btn-outline-danger" onclick="removeTelegramAdmin('${admin.telegram_id}')">
                    <i class="fas fa-trash"></i> Удалить
                </button>
            </td>`;
        tbody.appendChild(tr);
    });
}

function showAddTelegramAdminModal() {
    document.getElementById('add-telegram-admin-form').reset();
    new bootstrap.Modal(document.getElementById('addTelegramAdminModal')).show();
}

async function submitAddTelegramAdmin() {
    const form = document.getElementById('add-telegram-admin-form');
    const data = {
        telegram_id: form.telegram_id.value.trim(),
        username:    form.username.value.trim(),
        full_name:   form.full_name.value.trim(),
    };
    if (!data.telegram_id) { toast('Telegram ID обязателен', 'error'); return; }

    const res = await api('POST', '/api/telegram-admins', data);
    if (res.status === 'success') {
        toast('Telegram админ добавлен');
        bootstrap.Modal.getInstance(document.getElementById('addTelegramAdminModal')).hide();
        loadTelegramAdmins();
    } else toast(res.message || 'Ошибка', 'error');
}

async function removeTelegramAdmin(telegramID) {
    if (!confirm(`Удалить Telegram админа ${telegramID}?`)) return;
    const res = await api('DELETE', '/api/telegram-admins', { telegram_id: telegramID });
    if (res.status === 'success') { toast('Telegram админ удалён'); loadTelegramAdmins(); }
    else toast(res.message || 'Ошибка', 'error');
}
