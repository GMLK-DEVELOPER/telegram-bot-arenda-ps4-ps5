async function loadRatings() {
    const res     = await api('GET', '/api/ratings');
    const ratings = res.ratings || {};
    const tbody   = document.getElementById('ratings-table-body');
    tbody.innerHTML = '';

    Object.entries(usersData).forEach(([id, u]) => {
        const r    = ratings[id] || {};
        const name = u.full_name || u.first_name || id;
        const statusBadge = {
            premium: '<span class="badge bg-warning text-dark">Premium ⭐</span>',
            regular: '<span class="badge bg-info text-white">Обычный</span>',
            risk:    '<span class="badge bg-danger">Риск ⚠️</span>',
        }[r.Status] || '<span class="badge bg-secondary">—</span>';

        tbody.innerHTML += `<tr>
            <td>${name}</td>
            <td><b>${r.FinalScore ?? '—'}</b></td>
            <td>${r.Discipline ?? '—'}</td>
            <td>${r.Loyalty ?? '—'}</td>
            <td>${statusBadge}</td>
            <td>
                <button class="btn btn-sm btn-outline-primary me-1" onclick="addPoints('${id}', 'discipline')"><i class="fas fa-plus"></i> Дисц.</button>
                <button class="btn btn-sm btn-outline-success"  onclick="addPoints('${id}', 'loyalty')"><i class="fas fa-plus"></i> Лояльн.</button>
            </td>
        </tr>`;
    });

    if (!tbody.innerHTML) {
        tbody.innerHTML = '<tr><td colspan="6" class="text-center text-muted py-3">Нет данных</td></tr>';
    }
}

async function addPoints(userId, type) {
    const label = type === 'discipline' ? 'дисциплины' : 'лояльности';
    const pts   = parseInt(prompt(`Очки ${label} (отрицательное — снять):`));
    if (isNaN(pts)) return;
    const comment = prompt('Комментарий (опционально):') || '';
    const url = type === 'discipline' ? '/api/ratings/discipline' : '/api/ratings/loyalty';
    const res = await api('POST', url, { user_id: userId, points: pts, comment });
    if (res.status === 'success') { loadRatings(); toast('Очки обновлены'); }
    else toast(res.message, 'error');
}
