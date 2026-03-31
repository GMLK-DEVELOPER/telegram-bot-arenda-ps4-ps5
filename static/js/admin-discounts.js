function renderDiscounts() {
    const tbody = document.getElementById('discounts-table-body');
    tbody.innerHTML = '';

    const entries = Object.entries(discountsData);
    if (!entries.length) {
        tbody.innerHTML = '<tr><td colspan="7" class="text-center text-muted py-3">Нет скидок</td></tr>';
        return;
    }

    entries.forEach(([id, d]) => {
        const c = consolesData[d.console_id] || {};
        const active = d.active
            ? '<span class="badge bg-success">Активна</span>'
            : '<span class="badge bg-secondary">Неактивна</span>';

        tbody.innerHTML += `<tr>
            <td>${c.name || '—'}</td>
            <td>${d.type === 'percentage' ? 'Процент' : 'Фиксированная'}</td>
            <td class="fw-bold">${d.value}${d.type === 'percentage' ? '%' : ' лей'}</td>
            <td>${(d.start_date || '').slice(0, 10)}</td>
            <td>${(d.end_date || '').slice(0, 10)}</td>
            <td>${active}</td>
            <td>
                <button class="btn btn-sm btn-${d.active ? 'outline-warning' : 'outline-success'} me-1" onclick="toggleDiscount('${id}', ${!d.active})">
                    ${d.active ? '<i class="fas fa-pause"></i>' : '<i class="fas fa-play"></i>'}
                </button>
                <button class="btn btn-sm btn-outline-danger" onclick="deleteDiscount('${id}')"><i class="fas fa-trash"></i></button>
            </td>
        </tr>`;
    });
}

async function submitAddDiscount() {
    const form = document.getElementById('add-discount-form');
    const fd   = new FormData(form);
    const data = {
        console_id: fd.get('console_id'),
        type:       fd.get('type'),
        value:      parseFloat(fd.get('value')) || 0,
        start_date: new Date(fd.get('start_date')).toISOString(),
        end_date:   new Date(fd.get('end_date')).toISOString(),
        active:     true
    };
    const res = await api('POST', '/api/discounts', data);
    if (res.status === 'success') {
        discountsData[res.discount.id] = res.discount;
        bootstrap.Modal.getInstance(document.getElementById('addDiscountModal')).hide();
        form.reset(); renderDiscounts(); toast('Скидка добавлена');
    } else toast(res.message, 'error');
}

async function toggleDiscount(id, active) {
    const res = await api('PUT', '/api/discounts', { discount_id: id, active });
    if (res.status === 'success') { discountsData[id].active = active; renderDiscounts(); toast('Скидка обновлена'); }
    else toast(res.message, 'error');
}

async function deleteDiscount(id) {
    if (!confirm('Удалить скидку?')) return;
    const res = await api('DELETE', `/api/discounts?discount_id=${id}`);
    if (res.status === 'success') { delete discountsData[id]; renderDiscounts(); toast('Скидка удалена'); }
    else toast(res.message, 'error');
}
