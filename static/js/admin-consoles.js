function renderConsoles() {
    const grid = document.getElementById('consoles-grid');
    grid.innerHTML = '';

    const entries = Object.entries(consolesData);
    if (!entries.length) {
        grid.innerHTML = '<div class="col-12"><p class="text-muted text-center py-4">Нет консолей. Добавьте первую!</p></div>';
        return;
    }

    entries.forEach(([id, c]) => {
        const statusBadge = c.status === 'available'
            ? '<span class="badge bg-success">Свободна</span>'
            : '<span class="badge bg-warning text-dark">Арендована</span>';
        const photo = c.photo_path
            ? `<img src="${c.photo_path}" class="console-photo me-3">`
            : `<div class="me-3 d-flex align-items-center justify-content-center bg-light rounded" style="width:52px;height:52px;"><i class="fas fa-gamepad text-secondary"></i></div>`;

        grid.innerHTML += `
        <div class="col-xl-3 col-md-4 col-sm-6">
            <div class="card h-100">
                <div class="card-body">
                    <div class="d-flex align-items-center mb-3">
                        ${photo}
                        <div>
                            <h6 class="mb-0 fw-bold">${c.name}</h6>
                            <small class="text-muted">${c.model || 'Модель не указана'}</small>
                        </div>
                    </div>
                    <div class="mb-2">${statusBadge}</div>
                    <div class="small text-muted">
                        <div class="mb-1"><i class="fas fa-tag me-1 text-primary"></i> Аренда: <b>${c.rental_price} лей/ч</b></div>
                        ${c.sale_price > 0 ? `<div class="mb-1"><i class="fas fa-dollar-sign me-1 text-success"></i> Продажа: <b>${c.sale_price} лей</b></div>` : ''}
                        <div><i class="fas fa-gamepad me-1 text-info"></i> ${(c.games || []).slice(0,3).join(', ') || 'Игры не указаны'}</div>
                    </div>
                    <div class="mt-3 d-flex gap-2">
                        <button class="btn btn-sm btn-outline-warning flex-fill" onclick="openEditConsole('${id}')"><i class="fas fa-edit"></i> Изменить</button>
                        <button class="btn btn-sm btn-outline-danger" onclick="deleteConsole('${id}')"><i class="fas fa-trash"></i></button>
                    </div>
                </div>
            </div>
        </div>`;
    });

    // Fill console select in discount modal
    const sel = document.getElementById('discount-console-select');
    if (sel) {
        sel.innerHTML = Object.entries(consolesData)
            .map(([id, c]) => `<option value="${id}">${c.name}</option>`).join('');
    }
}

async function submitAddConsole() {
    const form  = document.getElementById('add-console-form');
    const fd    = new FormData(form);
    const games = (fd.get('games_str') || '').split(',').map(g => g.trim()).filter(Boolean);
    const data  = {
        name: fd.get('name'), model: fd.get('model'),
        rental_price: parseFloat(fd.get('rental_price')) || 0,
        sale_price:   parseFloat(fd.get('sale_price'))   || 0,
        games, show_photo_in_bot: !!fd.get('show_photo_in_bot')
    };
    const res = await api('POST', '/api/consoles', data);
    if (res.status === 'success') {
        consolesData[res.console.id] = res.console;
        const photoFile = form.querySelector('[name=photo]').files[0];
        if (photoFile) await uploadConsolePhoto(res.console.id, photoFile);
        bootstrap.Modal.getInstance(document.getElementById('addConsoleModal')).hide();
        form.reset(); renderConsoles(); toast('Консоль добавлена');
    } else toast(res.message, 'error');
}

function openEditConsole(id) {
    const c = consolesData[id];
    document.getElementById('edit-console-id').value        = id;
    document.getElementById('edit-name').value              = c.name;
    document.getElementById('edit-model').value             = c.model || '';
    document.getElementById('edit-rental-price').value      = c.rental_price;
    document.getElementById('edit-sale-price').value        = c.sale_price || 0;
    document.getElementById('edit-games').value             = (c.games || []).join(', ');
    new bootstrap.Modal(document.getElementById('editConsoleModal')).show();
}

async function submitEditConsole() {
    const form  = document.getElementById('edit-console-form');
    const fd    = new FormData(form);
    const id    = fd.get('console_id');
    const games = (fd.get('games_str') || '').split(',').map(g => g.trim()).filter(Boolean);
    const data  = {
        console_id: id, name: fd.get('name'), model: fd.get('model'),
        rental_price: parseFloat(fd.get('rental_price')),
        sale_price:   parseFloat(fd.get('sale_price')), games
    };
    const res = await api('PUT', '/api/consoles', data);
    if (res.status === 'success') {
        consolesData[id] = res.console;
        const photoFile = form.querySelector('[name=photo]').files[0];
        if (photoFile) await uploadConsolePhoto(id, photoFile);
        bootstrap.Modal.getInstance(document.getElementById('editConsoleModal')).hide();
        renderConsoles(); toast('Консоль обновлена');
    } else toast(res.message, 'error');
}

async function uploadConsolePhoto(consoleId, file) {
    const fd = new FormData();
    fd.append('console_id', consoleId);
    fd.append('photo', file);
    const r   = await fetch('/api/consoles/photo', { method: 'POST', body: fd });
    const res = await r.json();
    if (res.status === 'success' && consolesData[consoleId]) {
        consolesData[consoleId].photo_path = res.photo_path;
    }
}

async function deleteConsole(id) {
    if (!confirm('Удалить консоль? Это действие нельзя отменить.')) return;
    const res = await api('DELETE', `/api/consoles?console_id=${id}`);
    if (res.status === 'success') { delete consolesData[id]; renderConsoles(); toast('Консоль удалена'); }
    else toast(res.message, 'error');
}
