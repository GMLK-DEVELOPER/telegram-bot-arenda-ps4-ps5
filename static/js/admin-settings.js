async function loadSettings() {
    const res = await api('GET', '/api/settings');
    const s   = res.settings || {};
    document.getElementById('s-max-hours').value          = s.max_rental_hours || 24;
    document.getElementById('s-reminder').value           = s.reminder_hours   || 23;
    document.getElementById('s-freq').value               = s.notification_frequency || 5;
    document.getElementById('s-require-approval').checked = s.require_approval !== false;
    document.getElementById('s-notif').checked            = s.notifications_enabled !== false;
    document.getElementById('s-push').checked             = s.push_notifications_enabled !== false;
    document.getElementById('s-critical').checked         = s.critical_notifications_enabled !== false;
    document.getElementById('s-photos').checked           = s.show_console_photos !== false;
    // Shop location
    document.getElementById('s-shop-title').value   = s.shop_title   || '';
    document.getElementById('s-shop-address').value = s.shop_address || '';
    document.getElementById('s-shop-lat').value     = s.shop_latitude  || '';
    document.getElementById('s-shop-lng').value     = s.shop_longitude || '';
    if (s.shop_latitude && s.shop_longitude) {
        document.getElementById('location-status').textContent = '✅ Координаты сохранены';
    }
    settingsData = s;
}

function openGoogleMaps() {
    window.open('https://www.google.com/maps', '_blank');
    document.getElementById('location-status').textContent = 'Найдите место → ПКМ → "Что здесь?" → скопируйте координаты';
}

document.addEventListener('DOMContentLoaded', () => {
    const form = document.getElementById('settings-form');
    if (!form) return;
    form.addEventListener('submit', async (e) => {
        e.preventDefault();
        const lat = parseFloat(document.getElementById('s-shop-lat').value) || 0;
        const lng = parseFloat(document.getElementById('s-shop-lng').value) || 0;
        const data = {
            ...settingsData,
            max_rental_hours:               parseInt(document.getElementById('s-max-hours').value),
            reminder_hours:                 parseInt(document.getElementById('s-reminder').value),
            notification_frequency:         parseInt(document.getElementById('s-freq').value),
            require_approval:               document.getElementById('s-require-approval').checked,
            notifications_enabled:          document.getElementById('s-notif').checked,
            push_notifications_enabled:     document.getElementById('s-push').checked,
            critical_notifications_enabled: document.getElementById('s-critical').checked,
            show_console_photos:            document.getElementById('s-photos').checked,
            shop_title:     document.getElementById('s-shop-title').value.trim(),
            shop_address:   document.getElementById('s-shop-address').value.trim(),
            shop_latitude:  lat,
            shop_longitude: lng,
        };
        const res = await api('POST', '/api/settings', data);
        if (res.status === 'success') {
            toast('Настройки сохранены');
            if (lat && lng) document.getElementById('location-status').textContent = '✅ Координаты сохранены';
        } else {
            toast(res.message, 'error');
        }
    });
});
