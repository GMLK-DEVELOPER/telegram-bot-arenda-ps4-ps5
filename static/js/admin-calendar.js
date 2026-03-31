// ── Calendar state ────────────────────────────────────────────────────────────
let calYear  = new Date().getFullYear();
let calMonth = new Date().getMonth(); // 0-based
let calBlockedDates = [];
let calRentals = {};

const MONTH_NAMES = [
    'Январь','Февраль','Март','Апрель','Май','Июнь',
    'Июль','Август','Сентябрь','Октябрь','Ноябрь','Декабрь'
];
const DAY_NAMES = ['Пн','Вт','Ср','Чт','Пт','Сб','Вс'];

// ── Load calendar data ────────────────────────────────────────────────────────
async function loadCalendar() {
    const [calRes, rentRes] = await Promise.all([
        api('GET', '/api/calendar'),
        api('GET', '/api/rentals'),
    ]);

    const cal = calRes.calendar || {};
    calBlockedDates = cal.system_blocked_dates || [];
    calRentals = rentRes.rentals || {};

    renderCalendar();
    renderBlockedList();

    document.getElementById('calendar-info').innerHTML = `
        <div class="mb-2"><i class="fas fa-calendar-check me-2 text-success"></i>
            <b>Рабочие дни:</b> ${(cal.working_days || [1,2,3,4,5,6,7]).join(', ')}
        </div>
        <div class="mb-2"><i class="fas fa-star me-2 text-warning"></i>
            <b>Праздники:</b> ${(cal.holidays || []).length} записей
        </div>
        <div><i class="fas fa-ban me-2 text-danger"></i>
            <b>Заблокировано:</b> ${calBlockedDates.length} дат
        </div>`;
}

// ── Render visual calendar ────────────────────────────────────────────────────
function renderCalendar() {
    document.getElementById('cal-month-title').textContent =
        `${MONTH_NAMES[calMonth]} ${calYear}`;

    // Build set of active rental date-ranges
    const rentalRanges = [];
    Object.values(calRentals).forEach(r => {
        if (r.status !== 'active') return;
        const start = r.start_time ? new Date(r.start_time) : null;
        const end   = r.expected_end_time ? new Date(r.expected_end_time) : null;
        if (start) rentalRanges.push({ start, end: end || start, consoleName: r.console_id });
    });

    const today = new Date();
    today.setHours(0,0,0,0);

    // First day of month (Monday-based)
    const firstDay = new Date(calYear, calMonth, 1);
    let startDow = firstDay.getDay(); // 0=Sun
    startDow = startDow === 0 ? 6 : startDow - 1; // Mon=0

    const daysInMonth = new Date(calYear, calMonth + 1, 0).getDate();

    let html = '<table class="cal-table w-100"><thead><tr>';
    DAY_NAMES.forEach(d => {
        html += `<th class="cal-th">${d}</th>`;
    });
    html += '</tr></thead><tbody><tr>';

    // Empty cells before month start
    for (let i = 0; i < startDow; i++) html += '<td class="cal-td"></td>';

    let col = startDow;
    for (let day = 1; day <= daysInMonth; day++) {
        const date = new Date(calYear, calMonth, day);
        date.setHours(0,0,0,0);
        const dateStr = `${calYear}-${String(calMonth+1).padStart(2,'0')}-${String(day).padStart(2,'0')}`;

        const isToday    = date.getTime() === today.getTime();
        const isBlocked  = calBlockedDates.includes(dateStr);
        const isRented   = rentalRanges.some(r => date >= r.start && date <= r.end);

        let cls = 'cal-td cal-day';
        if (isToday)   cls += ' cal-today';
        if (isBlocked) cls += ' cal-blocked';
        if (isRented)  cls += ' cal-rented';

        const tooltip = isBlocked ? 'Заблокировано' : isRented ? 'Аренда' : '';

        html += `<td class="${cls}" title="${tooltip}" onclick="calDayClick('${dateStr}',${isBlocked})">
            <span class="cal-day-num">${day}</span>
        </td>`;

        col++;
        if (col === 7 && day < daysInMonth) {
            html += '</tr><tr>';
            col = 0;
        }
    }

    // Fill remaining cells
    while (col > 0 && col < 7) {
        html += '<td class="cal-td"></td>';
        col++;
    }

    html += '</tr></tbody></table>';
    document.getElementById('cal-grid').innerHTML = html;
}

// ── Day click: block / unblock ────────────────────────────────────────────────
async function calDayClick(dateStr, isBlocked) {
    if (isBlocked) {
        if (!confirm(`Снять блокировку с ${dateStr}?`)) return;
        await removeBlockedDate(dateStr);
    } else {
        if (!confirm(`Заблокировать ${dateStr}?`)) return;
        document.getElementById('block-date-input').value = dateStr;
        await addBlockedDate();
    }
}

// ── Navigation ────────────────────────────────────────────────────────────────
function calPrevMonth() {
    calMonth--;
    if (calMonth < 0) { calMonth = 11; calYear--; }
    renderCalendar();
}

function calNextMonth() {
    calMonth++;
    if (calMonth > 11) { calMonth = 0; calYear++; }
    renderCalendar();
}

// ── Blocked dates list ────────────────────────────────────────────────────────
function renderBlockedList() {
    const list = document.getElementById('blocked-dates-list');
    list.innerHTML = calBlockedDates.length
        ? calBlockedDates.map(d => `
            <li class="list-group-item d-flex justify-content-between align-items-center py-1 px-2">
                <span><i class="fas fa-ban text-danger me-1"></i>${d}</span>
                <button class="btn btn-sm btn-outline-danger py-0 px-1" onclick="removeBlockedDate('${d}')">
                    <i class="fas fa-times"></i>
                </button>
            </li>`).join('')
        : '<li class="list-group-item text-muted text-center py-2">Нет заблокированных дат</li>';
}

async function addBlockedDate() {
    const date = document.getElementById('block-date-input').value;
    if (!date) { toast('Выберите дату', 'error'); return; }
    const res = await api('POST', '/api/calendar/blocked-dates', { date });
    if (res.status === 'success') { await loadCalendar(); toast('Дата заблокирована'); }
    else toast(res.message, 'error');
}

async function removeBlockedDate(date) {
    const res = await api('DELETE', `/api/calendar/blocked-dates?date=${date}`);
    if (res.status === 'success') { await loadCalendar(); toast('Блокировка снята'); }
    else toast(res.message, 'error');
}
