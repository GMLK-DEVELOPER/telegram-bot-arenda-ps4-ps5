package bot

import (
	"fmt"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"ps4-rental/internal/storage"
)

// getOccupiedDates returns all dates that are unavailable for a console
func getOccupiedDates(consoleID string) map[string]bool {
	occupied := make(map[string]bool)

	// Active rentals
	rentals := storage.LoadRentals()
	for _, r := range rentals {
		if r.ConsoleID == consoleID && r.Status == "active" && r.ExpectedEndTime != "" {
			start, _ := time.Parse(time.RFC3339, r.StartTime)
			end, _ := time.Parse(time.RFC3339, r.ExpectedEndTime)
			if start.IsZero() {
				start, _ = time.Parse("2006-01-02T15:04:05", r.StartTime)
			}
			if end.IsZero() {
				end, _ = time.Parse("2006-01-02T15:04:05", r.ExpectedEndTime)
			}
			for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
				occupied[d.Format("2006-01-02")] = true
			}
		}
	}

	// Calendar blocked dates
	cal := storage.LoadCalendar()
	for _, ds := range cal.SystemBlockedDates {
		occupied[ds] = true
	}
	for _, ds := range cal.ConsoleBlockedDates[consoleID] {
		occupied[ds] = true
	}
	for _, h := range cal.Holidays {
		if !h.Working {
			occupied[h.Date] = true
		}
	}

	return occupied
}

// createCalendar builds an inline keyboard calendar for a given month
func createCalendar(consoleID string, year, month int) tgbotapi.InlineKeyboardMarkup {
	occupied := getOccupiedDates(consoleID)

	cal := storage.LoadCalendar()
	workingDays := cal.WorkingDays
	if len(workingDays) == 0 {
		workingDays = []int{1, 2, 3, 4, 5, 6, 7}
	}
	workingSet := make(map[int]bool)
	for _, d := range workingDays {
		workingSet[d] = true
	}

	shortID := consoleID
	if len(shortID) > 8 {
		shortID = shortID[:8]
	}

	monthName := russianMonthName(month)

	var rows [][]tgbotapi.InlineKeyboardButton

	// Header row: Month Year
	rows = append(rows, []tgbotapi.InlineKeyboardButton{
		tgbotapi.NewInlineKeyboardButtonData(fmt.Sprintf("📅 %s %d", monthName, year), "ignore"),
	})

	// Day-of-week header
	rows = append(rows, []tgbotapi.InlineKeyboardButton{
		tgbotapi.NewInlineKeyboardButtonData("Пн", "ignore"),
		tgbotapi.NewInlineKeyboardButtonData("Вт", "ignore"),
		tgbotapi.NewInlineKeyboardButtonData("Ср", "ignore"),
		tgbotapi.NewInlineKeyboardButtonData("Чт", "ignore"),
		tgbotapi.NewInlineKeyboardButtonData("Пт", "ignore"),
		tgbotapi.NewInlineKeyboardButtonData("Сб", "ignore"),
		tgbotapi.NewInlineKeyboardButtonData("Вс", "ignore"),
	})

	firstDay := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.Local)
	daysInMonth := time.Date(year, time.Month(month)+1, 0, 0, 0, 0, 0, time.Local).Day()

	// Weekday of first day (Monday=1)
	startWeekday := int(firstDay.Weekday())
	if startWeekday == 0 {
		startWeekday = 7
	}

	day := 1
	for day <= daysInMonth {
		var row []tgbotapi.InlineKeyboardButton

		for col := 1; col <= 7; col++ {
			if (day == 1 && col < startWeekday) || day > daysInMonth {
				row = append(row, tgbotapi.NewInlineKeyboardButtonData(" ", "ignore"))
			} else {
				date := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.Local)
				dateStr := date.Format("2006-01-02")
				weekday := int(date.Weekday())
				if weekday == 0 {
					weekday = 7
				}

				isOccupied := occupied[dateStr] || !workingSet[weekday]
				if isOccupied {
					cb := fmt.Sprintf("busy_%s_%s", shortID, dateStr)
					row = append(row, tgbotapi.NewInlineKeyboardButtonData(fmt.Sprintf("🔴%d", day), cb))
				} else {
					hasDiscount := checkDateHasDiscount(consoleID, date)
					cb := fmt.Sprintf("dt_%s_%s", shortID, dateStr)
					label := fmt.Sprintf("%d", day)
					if hasDiscount {
						label = fmt.Sprintf("🔥%d", day)
					}
					row = append(row, tgbotapi.NewInlineKeyboardButtonData(label, cb))
				}
				day++
			}
		}
		rows = append(rows, row)
	}

	// Navigation
	prevMonth := month - 1
	prevYear := year
	if prevMonth < 1 {
		prevMonth = 12
		prevYear--
	}
	nextMonth := month + 1
	nextYear := year
	if nextMonth > 12 {
		nextMonth = 1
		nextYear++
	}

	rows = append(rows, []tgbotapi.InlineKeyboardButton{
		tgbotapi.NewInlineKeyboardButtonData("⬅️", fmt.Sprintf("cal_%s_%04d-%02d", shortID, prevYear, prevMonth)),
		tgbotapi.NewInlineKeyboardButtonData("➡️", fmt.Sprintf("cal_%s_%04d-%02d", shortID, nextYear, nextMonth)),
	})

	rows = append(rows, []tgbotapi.InlineKeyboardButton{
		tgbotapi.NewInlineKeyboardButtonData("⬅️ Назад к консоли", "console_"+consoleID),
	})

	return tgbotapi.InlineKeyboardMarkup{InlineKeyboard: rows}
}

func russianMonthName(month int) string {
	names := []string{
		"", "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
		"Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь",
	}
	if month >= 1 && month <= 12 {
		return names[month]
	}
	return ""
}
