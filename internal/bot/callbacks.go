package bot

import (
	"fmt"
	"strings"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/google/uuid"
	"ps4-rental/internal/models"
	"ps4-rental/internal/storage"
)

func handleCallback(call *tgbotapi.CallbackQuery) {
	data := call.Data
	userID := fmt.Sprintf("%d", call.From.ID)

	switch {
	case data == "ignore":
		answerCallback(call.ID, "", false)

	case strings.HasPrefix(data, "reserved_"):
		answerCallback(call.ID, "⏳ Эта консоль временно занята другим пользователем. Попробуйте позже.", true)

	case strings.HasPrefix(data, "console_"):
		handleConsoleSelection(call, userID)

	case strings.HasPrefix(data, "cal_"):
		handleCalendarNavigation(call)

	case strings.HasPrefix(data, "busy_"):
		answerCallback(call.ID, "🔴 Эта дата занята! Выберите другую дату.", true)

	case strings.HasPrefix(data, "dt_"):
		handleDateSelection(call)

	case strings.HasPrefix(data, "rd_"):
		handleConfirmRentWithDate(call)

	case strings.HasPrefix(data, "crd_"):
		handleFinalRentConfirmation(call)

	case strings.HasPrefix(data, "rent_") && len(strings.Split(data, "_")) == 3:
		handleConfirmRentWithTime(call)

	case strings.HasPrefix(data, "confirm_rent_"):
		handleConfirmRent(call)

	case strings.HasPrefix(data, "buy_"):
		handleBuyCallback(call)

	case strings.HasPrefix(data, "end_rental_"):
		handleEndRentalCallback(call, userID)

	case data == "back_to_selection":
		handleBackToSelection(call, userID)

	// Admin callbacks
	case strings.HasPrefix(data, "approve_"):
		handleApproveRequest(call, userID)

	case strings.HasPrefix(data, "reject_"):
		handleRejectRequest(call, userID)

	case data == "admin_requests":
		handleAdminRequests(call, userID)

	case data == "admin_settings":
		handleAdminSettings(call, userID)

	case data == "admin_users":
		handleAdminUsersCallback(call, userID)

	case data == "admin_ratings":
		handleAdminRatings(call, userID)

	case data == "admin_web_info":
		handleAdminWebInfo(call, userID)

	case strings.HasPrefix(data, "user_manage_"):
		handleUserManage(call, userID)

	case strings.HasPrefix(data, "ban_user_"):
		handleBanUser(call, userID)

	case strings.HasPrefix(data, "unban_user_"):
		handleUnbanUser(call, userID)

	case strings.HasPrefix(data, "rating_manage_"):
		handleRatingManage(call, userID)

	case data == "rating_stats":
		handleRatingStats(call, userID)

	case strings.HasPrefix(data, "waitlist_"):
		handleWaitlistCallback(call, userID)
	}
}

func handleWaitlistCallback(call *tgbotapi.CallbackQuery, userID string) {
	// data: waitlist_join_<consoleID> or waitlist_leave_<consoleID>
	parts := strings.SplitN(call.Data, "_", 3)
	if len(parts) < 3 {
		answerCallback(call.ID, "❌ Ошибка", false)
		return
	}
	action := parts[1]    // join or leave
	consoleID := parts[2] // consoleID (UUID with dashes)

	if action == "join" {
		_ = storage.AddToWaitlist(userID, consoleID)
		answerCallback(call.ID, "🔔 Вы в очереди! Уведомим когда консоль освободится.", false)
	} else {
		_ = storage.RemoveFromWaitlist(userID, consoleID)
		answerCallback(call.ID, "Вы вышли из очереди.", false)
	}

	// Refresh the unavailable console view
	consoles := storage.LoadConsoles()
	c, ok := consoles[consoleID]
	if !ok {
		return
	}
	info := getConsoleRentalInfo(consoleID)
	var text string
	if info != nil {
		end := info["estimated_end_time"].(time.Time)
		text = fmt.Sprintf("*%s* (%s)\n\n🔴 *Статус:* Занята\n📅 *Занята до:* %s\n\n💰 Цена аренды: %.0f лей/час",
			c.Name, c.Model, end.Format("02.01.2006 15:04"), c.RentalPrice)
	} else {
		text = fmt.Sprintf("*%s* (%s)\n\n🔴 *Статус:* Занята\n💰 Цена аренды: %.0f лей/час", c.Name, c.Model, c.RentalPrice)
	}

	inQueue := storage.IsInWaitlist(userID, consoleID)
	var queueBtn tgbotapi.InlineKeyboardButton
	if inQueue {
		queueBtn = tgbotapi.NewInlineKeyboardButtonData("✅ Вы в очереди (отменить)", "waitlist_leave_"+consoleID)
		text += "\n\n🔔 *Вы в очереди* — получите уведомление когда освободится"
	} else {
		queueBtn = tgbotapi.NewInlineKeyboardButtonData("🔔 Встать в очередь", "waitlist_join_"+consoleID)
	}
	markup := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(queueBtn),
		tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("⬅️ Назад к выбору", "back_to_selection")),
	)
	editOrSend(call, text, markup)
}

// ─── Console selection ───────────────────────────────────────────────────────

func handleConsoleSelection(call *tgbotapi.CallbackQuery, userID string) {
	if isUserBanned(userID) {
		answerCallback(call.ID, "❌ Ваш аккаунт заблокирован.", false)
		return
	}

	parts := strings.Split(call.Data, "_")
	consoles := storage.LoadConsoles()

	// console_unavailable_<id>
	if len(parts) > 2 && parts[1] == "unavailable" {
		consoleID := parts[2]
		c, ok := consoles[consoleID]
		if !ok {
			answerCallback(call.ID, "❌ Консоль не найдена", false)
			return
		}
		info := getConsoleRentalInfo(consoleID)
		var text string
		if info != nil {
			end := info["estimated_end_time"].(time.Time)
			text = fmt.Sprintf("*%s* (%s)\n\n🔴 *Статус:* Занята\n📅 *Занята до:* %s\n\n💰 Цена аренды: %.0f лей/час",
				c.Name, c.Model,
				end.Format("02.01.2006 15:04"), c.RentalPrice)
		} else {
			text = fmt.Sprintf("*%s* (%s)\n\n🔴 *Статус:* Занята\n💰 Цена аренды: %.0f лей/час", c.Name, c.Model, c.RentalPrice)
		}

		inQueue := storage.IsInWaitlist(userID, consoleID)
		var queueBtn tgbotapi.InlineKeyboardButton
		if inQueue {
			queueBtn = tgbotapi.NewInlineKeyboardButtonData("✅ Вы в очереди (отменить)", "waitlist_leave_"+consoleID)
			text += "\n\n🔔 *Вы в очереди* — получите уведомление когда консоль освободится"
		} else {
			queueBtn = tgbotapi.NewInlineKeyboardButtonData("🔔 Встать в очередь", "waitlist_join_"+consoleID)
		}

		markup := tgbotapi.NewInlineKeyboardMarkup(
			tgbotapi.NewInlineKeyboardRow(queueBtn),
			tgbotapi.NewInlineKeyboardRow(
				tgbotapi.NewInlineKeyboardButtonData("⬅️ Назад к выбору", "back_to_selection"),
			),
		)
		editOrSend(call, text, markup)
		return
	}

	// console_<id>
	consoleID := parts[1]
	c, ok := consoles[consoleID]
	if !ok {
		answerCallback(call.ID, "❌ Консоль не найдена", false)
		return
	}

	caption := fmt.Sprintf("*%s* (%s)\n\n💰 Цена аренды: %.0f лей/час\n", c.Name, c.Model, c.RentalPrice)
	if c.SalePrice > 0 {
		caption += fmt.Sprintf("🏷️ Цена покупки: %.0f лей\n", c.SalePrice)
	}
	status := "🔴 Занята"
	if c.Status == "available" {
		status = "✅ Доступна"
	}
	caption += fmt.Sprintf("📊 Статус: %s\n\n", status)
	if len(c.Games) > 0 {
		caption += "*Доступные игры:*\n"
		for _, g := range c.Games {
			caption += fmt.Sprintf("• %s\n", g)
		}
	}

	var rows [][]tgbotapi.InlineKeyboardButton
	if c.Status == "available" {
		now := time.Now()
		shortID := consoleID
		if len(shortID) > 8 {
			shortID = shortID[:8]
		}
		calCb := fmt.Sprintf("cal_%s_%s", shortID, now.Format("2006-01"))
		rows = append(rows, tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("📅 Выбрать дату аренды", calCb),
		))
	}
	rows = append(rows, tgbotapi.NewInlineKeyboardRow(
		tgbotapi.NewInlineKeyboardButtonData("⬅️ Назад к выбору", "back_to_selection"),
	))
	markup := tgbotapi.InlineKeyboardMarkup{InlineKeyboard: rows}

	// Try to send with photo
	photoPath := getConsolePhotoPath(consoleID, &c)
	settings := storage.LoadSettings()

	if settings.ShowConsolePhotos && c.ShowPhotoInBot && photoPath != "" {
		// Delete old message and send photo
		del := tgbotapi.NewDeleteMessage(call.Message.Chat.ID, call.Message.MessageID)
		_, _ = botInstance.Request(del)

		photo := tgbotapi.NewPhoto(call.Message.Chat.ID, tgbotapi.FilePath(photoPath))
		photo.Caption = caption
		photo.ParseMode = "Markdown"
		photo.ReplyMarkup = markup
		_, _ = botInstance.Send(photo)
	} else {
		editOrSend(call, caption, markup)
	}
}

// ─── Calendar navigation ─────────────────────────────────────────────────────

func handleCalendarNavigation(call *tgbotapi.CallbackQuery) {
	// cal_<shortID>_<YYYY-MM>
	parts := strings.Split(call.Data, "_")
	if len(parts) < 3 {
		answerCallback(call.ID, "❌ Ошибка навигации", false)
		return
	}
	shortID := parts[1]
	dateStr := parts[2]

	// Find full console ID
	consoles := storage.LoadConsoles()
	var consoleID string
	for id := range consoles {
		if strings.HasPrefix(id, shortID) {
			consoleID = id
			break
		}
	}
	if consoleID == "" {
		answerCallback(call.ID, "❌ Консоль не найдена", false)
		return
	}

	var year, month int
	fmt.Sscanf(dateStr, "%d-%d", &year, &month)
	if year == 0 || month == 0 {
		now := time.Now()
		year, month = now.Year(), int(now.Month())
	}

	text := "📅 *Выберите дату начала аренды*\n\n🔴 - Занято\n\nВыберите свободную дату:"
	markup := createCalendar(consoleID, year, month)
	editOrSend(call, text, markup)
}

// ─── Date selection ──────────────────────────────────────────────────────────

var userDateStates = make(map[string]map[string]string) // userID -> {console_id, selected_date}

func handleDateSelection(call *tgbotapi.CallbackQuery) {
	// dt_<shortID>_<YYYY-MM-DD>
	parts := strings.Split(call.Data, "_")
	if len(parts) < 3 {
		answerCallback(call.ID, "❌ Ошибка выбора даты", false)
		return
	}
	shortID := parts[1]
	selectedDate := parts[2]

	consoles := storage.LoadConsoles()
	var consoleID string
	for id := range consoles {
		if strings.HasPrefix(id, shortID) {
			consoleID = id
			break
		}
	}
	if consoleID == "" {
		answerCallback(call.ID, "❌ Консоль не найдена", false)
		return
	}

	userID := fmt.Sprintf("%d", call.From.ID)
	userDateStates[userID] = map[string]string{"console_id": consoleID, "selected_date": selectedDate}

	console := consoles[consoleID]
	pricePerHour := console.RentalPrice

	selectedDateObj, _ := time.Parse("2006-01-02", selectedDate)
	formattedDate := selectedDateObj.Format("02.01.2006")

	resp := fmt.Sprintf("⏰ *Выберите продолжительность аренды*\n\n🎮 Консоль: %s\n📅 Дата начала: %s\n💰 Цена: %.0f лей/час\n\n📊 *Варианты аренды:*\n",
		console.Name, formattedDate, pricePerHour)

	timeOptions := []int{24, 48, 72, 168, 336}
	dayLabels := []int{1, 2, 3, 7, 14}
	occupied := getOccupiedDates(consoleID)

	var rows [][]tgbotapi.InlineKeyboardButton
	for i, hours := range timeOptions {
		days := dayLabels[i]
		originalCost := float64(hours) * pricePerHour
		discountedCost, discountAmt, _ := calculateDiscountedPrice(consoleID, originalCost, hours)

		// Check availability for this period
		endDate := selectedDateObj.AddDate(0, 0, days)
		available := true
		for d := selectedDateObj; d.Before(endDate); d = d.AddDate(0, 0, 1) {
			if occupied[d.Format("2006-01-02")] {
				available = false
				break
			}
		}

		var label string
		if discountAmt > 0 {
			label = fmt.Sprintf("%d %s - %.0f лей 🔥", days, daysLabel(days), discountedCost)
			resp += fmt.Sprintf("• %d %s = ~~%.0f~~ *%.0f лей* 🔥 (-%.0f лей)\n",
				days, daysLabel(days), originalCost, discountedCost, discountAmt)
		} else {
			label = fmt.Sprintf("%d %s - %.0f лей", days, daysLabel(days), originalCost)
			resp += fmt.Sprintf("• %d %s = %.0f лей\n", days, daysLabel(days), originalCost)
		}

		var cb string
		if !available {
			label += " ❌"
			resp += " ❌ (пересекается с занятыми датами)\n"
			cb = "ignore"
		} else {
			shortConsoleID := consoleID
			if len(shortConsoleID) > 8 {
				shortConsoleID = shortConsoleID[:8]
			}
			cb = fmt.Sprintf("rd_%s_%s_%d", shortConsoleID, selectedDate, hours)
		}
		rows = append(rows, tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData(label, cb)))
	}

	shortID2 := consoleID
	if len(shortID2) > 8 {
		shortID2 = shortID2[:8]
	}
	rows = append(rows, tgbotapi.NewInlineKeyboardRow(
		tgbotapi.NewInlineKeyboardButtonData("⬅️ Выбрать другую дату",
			fmt.Sprintf("cal_%s_%s", shortID2, selectedDateObj.Format("2006-01"))),
	))
	markup := tgbotapi.InlineKeyboardMarkup{InlineKeyboard: rows}
	editOrSend(call, resp, markup)
}

// ─── rd_: show confirmation of rent with specific date ───────────────────────

func handleConfirmRentWithDate(call *tgbotapi.CallbackQuery) {
	// rd_<shortID>_<YYYY-MM-DD>_<hours>
	parts := strings.Split(call.Data, "_")
	if len(parts) < 4 {
		answerCallback(call.ID, "❌ Ошибка", false)
		return
	}
	shortID := parts[1]
	selectedDate := parts[2]
	var selectedHours int
	fmt.Sscanf(parts[3], "%d", &selectedHours)

	consoles := storage.LoadConsoles()
	var consoleID string
	for id := range consoles {
		if strings.HasPrefix(id, shortID) {
			consoleID = id
			break
		}
	}
	if consoleID == "" {
		answerCallback(call.ID, "❌ Консоль не найдена", false)
		return
	}

	userID := fmt.Sprintf("%d", call.From.ID)
	if isUserBanned(userID) {
		answerCallback(call.ID, "❌ Ваш аккаунт заблокирован.", false)
		return
	}

	// Check availability
	selectedDateObj, _ := time.Parse("2006-01-02", selectedDate)
	endDateObj := selectedDateObj.AddDate(0, 0, selectedHours/24)
	occupied := getOccupiedDates(consoleID)
	for d := selectedDateObj; d.Before(endDateObj); d = d.AddDate(0, 0, 1) {
		if occupied[d.Format("2006-01-02")] {
			answerCallback(call.ID, "❌ Выбранные даты больше недоступны", false)
			return
		}
	}

	c := consoles[consoleID]
	if c.Status != "available" {
		answerCallback(call.ID, "❌ Консоль недоступна", false)
		return
	}

	originalCost := float64(selectedHours) * c.RentalPrice
	totalCost, discountAmt, _ := calculateDiscountedPrice(consoleID, originalCost, selectedHours)

	days := selectedHours / 24
	resp := fmt.Sprintf("🎮 *Подтверждение аренды*\n\n*Консоль:* %s (%s)\n*Период:* %s - %s\n*Продолжительность:* %d %s\n",
		c.Name, c.Model,
		selectedDateObj.Format("02.01.2006"), endDateObj.Format("02.01.2006"),
		days, daysLabel(days))

	if discountAmt > 0 {
		resp += fmt.Sprintf("*Цена без скидки:* ~~%.0f лей~~\n*Скидка:* -%.0f лей 🔥\n*Итоговая стоимость:* *%.0f лей*\n\n",
			originalCost, discountAmt, totalCost)
	} else {
		resp += fmt.Sprintf("*Стоимость:* %.0f лей\n\n", totalCost)
	}

	if len(c.Games) > 0 {
		resp += "*Игры:*\n"
		for _, g := range c.Games {
			resp += fmt.Sprintf("• %s\n", g)
		}
		resp += "\n"
	}
	resp += "Подтвердить аренду?"

	shortConsoleID := consoleID
	if len(shortConsoleID) > 8 {
		shortConsoleID = shortConsoleID[:8]
	}

	markup := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("✅ Подтвердить",
				fmt.Sprintf("crd_%s_%s_%d", shortConsoleID, selectedDate, selectedHours)),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("⬅️ Выбрать другую дату",
				fmt.Sprintf("cal_%s_%s", shortConsoleID, selectedDateObj.Format("2006-01"))),
		),
	)
	editOrSend(call, resp, markup)
}

// ─── crd_: final confirmation ────────────────────────────────────────────────

func handleFinalRentConfirmation(call *tgbotapi.CallbackQuery) {
	parts := strings.Split(call.Data, "_")
	if len(parts) < 4 {
		answerCallback(call.ID, "❌ Ошибка", false)
		return
	}
	shortID := parts[1]
	selectedDate := parts[2]
	var selectedHours int
	fmt.Sscanf(parts[3], "%d", &selectedHours)

	consoles := storage.LoadConsoles()
	var consoleID string
	for id := range consoles {
		if strings.HasPrefix(id, shortID) {
			consoleID = id
			break
		}
	}
	if consoleID == "" {
		answerCallback(call.ID, "❌ Консоль не найдена", false)
		return
	}

	userID := fmt.Sprintf("%d", call.From.ID)
	if isUserBanned(userID) {
		answerCallback(call.ID, "❌ Ваш аккаунт заблокирован.", false)
		return
	}

	users := storage.LoadUsers()
	user := users[userID]
	if user.PhoneNumber == "" || user.FullName == "" {
		answerCallback(call.ID, "❌ Необходимо завершить регистрацию", false)
		return
	}

	c := consoles[consoleID]
	if c.Status != "available" {
		answerCallback(call.ID, "❌ Консоль уже недоступна", false)
		return
	}

	selectedDateObj, _ := time.Parse("2006-01-02", selectedDate)
	endDateObj := selectedDateObj.Add(time.Duration(selectedHours) * time.Hour)

	originalCost := float64(selectedHours) * c.RentalPrice
	totalCost, _, _ := calculateDiscountedPrice(consoleID, originalCost, selectedHours)

	rentalID := uuid.New().String()
	now := time.Now()

	rentalData := models.RentalRequest{
		ID:               rentalID,
		UserID:           userID,
		ConsoleID:        consoleID,
		StartTime:        selectedDateObj.Format(time.RFC3339),
		EstimatedEndTime: endDateObj.Format(time.RFC3339),
		DurationHours:    selectedHours,
		TotalCost:        totalCost,
		CreatedAt:        now.Format(time.RFC3339),
	}

	var resp string
	if isApprovalRequired() {
		rentalData.Status = "pending_approval"
		_ = storage.SaveRequest(rentalData)

		resp = fmt.Sprintf("📋 *Заявка на аренду отправлена!*\n\n🎮 *Консоль:* %s\n📅 *Период:* %s - %s\n💰 *Стоимость:* %.0f лей\n\n⏳ Ожидайте подтверждения от администратора",
			c.Name, selectedDateObj.Format("02.01.2006"), endDateObj.Format("02.01.2006"), totalCost)

		// Notify all admins
		admins := storage.LoadTelegramAdmins()
		if len(admins) > 0 {
			adminMsg := fmt.Sprintf("📋 *Новая заявка на аренду!*\n\n👤 *Пользователь:* %s\n📞 *Телефон:* %s\n🎮 *Консоль:* %s\n📅 *Период:* %s - %s\n💰 *Стоимость:* %.0f лей\n🆔 *ID заявки:* %s",
				user.FullName, user.PhoneNumber, c.Name,
				selectedDateObj.Format("02.01.2006"), endDateObj.Format("02.01.2006"),
				totalCost, rentalID)

			kbAdmin := tgbotapi.NewInlineKeyboardMarkup(
				tgbotapi.NewInlineKeyboardRow(
					tgbotapi.NewInlineKeyboardButtonData("✅ Одобрить", "approve_"+rentalID),
					tgbotapi.NewInlineKeyboardButtonData("❌ Отклонить", "reject_"+rentalID),
				),
			)

			for _, admin := range admins {
				var adminChatID int64
				fmt.Sscanf(admin.TelegramID, "%d", &adminChatID)
				m := tgbotapi.NewMessage(adminChatID, adminMsg)
				m.ParseMode = "Markdown"
				m.ReplyMarkup = kbAdmin
				_, _ = botInstance.Send(m)
			}
		}
	} else {
		// Direct approval
		rentalData.Status = "active"
		rentals := storage.LoadRentals()
		rental := models.Rental{
			ID:              rentalID,
			UserID:          userID,
			ConsoleID:       consoleID,
			StartTime:       selectedDateObj.Format(time.RFC3339),
			ExpectedEndTime: endDateObj.Format(time.RFC3339),
			DurationHours:   selectedHours,
			TotalCost:       totalCost,
			Status:          "active",
			CreatedAt:       now.Format(time.RFC3339),
		}
		rentals[rentalID] = rental
		_ = storage.SaveRentals(rentals)

		c.Status = "rented"
		consoles[consoleID] = c
		_ = storage.SaveConsoles(consoles)

		resp = fmt.Sprintf("✅ *Аренда подтверждена!*\n\n🎮 *Консоль:* %s\n📅 *Период:* %s - %s\n💰 *Стоимость:* %.0f лей\n🆔 *ID аренды:* `%s`\n\n📞 Свяжитесь с нами для получения консоли!",
			c.Name, selectedDateObj.Format("02.01.2006"), endDateObj.Format("02.01.2006"), totalCost, rentalID)
	}

	del := tgbotapi.NewDeleteMessage(call.Message.Chat.ID, call.Message.MessageID)
	_, _ = botInstance.Request(del)
	sendMessage(call.Message.Chat.ID, resp, nil)
}

// ─── rent_ (time selection → confirm) ───────────────────────────────────────

func handleConfirmRentWithTime(call *tgbotapi.CallbackQuery) {
	// rent_<consoleID>_<hours>
	parts := strings.Split(call.Data, "_")
	consoleID := parts[1]
	var selectedHours int
	fmt.Sscanf(parts[2], "%d", &selectedHours)

	userID := fmt.Sprintf("%d", call.From.ID)
	if isUserBanned(userID) {
		answerCallback(call.ID, "❌ Ваш аккаунт заблокирован.", false)
		return
	}

	consoles := storage.LoadConsoles()
	c, ok := consoles[consoleID]
	if !ok || c.Status != "available" {
		answerCallback(call.ID, "❌ Консоль недоступна", false)
		return
	}

	originalCost := float64(selectedHours) * c.RentalPrice
	totalCost, discountAmt, _ := calculateDiscountedPrice(consoleID, originalCost, selectedHours)
	days := selectedHours / 24

	resp := fmt.Sprintf("✅ *Подтвердите аренду*\n\n🎮 Консоль: %s\n⏰ Время: %d %s\n",
		c.Name, days, daysLabel(days))
	if discountAmt > 0 {
		resp += fmt.Sprintf("💰 Цена без скидки: ~~%.0f лей~~\n🔥 Скидка: -%.0f лей\n💰 *К оплате: %.0f лей*\n\n",
			originalCost, discountAmt, totalCost)
	} else {
		resp += fmt.Sprintf("💰 К оплате: %.0f лей\n\n", totalCost)
	}
	resp += "Подтвердите аренду на выбранное время:"

	markup := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("✅ Подтвердить",
				fmt.Sprintf("confirm_rent_%s_%d", consoleID, selectedHours)),
			tgbotapi.NewInlineKeyboardButtonData("❌ Отмена",
				"select_time_"+consoleID),
		),
	)
	editOrSend(call, resp, markup)
}

// ─── confirm_rent_ ───────────────────────────────────────────────────────────

func handleConfirmRent(call *tgbotapi.CallbackQuery) {
	parts := strings.Split(call.Data, "_")
	// confirm_rent_<consoleID>_<hours>
	if len(parts) < 4 {
		return
	}
	consoleID := parts[2]
	var selectedHours int
	fmt.Sscanf(parts[3], "%d", &selectedHours)

	userID := fmt.Sprintf("%d", call.From.ID)
	if isUserBanned(userID) {
		answerCallback(call.ID, "❌ Ваш аккаунт заблокирован.", false)
		return
	}

	consoles := storage.LoadConsoles()
	c, ok := consoles[consoleID]
	if !ok || c.Status != "available" {
		answerCallback(call.ID, "❌ Консоль недоступна", false)
		return
	}

	if isApprovalRequired() {
		// Create request
		requestID := uuid.New().String()
		_ = storage.SaveRequest(models.RentalRequest{
			ID:            requestID,
			UserID:        userID,
			ConsoleID:     consoleID,
			SelectedHours: selectedHours,
			ExpectedCost:  float64(selectedHours) * c.RentalPrice,
			RequestTime:   time.Now().Format(time.RFC3339),
			Status:        "pending",
		})

		users := storage.LoadUsers()
		user := users[userID]
		days := selectedHours / 24

		adminMsg := fmt.Sprintf("🔔 *Новая заявка на аренду*\n\n👤 *Пользователь:*\n• ФИО: %s\n• Телефон: %s\n• ID: %s\n\n🎮 *Консоль:*\n• %s (%s)\n• Цена: %.0f лей/час\n• Время аренды: %d %s\n• К оплате: %.0f лей\n\n⏰ Время заявки: %s",
			user.FullName, user.PhoneNumber, userID,
			c.Name, c.Model, c.RentalPrice,
			days, daysLabel(days), float64(selectedHours)*c.RentalPrice,
			time.Now().Format("2006-01-02 15:04"))

		// Notify all admins
		admins := storage.LoadTelegramAdmins()
		if len(admins) > 0 {
			kbAdmin := tgbotapi.NewInlineKeyboardMarkup(
				tgbotapi.NewInlineKeyboardRow(
					tgbotapi.NewInlineKeyboardButtonData("✅ Одобрить", "approve_"+requestID),
					tgbotapi.NewInlineKeyboardButtonData("❌ Отклонить", "reject_"+requestID),
				),
			)

			for _, admin := range admins {
				var adminChatID int64
				fmt.Sscanf(admin.TelegramID, "%d", &adminChatID)
				m := tgbotapi.NewMessage(adminChatID, adminMsg)
				m.ParseMode = "Markdown"
				m.ReplyMarkup = kbAdmin
				_, _ = botInstance.Send(m)
			}
		}

		resp := fmt.Sprintf("📝 *Заявка на аренду отправлена!*\n\n🎮 Консоль: %s\n💰 Цена: %.0f лей/час\n⏰ Время: %d %s\n💵 К оплате: %.0f лей\n\n⏳ Ожидайте подтверждения от администратора.\n🆔 ID заявки: `%s`",
			c.Name, c.RentalPrice, days, daysLabel(days), float64(selectedHours)*c.RentalPrice, requestID)
		editOrSend(call, resp, nil)
	} else {
		// Instant rental
		rentalID := createRentalDirect(userID, consoleID, selectedHours, nil)
		days := selectedHours / 24
		endTime := time.Now().Add(time.Duration(selectedHours) * time.Hour)
		resp := fmt.Sprintf("✅ Консоль *%s* арендована!\n\n🆔 ID аренды: `%s`\n💰 Цена: %.0f лей/час\n⏰ Время аренды: %d %s\n💵 К оплате: %.0f лей\n🕐 Окончание: %s\n\nДля завершения аренды используйте команду /end с ID аренды",
			c.Name, rentalID, c.RentalPrice, days, daysLabel(days),
			float64(selectedHours)*c.RentalPrice, endTime.Format("2006-01-02 15:04"))
		editOrSend(call, resp, nil)
	}
}

// ─── buy_ ────────────────────────────────────────────────────────────────────

func handleBuyCallback(call *tgbotapi.CallbackQuery) {
	consoleID := strings.TrimPrefix(call.Data, "buy_")
	consoles := storage.LoadConsoles()
	c, ok := consoles[consoleID]
	if !ok || c.Status != "available" {
		answerCallback(call.ID, "❌ Консоль недоступна", false)
		return
	}

	adminID := storage.GetAdminChatID()
	resp := fmt.Sprintf("💰 *Покупка консоли*\n\n🎮 Консоль: %s (%s)\n💵 Цена: %.0f лей\n\nДля покупки обратитесь к администратору:\nTelegram ID: %s\nID консоли: `%s`",
		c.Name, c.Model, c.SalePrice, adminID, consoleID)
	editOrSend(call, resp, nil)
}

// ─── end_rental_ ─────────────────────────────────────────────────────────────

func handleEndRentalCallback(call *tgbotapi.CallbackQuery, userID string) {
	rentalID := strings.TrimPrefix(call.Data, "end_rental_")

	if isUserBanned(userID) {
		answerCallback(call.ID, "❌ Ваш аккаунт заблокирован.", false)
		return
	}

	result := endRentalByID(userID, rentalID)
	if result["success"].(bool) {
		hours := result["hours"].(int)
		cost := result["total_cost"].(float64)
		name := result["console_name"].(string)

		resp := fmt.Sprintf("✅ *Аренда завершена!*\n\n🎮 Консоль: %s\n⏰ Длительность: %d ч.\n💰 К оплате: %.0f лей\n\nСпасибо за использование нашего сервиса!", name, hours, cost)
		editOrSend(call, resp, nil)

		userName := result["user_name"].(string)
		userPhone := result["user_phone"].(string)
		adminMsg := fmt.Sprintf("🏁 *Аренда завершена пользователем*\n\n👤 %s\n📱 %s\n🎮 %s\n⏰ %d ч.\n💰 %.0f лей",
			userName, userPhone, name, hours, cost)
		notifyAdmin(adminMsg)
	} else {
		answerCallback(call.ID, fmt.Sprintf("❌ %s", result["error"]), false)
	}
}

// ─── back_to_selection ───────────────────────────────────────────────────────

func handleBackToSelection(call *tgbotapi.CallbackQuery, userID string) {
	if isUserBanned(userID) {
		answerCallback(call.ID, "❌ Ваш аккаунт заблокирован.", false)
		return
	}

	consoles := storage.LoadConsoles()
	var rows [][]tgbotapi.InlineKeyboardButton
	for id, c := range consoles {
		var btn tgbotapi.InlineKeyboardButton
		if c.Status == "available" {
			btn = tgbotapi.NewInlineKeyboardButtonData(
				fmt.Sprintf("%s - %.0f лей/час", c.Name, c.RentalPrice), "console_"+id)
		} else {
			btn = tgbotapi.NewInlineKeyboardButtonData(
				fmt.Sprintf("%s - Занята", c.Name), "console_unavailable_"+id)
		}
		rows = append(rows, []tgbotapi.InlineKeyboardButton{btn})
	}
	markup := tgbotapi.InlineKeyboardMarkup{InlineKeyboard: rows}
	editOrSend(call, "Выберите консоль для аренды:\n\n", markup)
}

// ─── approve/reject ──────────────────────────────────────────────────────────

func handleApproveRequest(call *tgbotapi.CallbackQuery, userID string) {
	if !isUserAdmin(userID) {
		answerCallback(call.ID, "❌ У вас нет прав администратора", false)
		return
	}

	requestID := strings.TrimPrefix(call.Data, "approve_")
	requests := storage.LoadRequests()
	req, ok := requests[requestID]
	if !ok {
		answerCallback(call.ID, "❌ Заявка не найдена", false)
		return
	}
	if req.Status != "pending" && req.Status != "pending_approval" {
		answerCallback(call.ID, "❌ Заявка уже обработана", false)
		return
	}

	consoles := storage.LoadConsoles()
	c, ok := consoles[req.ConsoleID]
	if !ok || c.Status != "available" {
		answerCallback(call.ID, "❌ Консоль больше недоступна", false)
		req.Status = "rejected"
		requests[requestID] = req
		_ = storage.SaveRequests(requests)
		return
	}

	req.Status = "approved"
	requests[requestID] = req
	_ = storage.SaveRequests(requests)

	createTempReservation(req.UserID, req.ConsoleID, 30)

	users := storage.LoadUsers()
	user := users[req.UserID]
	fullName := user.FullName
	if fullName == "" {
		fullName = user.FirstName
	}

	// Check existing documents
	existingDocs := checkUserDocuments(fullName)
	allDocsExist := true
	for _, v := range existingDocs {
		if v == false {
			allDocsExist = false
			break
		}
	}

	var userMsg string
	var replyMarkup interface{}
	if allDocsExist {
		user.VerificationStep = "location_request"
		user.PendingRentalID = req.ConsoleID
		users[req.UserID] = user
		_ = storage.SaveUsers(users)

		userMsg = fmt.Sprintf("✅ *Ваша заявка одобрена!*\n\n🎮 Консоль: %s\n💰 Цена: %.0f лей/час\n\n📄 *Паспорт верифицирован* ✅\n(Документы уже загружены ранее)\n\n📍 *Финальный шаг:* Отправьте свою геолокацию для начала аренды\n\n⚠️ *ВАЖНО:* Отправляйте геолокацию только когда будете готовы получить консоль!\nНажмите кнопку ниже для отправки геолокации",
			c.Name, c.RentalPrice)

		kb := tgbotapi.NewReplyKeyboard(
			tgbotapi.NewKeyboardButtonRow(
				tgbotapi.KeyboardButton{Text: "📍 Отправить геолокацию", RequestLocation: true},
			),
		)
		kb.ResizeKeyboard = true
		kb.OneTimeKeyboard = true
		replyMarkup = kb
	} else {
		user.VerificationStep = "passport_front"
		user.PendingRentalID = req.ConsoleID
		users[req.UserID] = user
		_ = storage.SaveUsers(users)

		userMsg = fmt.Sprintf("✅ *Ваша заявка одобрена!*\n\n🎮 Консоль: %s\n💰 Цена: %.0f лей/час\n\n📄 *Для начала аренды необходима верификация документов*\n\n*Шаг 1 из 3:* Отправьте фото *ПЕРЕДНЕЙ стороны паспорта*\n\n⚠️ *Требования к фото:*\n• Четкое изображение без бликов\n• Все данные должны быть читаемыми\n• Фото целиком, без обрезанных краев\n\n📷 Отправьте фото как обычное изображение",
			c.Name, c.RentalPrice)
		replyMarkup = tgbotapi.NewRemoveKeyboard(true)
	}

	var userChatID int64
	fmt.Sscanf(req.UserID, "%d", &userChatID)
	m := tgbotapi.NewMessage(userChatID, userMsg)
	m.ParseMode = "Markdown"
	if replyMarkup != nil {
		m.ReplyMarkup = replyMarkup
	}
	_, _ = botInstance.Send(m)

	docStatus := "📄 Ожидает загрузку документов"
	if allDocsExist {
		docStatus = "📄 Паспорт верифицирован ✅\n📍 Ожидает геолокацию для начала аренды"
	}

	adminUpdate := fmt.Sprintf("✅ *Заявка одобрена*\n\n👤 %s\n🎮 %s\n%s\n⏰ %s",
		fullName, c.Name, docStatus, time.Now().Format("2006-01-02 15:04"))
	edit := tgbotapi.NewEditMessageText(call.Message.Chat.ID, call.Message.MessageID, adminUpdate)
	edit.ParseMode = "Markdown"
	_, _ = botInstance.Send(edit)
}

func handleRejectRequest(call *tgbotapi.CallbackQuery, userID string) {
	if !isUserAdmin(userID) {
		answerCallback(call.ID, "❌ У вас нет прав администратора", false)
		return
	}

	requestID := strings.TrimPrefix(call.Data, "reject_")
	requests := storage.LoadRequests()
	req, ok := requests[requestID]
	if !ok {
		answerCallback(call.ID, "❌ Заявка не найдена", false)
		return
	}
	if req.Status != "pending" && req.Status != "pending_approval" {
		answerCallback(call.ID, "❌ Заявка уже обработана", false)
		return
	}

	req.Status = "rejected"
	removeTempReservation(req.UserID)
	requests[requestID] = req
	_ = storage.SaveRequests(requests)

	users := storage.LoadUsers()
	user := users[req.UserID]
	consoles := storage.LoadConsoles()
	c := consoles[req.ConsoleID]

	user.VerificationStep = ""
	user.PendingRentalID = ""
	users[req.UserID] = user
	_ = storage.SaveUsers(users)

	name := user.FullName
	if name == "" {
		name = user.FirstName
	}

	var userChatID int64
	fmt.Sscanf(req.UserID, "%d", &userChatID)
	userMsg := fmt.Sprintf("❌ *Ваша заявка отклонена*\n\n🎮 Консоль: %s\n⏰ %s\n\nПопробуйте арендовать другую консоль или обратитесь к администратору.",
		c.Name, time.Now().Format("2006-01-02 15:04"))
	m := tgbotapi.NewMessage(userChatID, userMsg)
	m.ParseMode = "Markdown"
	_, _ = botInstance.Send(m)

	adminUpdate := fmt.Sprintf("❌ *Заявка отклонена*\n\n👤 %s\n🎮 %s\n⏰ %s",
		name, c.Name, time.Now().Format("2006-01-02 15:04"))
	edit := tgbotapi.NewEditMessageText(call.Message.Chat.ID, call.Message.MessageID, adminUpdate)
	edit.ParseMode = "Markdown"
	_, _ = botInstance.Send(edit)
}

// ─── Admin panel callbacks ───────────────────────────────────────────────────

func handleAdminRequests(call *tgbotapi.CallbackQuery, userID string) {
	if !isUserAdmin(userID) {
		answerCallback(call.ID, "❌ Нет прав", false)
		return
	}

	requests := storage.LoadRequests()
	var pending []models.RentalRequest
	for _, r := range requests {
		if r.Status == "pending" {
			pending = append(pending, r)
		}
	}

	resp := "📊 *Заявки на аренду*\n\n"
	if len(pending) == 0 {
		resp += "✅ Нет ожидающих заявок"
		editOrSend(call, resp, nil)
		return
	}

	resp += fmt.Sprintf("⏳ Ожидающих заявок: %d\n\n", len(pending))
	users := storage.LoadUsers()
	consoles := storage.LoadConsoles()
	var rows [][]tgbotapi.InlineKeyboardButton

	max := len(pending)
	if max > 5 {
		max = 5
	}
	for _, req := range pending[:max] {
		user := users[req.UserID]
		c := consoles[req.ConsoleID]
		name := user.FullName
		if name == "" {
			name = user.FirstName
		}
		cname := c.Name
		if cname == "" {
			cname = "Неизвестная консоль"
		}

		reqTime := req.RequestTime
		if len(reqTime) > 16 {
			reqTime = reqTime[:16]
		}
		resp += fmt.Sprintf("👤 *%s*\n🎮 %s\n💰 %.0f лей/час\n⏰ %s\n\n",
			name, cname, c.RentalPrice, reqTime)

		short := name
		if len(short) > 10 {
			short = short[:10]
		}
		rows = append(rows, tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("✅ Одобрить "+short, "approve_"+req.ID),
			tgbotapi.NewInlineKeyboardButtonData("❌ Отклонить "+short, "reject_"+req.ID),
		))
	}

	markup := tgbotapi.InlineKeyboardMarkup{InlineKeyboard: rows}
	editOrSend(call, resp, markup)
}

func handleAdminSettings(call *tgbotapi.CallbackQuery, userID string) {
	if !isUserAdmin(userID) {
		answerCallback(call.ID, "❌ Нет прав", false)
		return
	}

	s := storage.LoadSettings()
	approval := "Нет"
	if s.RequireApproval {
		approval = "Да"
	}
	notif := "Отключены"
	if s.NotificationsEnabled {
		notif = "Включены"
	}

	resp := fmt.Sprintf("⚙️ *Настройки системы*\n\n🕒 Макс. время аренды: %d ч.\n⏰ Напоминание за: %d ч.\n✅ Требовать подтверждение: %s\n🔔 Уведомления: %s\n\nДля изменения настроек используйте веб-панель",
		s.MaxRentalHours, s.ReminderHours, approval, notif)
	editOrSend(call, resp, nil)
}

func handleAdminUsersCallback(call *tgbotapi.CallbackQuery, userID string) {
	if !isUserAdmin(userID) {
		answerCallback(call.ID, "❌ Нет прав", false)
		return
	}

	users := storage.LoadUsers()
	resp := fmt.Sprintf("👥 *Управление пользователями*\n\nВсего пользователей: %d\n\n", len(users))

	type kv struct {
		id   string
		user models.User
	}
	var sorted []kv
	for id, u := range users {
		sorted = append(sorted, kv{id, u})
	}

	var rows [][]tgbotapi.InlineKeyboardButton
	max := len(sorted)
	if max > 10 {
		max = 10
	}
	for _, item := range sorted[:max] {
		status := "✅"
		if item.user.IsBanned {
			status = "🚫"
		}
		name := item.user.FullName
		if name == "" {
			name = item.user.FirstName
		}
		if name == "" {
			name = "Неизвестный"
		}
		if len(name) > 20 {
			name = name[:20]
		}
		rows = append(rows, tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData(status+" "+name, "user_manage_"+item.id),
		))
	}

	markup := tgbotapi.InlineKeyboardMarkup{InlineKeyboard: rows}
	editOrSend(call, resp, markup)
}

func handleAdminRatings(call *tgbotapi.CallbackQuery, userID string) {
	if !isUserAdmin(userID) {
		answerCallback(call.ID, "❌ Нет прав", false)
		return
	}

	users := storage.LoadUsers()
	r := storage.LoadRatings()

	resp := fmt.Sprintf("⭐ *Управление рейтингами*\n\nВсего пользователей: %d\nПользователей с рейтингом: %d\n\nВыберите пользователя для управления рейтингом:",
		len(users), len(r.UserRatings))

	type kv struct {
		id   string
		user models.User
	}
	var sorted []kv
	for id, u := range users {
		sorted = append(sorted, kv{id, u})
	}

	var rows [][]tgbotapi.InlineKeyboardButton
	max := len(sorted)
	if max > 10 {
		max = 10
	}
	for _, item := range sorted[:max] {
		name := item.user.FullName
		if name == "" {
			name = item.user.FirstName
		}
		if name == "" {
			name = "Неизвестный"
		}
		label := fmt.Sprintf("➖ %s (без рейтинга)", name)
		if rating := calculateUserFinalRating(item.id); rating != nil {
			label = fmt.Sprintf("⭐ %s (%d)", name, rating.FinalScore)
		}
		if len(label) > 30 {
			label = label[:30]
		}
		rows = append(rows, tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData(label, "rating_manage_"+item.id),
		))
	}
	rows = append(rows, tgbotapi.NewInlineKeyboardRow(
		tgbotapi.NewInlineKeyboardButtonData("📊 Статистика рейтингов", "rating_stats"),
	))

	markup := tgbotapi.InlineKeyboardMarkup{InlineKeyboard: rows}
	editOrSend(call, resp, markup)
}

func handleAdminWebInfo(call *tgbotapi.CallbackQuery, userID string) {
	if !isUserAdmin(userID) {
		answerCallback(call.ID, "❌ Нет прав", false)
		return
	}

	resp := "🌐 *Веб-панель администратора*\n\nДля доступа к полной веб-панели управления:\n\n1️⃣ Откройте браузер\n2️⃣ Перейдите по адресу: `http://localhost:5000`\n3️⃣ Войдите как администратор\n\nВ веб-панели доступно:\n• Управление консолями\n• Просмотр заявок на аренду\n• Настройки системы\n• Детальная статистика"
	editOrSend(call, resp, nil)
}

func handleUserManage(call *tgbotapi.CallbackQuery, userID string) {
	if !isUserAdmin(userID) {
		answerCallback(call.ID, "❌ Нет прав", false)
		return
	}

	targetID := strings.TrimPrefix(call.Data, "user_manage_")
	users := storage.LoadUsers()
	u, ok := users[targetID]
	if !ok {
		answerCallback(call.ID, "❌ Пользователь не найден", false)
		return
	}

	name := u.FullName
	if name == "" {
		name = u.FirstName
	}

	rentals := storage.LoadRentals()
	count := 0
	spent := 0.0
	for _, r := range rentals {
		if r.UserID == targetID {
			count++
			if r.Status == "completed" {
				spent += r.TotalCost
			}
		}
	}

	status := "✅ Активен"
	if u.IsBanned {
		status = "🚫 Заблокирован"
	}

	resp := fmt.Sprintf("👤 *Пользователь:* %s\n🆔 ID: `%s`\n📱 Телефон: %s\n📊 Аренд: %d\n💰 Потрачено: %.0f лей\n🔒 Статус: %s",
		name, targetID, u.PhoneNumber, count, spent, status)

	var rows [][]tgbotapi.InlineKeyboardButton
	if u.IsBanned {
		rows = append(rows, tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("✅ Разблокировать", "unban_user_"+targetID),
		))
	} else {
		rows = append(rows, tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("🚫 Заблокировать", "ban_user_"+targetID),
		))
	}
	rows = append(rows, tgbotapi.NewInlineKeyboardRow(
		tgbotapi.NewInlineKeyboardButtonData("⬅️ Назад", "admin_users"),
	))

	markup := tgbotapi.InlineKeyboardMarkup{InlineKeyboard: rows}
	editOrSend(call, resp, markup)
}

func handleBanUser(call *tgbotapi.CallbackQuery, adminID string) {
	if !isUserAdmin(adminID) {
		answerCallback(call.ID, "❌ Нет прав", false)
		return
	}
	targetID := strings.TrimPrefix(call.Data, "ban_user_")
	users := storage.LoadUsers()
	if u, ok := users[targetID]; ok {
		u.IsBanned = true
		users[targetID] = u
		_ = storage.SaveUsers(users)
		answerCallback(call.ID, "✅ Пользователь заблокирован", false)
		handleUserManage(call, adminID)
	}
}

func handleUnbanUser(call *tgbotapi.CallbackQuery, adminID string) {
	if !isUserAdmin(adminID) {
		answerCallback(call.ID, "❌ Нет прав", false)
		return
	}
	targetID := strings.TrimPrefix(call.Data, "unban_user_")
	users := storage.LoadUsers()
	if u, ok := users[targetID]; ok {
		u.IsBanned = false
		users[targetID] = u
		_ = storage.SaveUsers(users)
		answerCallback(call.ID, "✅ Пользователь разблокирован", false)
		handleUserManage(call, adminID)
	}
}

func handleRatingManage(call *tgbotapi.CallbackQuery, adminID string) {
	if !isUserAdmin(adminID) {
		answerCallback(call.ID, "❌ Нет прав", false)
		return
	}
	targetID := strings.TrimPrefix(call.Data, "rating_manage_")
	rating := calculateUserFinalRating(targetID)

	users := storage.LoadUsers()
	u := users[targetID]
	name := u.FullName
	if name == "" {
		name = u.FirstName
	}

	var resp string
	if rating == nil {
		resp = fmt.Sprintf("⭐ *Рейтинг пользователя:* %s\n\nРейтинг не рассчитан", name)
	} else {
		resp = fmt.Sprintf("⭐ *Рейтинг пользователя:* %s\n\n📊 Итоговый рейтинг: %d\n🏆 Дисциплина: %d\n💝 Лояльность: %d\n🎖 Статус: %s",
			name, rating.FinalScore, rating.Discipline, rating.Loyalty, rating.StatusName)
	}

	markup := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("⬅️ Назад", "admin_ratings"),
		),
	)
	editOrSend(call, resp, markup)
}

func handleRatingStats(call *tgbotapi.CallbackQuery, adminID string) {
	if !isUserAdmin(adminID) {
		answerCallback(call.ID, "❌ Нет прав", false)
		return
	}

	users := storage.LoadUsers()
	premium, regular, risk := 0, 0, 0
	for uid := range users {
		r := calculateUserFinalRating(uid)
		if r != nil {
			switch r.Status {
			case "premium":
				premium++
			case "regular":
				regular++
			case "risk":
				risk++
			}
		}
	}

	resp := fmt.Sprintf("📊 *Статистика рейтингов*\n\n⭐ Premium: %d\n👤 Обычный: %d\n⚠️ Риск: %d\n\nВсего пользователей: %d",
		premium, regular, risk, len(users))
	editOrSend(call, resp, nil)
}

// ─── Admin message handlers ──────────────────────────────────────────────────

func handleAdminPanel(msg *tgbotapi.Message) {
	userID := fmt.Sprintf("%d", msg.From.ID)
	if !isUserAdmin(userID) {
		sendMessage(msg.Chat.ID, "❌ У вас нет доступа к админ панели.", nil)
		return
	}

	markup := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("👥 Пользователи", "admin_users"),
			tgbotapi.NewInlineKeyboardButtonData("📊 Заявки на аренду", "admin_requests"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("⭐ Рейтинги", "admin_ratings"),
			tgbotapi.NewInlineKeyboardButtonData("⚙️ Настройки", "admin_settings"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("🎮 Веб-панель", "admin_web_info"),
		),
	)

	m := tgbotapi.NewMessage(msg.Chat.ID, "⚙️ *Админ панель*\n\nВыберите действие:")
	m.ParseMode = "Markdown"
	m.ReplyMarkup = markup
	_, _ = botInstance.Send(m)
}

func handleStatistics(msg *tgbotapi.Message) {
	userID := fmt.Sprintf("%d", msg.From.ID)
	if !isUserAdmin(userID) {
		sendMessage(msg.Chat.ID, "❌ У вас нет доступа к статистике.", nil)
		return
	}

	users := storage.LoadUsers()
	consoles := storage.LoadConsoles()
	rentals := storage.LoadRentals()

	activeRentals := 0
	completedRentals := 0
	availableConsoles := 0
	totalRevenue := 0.0

	for _, r := range rentals {
		if r.Status == "active" {
			activeRentals++
		}
		if r.Status == "completed" {
			completedRentals++
			totalRevenue += r.TotalCost
		}
	}
	for _, c := range consoles {
		if c.Status == "available" {
			availableConsoles++
		}
	}

	resp := fmt.Sprintf("📈 *Статистика системы*\n\n👥 Всего пользователей: %d\n🎮 Всего консолей: %d\n✅ Доступных консолей: %d\n🔄 Активных аренд: %d\n✅ Завершенных аренд: %d\n💰 Общая выручка: %.0f лей",
		len(users), len(consoles), availableConsoles, activeRentals, completedRentals, totalRevenue)

	if activeRentals > 0 {
		resp += "\n\n*Активные аренды:*\n"
		count := 0
		for _, r := range rentals {
			if r.Status == "active" && count < 5 {
				c := consoles[r.ConsoleID]
				u := users[r.UserID]
				name := u.FullName
				if name == "" {
					name = u.FirstName
				}
				resp += fmt.Sprintf("• %s - %s\n", c.Name, name)
				count++
			}
		}
	}

	kb := adminKeyboard()
	kb.ResizeKeyboard = true
	m := tgbotapi.NewMessage(msg.Chat.ID, resp)
	m.ParseMode = "Markdown"
	m.ReplyMarkup = kb
	_, _ = botInstance.Send(m)
}

func handleUsersAdmin(msg *tgbotapi.Message) {
	userID := fmt.Sprintf("%d", msg.From.ID)
	if !isUserAdmin(userID) {
		sendMessage(msg.Chat.ID, "❌ У вас нет доступа.", nil)
		return
	}

	users := storage.LoadUsers()
	resp := fmt.Sprintf("👥 *Управление пользователями*\n\nВсего пользователей: %d\n\nВыберите пользователя для управления:", len(users))

	type kv struct {
		id   string
		user models.User
	}
	var sorted []kv
	for id, u := range users {
		sorted = append(sorted, kv{id, u})
	}

	var rows [][]tgbotapi.InlineKeyboardButton
	max := len(sorted)
	if max > 10 {
		max = 10
	}
	for _, item := range sorted[:max] {
		status := "✅"
		if item.user.IsBanned {
			status = "🚫"
		}
		name := item.user.FullName
		if name == "" {
			name = item.user.FirstName
		}
		if name == "" {
			name = "Неизвестный"
		}
		if len(name) > 20 {
			name = name[:20]
		}
		rows = append(rows, tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData(status+" "+name, "user_manage_"+item.id),
		))
	}

	markup := tgbotapi.InlineKeyboardMarkup{InlineKeyboard: rows}
	m := tgbotapi.NewMessage(msg.Chat.ID, resp)
	m.ParseMode = "Markdown"
	m.ReplyMarkup = markup
	_, _ = botInstance.Send(m)
}

func handleNotifications(msg *tgbotapi.Message) {
	userID := fmt.Sprintf("%d", msg.From.ID)
	if !isUserAdmin(userID) {
		sendMessage(msg.Chat.ID, "❌ У вас нет доступа.", nil)
		return
	}

	requests := storage.LoadRequests()
	var pending []models.RentalRequest
	for _, r := range requests {
		if r.Status == "pending" {
			pending = append(pending, r)
		}
	}

	resp := "🔔 *Уведомления*\n\n"
	if len(pending) > 0 {
		resp += fmt.Sprintf("⏳ Ожидающих заявок: %d\n\n", len(pending))
		users := storage.LoadUsers()
		consoles := storage.LoadConsoles()
		max := len(pending)
		if max > 5 {
			max = 5
		}
		for _, req := range pending[:max] {
			u := users[req.UserID]
			c := consoles[req.ConsoleID]
			name := u.FullName
			if name == "" {
				name = u.FirstName
			}
			resp += fmt.Sprintf("• %s - %s\n", name, c.Name)
		}

		markup := tgbotapi.NewInlineKeyboardMarkup(
			tgbotapi.NewInlineKeyboardRow(
				tgbotapi.NewInlineKeyboardButtonData("📋 Перейти к заявкам", "admin_requests"),
			),
		)
		m := tgbotapi.NewMessage(msg.Chat.ID, resp)
		m.ParseMode = "Markdown"
		m.ReplyMarkup = markup
		_, _ = botInstance.Send(m)
	} else {
		resp += "✅ Нет ожидающих заявок"
		sendMessage(msg.Chat.ID, resp, nil)
	}
}

// ─── Utility ─────────────────────────────────────────────────────────────────

// editOrSend tries to edit an existing message; falls back to sending a new one
func editOrSend(call *tgbotapi.CallbackQuery, text string, markup interface{}) {
	if call.Message.Photo != nil {
		del := tgbotapi.NewDeleteMessage(call.Message.Chat.ID, call.Message.MessageID)
		_, _ = botInstance.Request(del)

		m := tgbotapi.NewMessage(call.Message.Chat.ID, text)
		m.ParseMode = "Markdown"
		if markup != nil {
			switch mk := markup.(type) {
			case tgbotapi.InlineKeyboardMarkup:
				m.ReplyMarkup = mk
			}
		}
		_, _ = botInstance.Send(m)
		return
	}

	edit := tgbotapi.NewEditMessageText(call.Message.Chat.ID, call.Message.MessageID, text)
	edit.ParseMode = "Markdown"
	if markup != nil {
		switch mk := markup.(type) {
		case tgbotapi.InlineKeyboardMarkup:
			edit.ReplyMarkup = &mk
		}
	}
	if _, err := botInstance.Send(edit); err != nil {
		// Fallback
		del := tgbotapi.NewDeleteMessage(call.Message.Chat.ID, call.Message.MessageID)
		_, _ = botInstance.Request(del)
		m := tgbotapi.NewMessage(call.Message.Chat.ID, text)
		m.ParseMode = "Markdown"
		if markup != nil {
			switch mk := markup.(type) {
			case tgbotapi.InlineKeyboardMarkup:
				m.ReplyMarkup = mk
			}
		}
		_, _ = botInstance.Send(m)
	}
}
