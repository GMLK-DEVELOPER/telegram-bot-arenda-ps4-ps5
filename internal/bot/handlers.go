package bot

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/google/uuid"
	"ps4-rental/internal/config"
	"ps4-rental/internal/models"
	"ps4-rental/internal/storage"
)

// ─── /start ──────────────────────────────────────────────────────────────────

func handleStart(msg *tgbotapi.Message) {
	userID := fmt.Sprintf("%d", msg.From.ID)

	if isUserBanned(userID) {
		sendMessage(msg.Chat.ID, "❌ Ваш аккаунт заблокирован. Обратитесь к администратору.", nil)
		return
	}

	users := storage.LoadUsers()
	user, exists := users[userID]

	if !exists {
		user = models.User{
			ID:               userID,
			Username:         msg.From.UserName,
			FirstName:        msg.From.FirstName,
			LastName:         msg.From.LastName,
			IsBanned:         false,
			TotalSpent:       0,
			JoinedAt:         time.Now().Format(time.RFC3339),
			RegistrationStep: "phone",
		}
		users[userID] = user
		_ = storage.SaveUsers(users)
	}

	if !isUserRegistered(userID) {
		// Request phone number
		kb := tgbotapi.NewReplyKeyboard(
			tgbotapi.NewKeyboardButtonRow(
				tgbotapi.KeyboardButton{
					Text:           "📱 Отправить номер телефона",
					RequestContact: true,
				},
			),
		)
		kb.ResizeKeyboard = true
		kb.OneTimeKeyboard = true

		m := tgbotapi.NewMessage(msg.Chat.ID,
			"Добро пожаловать в систему аренды PlayStation!\n\nДля продолжения регистрации, пожалуйста, поделитесь своим номером телефона:")
		m.ReplyMarkup = kb
		_, _ = botInstance.Send(m)
		return
	}

	// Already registered
	name := user.FullName
	if name == "" {
		name = user.FirstName
	}
	kb := getKeyboardForUser(userID)
	kb.ResizeKeyboard = true
	m := tgbotapi.NewMessage(msg.Chat.ID, fmt.Sprintf("С возвращением, %s!\n\nВыберите действие:", name))
	m.ReplyMarkup = kb
	_, _ = botInstance.Send(m)
}

// ─── /end <rental_id> ────────────────────────────────────────────────────────

func handleEnd(msg *tgbotapi.Message) {
	userID := fmt.Sprintf("%d", msg.From.ID)

	if isUserBanned(userID) {
		sendMessage(msg.Chat.ID, "❌ Ваш аккаунт заблокирован.", nil)
		return
	}

	parts := strings.Fields(msg.Text)
	if len(parts) < 2 {
		sendMessage(msg.Chat.ID, "❌ Укажите ID аренды: /end <ID аренды>", nil)
		return
	}

	rentalID := parts[1]
	result := endRentalByID(userID, rentalID)

	if result["success"].(bool) {
		hours := result["hours"].(int)
		cost := result["total_cost"].(float64)
		consoleName := result["console_name"].(string)

		resp := fmt.Sprintf("✅ *Аренда завершена!*\n\n🎮 Консоль: %s\n⏰ Длительность: %d ч.\n💰 К оплате: %.0f лей\n\nСпасибо за использование нашего сервиса!", consoleName, hours, cost)
		kb := getKeyboardForUser(userID)
		kb.ResizeKeyboard = true
		m := tgbotapi.NewMessage(msg.Chat.ID, resp)
		m.ParseMode = "Markdown"
		m.ReplyMarkup = kb
		_, _ = botInstance.Send(m)

		// Notify admin
		userName := result["user_name"].(string)
		userPhone := result["user_phone"].(string)
		adminMsg := fmt.Sprintf("🏁 *Аренда завершена пользователем*\n\n👤 %s\n📱 %s\n🎮 %s\n⏰ %d ч.\n💰 %.0f лей",
			userName, userPhone, consoleName, hours, cost)
		notifyAdmin(adminMsg)
	} else {
		sendMessage(msg.Chat.ID, fmt.Sprintf("❌ %s", result["error"]), nil)
	}
}

// ─── /help ───────────────────────────────────────────────────────────────────

func handleHelp(msg *tgbotapi.Message) {
	userID := fmt.Sprintf("%d", msg.From.ID)
	kb := getKeyboardForUser(userID)
	kb.ResizeKeyboard = true

	var text string
	if isUserAdmin(userID) {
		text = `🎮 *Команды бота (Администратор):*

📱 *Основные функции:*
📝 Арендовать - Арендовать консоль

👨‍💼 *Админ функции:*
⚙️ Админ панель - Управление системой
📈 Статистика - Статистика системы
👥 Пользователи - Управление пользователями
🔔 Уведомления - Проверка заявок

🌐 *Веб-панель:* Доступна на порту 5000`
	} else {
		text = `🎮 *Команды бота:*

📱 *Основные функции:*
📊 Мой кабинет - Ваша статистика
📝 Арендовать - Арендовать консоль

🎯 *Игры и модели:*
• PlayStation 4 / PS4 Pro
• PlayStation 5
• Большой выбор игр

💳 *Оплата:*
После завершения аренды обратитесь к администратору`
	}

	m := tgbotapi.NewMessage(msg.Chat.ID, text)
	m.ParseMode = "Markdown"
	m.ReplyMarkup = kb
	_, _ = botInstance.Send(m)
}

// ─── Contact handler ─────────────────────────────────────────────────────────

func handleContact(msg *tgbotapi.Message) {
	userID := fmt.Sprintf("%d", msg.From.ID)
	users := storage.LoadUsers()

	if msg.Contact == nil || msg.Contact.UserID != msg.From.ID {
		sendMessage(msg.Chat.ID, "❌ Отправьте свой собственный номер телефона", nil)
		return
	}

	user, ok := users[userID]
	if !ok {
		sendMessage(msg.Chat.ID, "❌ Пожалуйста, начните с /start", nil)
		return
	}

	user.PhoneNumber = msg.Contact.PhoneNumber
	user.RegistrationStep = "full_name"
	users[userID] = user
	_ = storage.SaveUsers(users)

	m := tgbotapi.NewMessage(msg.Chat.ID,
		fmt.Sprintf("✅ Номер телефона сохранен: %s\n\nТеперь введите ваше полное ФИО:", msg.Contact.PhoneNumber))
	m.ReplyMarkup = tgbotapi.NewRemoveKeyboard(true)
	_, _ = botInstance.Send(m)
}

// ─── Location handler ────────────────────────────────────────────────────────

func handleLocation(msg *tgbotapi.Message) {
	userID := fmt.Sprintf("%d", msg.From.ID)
	users := storage.LoadUsers()
	user, ok := users[userID]
	if !ok {
		return
	}

	lat := msg.Location.Latitude
	lon := msg.Location.Longitude

	// Check if user has a rental awaiting location (sent by admin via "Начать аренду")
	rentals := storage.LoadRentals()
	for rentalID, rental := range rentals {
		if rental.UserID == userID && rental.Status == "awaiting_location" {
			consoles := storage.LoadConsoles()
			console := consoles[rental.ConsoleID]

			hours := rental.SelectedHours
			if hours == 0 {
				hours = rental.DurationHours
			}
			if hours == 0 {
				hours = 1
			}
			now := time.Now()
			expectedEnd := now.Add(time.Duration(hours) * time.Hour)

			rental.Status          = "active"
			rental.StartTime       = now.Format(time.RFC3339)
			rental.ExpectedEndTime = expectedEnd.Format(time.RFC3339)
			rental.Location        = &models.Location{Latitude: float64(lat), Longitude: float64(lon)}
			rentals[rentalID] = rental
			_ = storage.SaveRentals(rentals)

			// Restore normal keyboard
			kb := getKeyboardForUser(userID)
			kb.ResizeKeyboard = true
			resp := fmt.Sprintf(
				"▶️ *Аренда началась!*\n\n🎮 Консоль: *%s*\n⏰ Время: *%d ч.*\n📅 Окончание: *%s*\n💰 Стоимость: *%.0f лей*\n\nПриятной игры! 🎮",
				console.Name, hours, expectedEnd.Format("02.01.2006 в 15:04"), rental.ExpectedCost,
			)
			m := tgbotapi.NewMessage(msg.Chat.ID, resp)
			m.ParseMode = "Markdown"
			m.ReplyMarkup = kb
			_, _ = botInstance.Send(m)

			notifyAdmin(fmt.Sprintf("▶️ *Аренда началась*\n\n🎮 %s\n👤 %s\n📍 %.6f, %.6f\n⏰ %s",
				console.Name, userID, lat, lon, now.Format("02.01 15:04")))
			return
		}
	}

	// Find approved request for this user
	requests := storage.LoadRequests()
	var approvedReq *models.RentalRequest
	for _, r := range requests {
		if r.UserID == userID && r.Status == "approved" {
			rCopy := r
			approvedReq = &rCopy
			break
		}
	}

	if approvedReq != nil && user.VerificationStep == "location_request" {
		// Start rental
		consoleID := approvedReq.ConsoleID
		locData := &models.Location{Latitude: float64(lat), Longitude: float64(lon)}
		rentalID := createRentalDirect(userID, consoleID, approvedReq.SelectedHours, locData)

		// Update request
		req := requests[approvedReq.ID]
		req.Status = "completed"
		req.RentalID = rentalID
		requests[approvedReq.ID] = req
		_ = storage.SaveRequests(requests)

		user.VerificationStep = "completed"
		removeTempReservation(userID)
		users[userID] = user
		_ = storage.SaveUsers(users)

		consoles := storage.LoadConsoles()
		console := consoles[consoleID]

		resp := fmt.Sprintf("✅ *Аренда началась!*\n\n🎮 Консоль: %s\n💰 Цена: %.0f лей/час\n🆔 ID аренды: `%s`\n📍 Геолокация получена\n📄 Документы верифицированы\n⏰ Время начала: %s\n\nДля завершения аренды используйте /end %s",
			console.Name, console.RentalPrice, rentalID, time.Now().Format("2006-01-02 15:04"), rentalID)

		kb := getKeyboardForUser(userID)
		kb.ResizeKeyboard = true
		m := tgbotapi.NewMessage(msg.Chat.ID, resp)
		m.ParseMode = "Markdown"
		m.ReplyMarkup = kb
		_, _ = botInstance.Send(m)

		name := user.FullName
		if name == "" {
			name = user.FirstName
		}
		adminMsg := fmt.Sprintf("🎮 *Аренда началась (с верификацией документов)*\n\n👤 %s\n📱 %s\n🎮 %s\n💰 %.0f лей/час\n📍 Геолокация: %.6f, %.6f\n🆔 ID аренды: `%s`\n\n📄 *Документы сохранены:*\n• Паспорт (лицо): %s\n• Паспорт (оборот): %s\n• Селфи с паспортом: %s\n\n⏰ %s",
			name, user.PhoneNumber, console.Name, console.RentalPrice,
			lat, lon, rentalID,
			user.PassportFrontFile, user.PassportBackFile, user.SelfieFile,
			time.Now().Format("2006-01-02 15:04"))
		notifyAdmin(adminMsg)
	} else {
		// Just forward location to admin
		name := user.FullName
		if name == "" {
			name = user.FirstName
		}
		resp := fmt.Sprintf("📍 *Геолокация отправлена администратору*\n\n⏰ Время: %s\n📍 Широта: %f\n📍 Долгота: %f",
			time.Now().Format("2006-01-02 15:04"), lat, lon)
		kb := getKeyboardForUser(userID)
		kb.ResizeKeyboard = true
		m := tgbotapi.NewMessage(msg.Chat.ID, resp)
		m.ParseMode = "Markdown"
		m.ReplyMarkup = kb
		_, _ = botInstance.Send(m)

		adminMsg := fmt.Sprintf("📍 *Получена геолокация от пользователя*\n\n👤 %s\n📱 %s\n🆔 ID: `%s`\n📍 Координаты: %f, %f\n⏰ %s\n\n[Открыть на карте](https://www.google.com/maps?q=%f,%f)",
			name, user.PhoneNumber, userID, lat, lon,
			time.Now().Format("2006-01-02 15:04"), lat, lon)
		notifyAdmin(adminMsg)
	}
}

// ─── Photo handler (documents) ───────────────────────────────────────────────

func handlePhoto(msg *tgbotapi.Message) {
	userID := fmt.Sprintf("%d", msg.From.ID)
	users := storage.LoadUsers()
	user, ok := users[userID]
	if !ok {
		sendMessage(msg.Chat.ID, "❌ Пользователь не найден. Выполните /start", nil)
		return
	}

	if isUserBanned(userID) {
		sendMessage(msg.Chat.ID, "❌ Ваш аккаунт заблокирован.", nil)
		return
	}

	step := user.VerificationStep
	if step == "" || (step != "passport_front" && step != "passport_back" && step != "selfie_with_passport") {
		sendMessage(msg.Chat.ID, "🤔 Отправка фото не требуется в данный момент.", getKeyboardForUser(userID))
		return
	}

	fullName := user.FullName
	if fullName == "" {
		fullName = user.FirstName
	}
	if fullName == "" {
		fullName = "user_" + userID
	}

	// Get largest photo
	photos := msg.Photo
	if len(photos) == 0 {
		sendMessage(msg.Chat.ID, "❌ Фото не найдено", nil)
		return
	}
	photo := photos[len(photos)-1]

	// Save photo
	result, filename, err := savePhotoDocument(photo.FileID, fullName, step)
	if err != nil || !result {
		sendMessage(msg.Chat.ID, fmt.Sprintf("❌ Ошибка сохранения фото: %v", err), nil)
		return
	}

	var resp string
	switch step {
	case "passport_front":
		user.VerificationStep = "passport_back"
		user.PassportFrontFile = filename
		resp = "✅ *Фото передней стороны паспорта сохранено!*\n\n*Шаг 2 из 3:* Теперь отправьте фото *ЗАДНЕЙ стороны паспорта*\n\n⚠️ *Требования к фото:*\n• Четкое изображение без бликов\n• Все данные должны быть читаемыми\n• Фото целиком, без обрезанных краев\n\n📷 Отправьте фото как обычное изображение"

	case "passport_back":
		user.VerificationStep = "selfie_with_passport"
		user.PassportBackFile = filename
		resp = "✅ *Фото задней стороны паспорта сохранено!*\n\n*Шаг 3 из 3:* Теперь отправьте *СЕЛФИ с паспортом*\n\n⚠️ *Требования к селфи:*\n• Ваше лицо и паспорт должны быть четко видны\n• Держите паспорт открытым на странице с фото\n• Хорошее освещение\n• Смотрите в камеру\n\n📷 Отправьте селфи как обычное изображение"

	case "selfie_with_passport":
		user.VerificationStep = "location_request"
		user.SelfieFile = filename
		resp = "✅ *Селфи с паспортом сохранено!*\n\n🎉 *Верификация документов завершена!*\n\n📍 *Финальный шаг:* Отправьте свою геолокацию для начала аренды\n\n⚠️ *ВАЖНО:* Отправляйте геолокацию только когда будете готовы получить консоль!\nНажмите кнопку ниже для отправки геолокации"

		locKb := tgbotapi.NewReplyKeyboard(
			tgbotapi.NewKeyboardButtonRow(
				tgbotapi.KeyboardButton{
					Text:            "📍 Отправить геолокацию",
					RequestLocation: true,
				},
			),
		)
		locKb.ResizeKeyboard = true
		locKb.OneTimeKeyboard = true

		users[userID] = user
		_ = storage.SaveUsers(users)

		m := tgbotapi.NewMessage(msg.Chat.ID, resp)
		m.ParseMode = "Markdown"
		m.ReplyMarkup = locKb
		_, _ = botInstance.Send(m)

		// Notify admin
		adminMsg := fmt.Sprintf("📄 *Верификация документов завершена*\n\n👤 %s\n📱 %s\n🆔 ID: `%s`\n\n📁 *Сохраненные документы:*\n• Паспорт (лицо): %s\n• Паспорт (оборот): %s\n• Селфи с паспортом: %s\n\n⏳ Ожидает отправки геолокации для начала аренды",
			fullName, user.PhoneNumber, userID,
			user.PassportFrontFile, user.PassportBackFile, filename)
		notifyAdmin(adminMsg)
		return
	}

	users[userID] = user
	_ = storage.SaveUsers(users)

	m := tgbotapi.NewMessage(msg.Chat.ID, resp)
	m.ParseMode = "Markdown"
	_, _ = botInstance.Send(m)

	// Notify admin about document
	stepNames := map[string]string{
		"passport_front":       "фото передней стороны паспорта",
		"passport_back":        "фото задней стороны паспорта",
		"selfie_with_passport": "селфи с паспортом",
	}
	adminMsg := fmt.Sprintf("📄 *Получен документ*\n\n👤 %s\n📱 %s\n🆔 ID: `%s`\n📁 Тип: %s\n💾 Файл: %s\n⏰ %s",
		fullName, user.PhoneNumber, userID,
		stepNames[step], filename, time.Now().Format("2006-01-02 15:04"))
	notifyAdmin(adminMsg)
}

func savePhotoDocument(fileID, fullName, docType string) (bool, string, error) {
	fileConfig := tgbotapi.FileConfig{FileID: fileID}
	fileInfo, err := botInstance.GetFile(fileConfig)
	if err != nil {
		return false, "", err
	}

	// Download file
	url := fmt.Sprintf("https://api.telegram.org/file/bot%s/%s", config.App.TelegramBotToken, fileInfo.FilePath)
	resp, err := http.Get(url)
	if err != nil {
		return false, "", err
	}
	defer resp.Body.Close()

	// Create user folder
	safeName := sanitizeName(fullName)
	userFolder := filepath.Join(config.App.PassportDir, safeName)
	if err := os.MkdirAll(userFolder, 0755); err != nil {
		return false, "", err
	}

	ext := "jpg"
	parts := strings.Split(fileInfo.FilePath, ".")
	if len(parts) > 1 {
		ext = parts[len(parts)-1]
	}

	filename := docType + "." + ext
	filePath := filepath.Join(userFolder, filename)

	f, err := os.Create(filePath)
	if err != nil {
		return false, "", err
	}
	defer f.Close()

	if _, err := io.Copy(f, resp.Body); err != nil {
		return false, "", err
	}

	return true, filepath.Join(safeName, filename), nil
}

// ─── Text message handlers ───────────────────────────────────────────────────

func handleFullName(msg *tgbotapi.Message) {
	userID := fmt.Sprintf("%d", msg.From.ID)
	users := storage.LoadUsers()
	user, ok := users[userID]
	if !ok {
		return
	}

	fullName := strings.TrimSpace(msg.Text)
	if len(fullName) < 2 {
		sendMessage(msg.Chat.ID, "❌ Пожалуйста, введите корректное ФИО (минимум 2 символа)", nil)
		return
	}

	user.FullName = fullName
	user.RegistrationStep = "completed"
	users[userID] = user
	_ = storage.SaveUsers(users)

	kb := getKeyboardForUser(userID)
	kb.ResizeKeyboard = true
	text := fmt.Sprintf("✅ Регистрация завершена!\n\n👤 ФИО: %s\n📱 Телефон: %s\n\nДобро пожаловать в систему аренды PlayStation консолей!\nВыберите действие:",
		fullName, user.PhoneNumber)
	m := tgbotapi.NewMessage(msg.Chat.ID, text)
	m.ReplyMarkup = kb
	_, _ = botInstance.Send(m)
}

func handleMyProfile(msg *tgbotapi.Message) {
	userID := fmt.Sprintf("%d", msg.From.ID)

	if isUserBanned(userID) {
		sendMessage(msg.Chat.ID, "❌ Ваш аккаунт заблокирован.", nil)
		return
	}
	if !isUserRegistered(userID) {
		sendMessage(msg.Chat.ID, "❌ Пожалуйста, завершите регистрацию с помощью команды /start", nil)
		return
	}

	users := storage.LoadUsers()
	user, ok := users[userID]
	if !ok {
		sendMessage(msg.Chat.ID, "❌ Пользователь не найден. Выполните /start", nil)
		return
	}

	rentals := storage.LoadRentals()
	var userRentals []models.Rental
	var activeRentals []models.Rental
	for _, r := range rentals {
		if r.UserID == userID {
			userRentals = append(userRentals, r)
			if r.Status == "active" {
				activeRentals = append(activeRentals, r)
			}
		}
	}

	name := user.FullName
	if name == "" {
		name = user.FirstName
	}
	resp := fmt.Sprintf("👤 *Ваш профиль:*\n\n🆔 ID: `%s`\n👤 ФИО: %s\n📱 Телефон: %s\n",
		userID, name, user.PhoneNumber)
	if user.Username != "" {
		resp += fmt.Sprintf("📞 Username: @%s\n", user.Username)
	}
	joinDate := user.JoinedAt
	if len(joinDate) >= 10 {
		joinDate = joinDate[:10]
	}
	resp += fmt.Sprintf("📅 Регистрация: %s\n📊 Всего аренд: %d\n💰 Потрачено: %.0f лей\n🔄 Активных аренд: %d\n",
		joinDate, len(userRentals), user.TotalSpent, len(activeRentals))

	if len(activeRentals) > 0 {
		resp += "\n*Активные аренды:*\n"
		consoles := storage.LoadConsoles()

		markup := tgbotapi.NewInlineKeyboardMarkup()
		var rows [][]tgbotapi.InlineKeyboardButton

		for _, rental := range activeRentals {
			console := consoles[rental.ConsoleID]
			startTime, _ := time.Parse(time.RFC3339, rental.StartTime)
			if startTime.IsZero() {
				startTime, _ = time.Parse("2006-01-02T15:04:05", rental.StartTime)
			}
			dur := time.Since(startTime)
			hours := int(dur.Hours())
			minutes := int(dur.Minutes()) % 60

			shortID := rental.ID
			if len(shortID) > 8 {
				shortID = shortID[:8]
			}
			resp += fmt.Sprintf("• %s\n  ⏰ Время: %dч %dм\n  💰 Текущая стоимость: %.0f лей\n  🆔 ID: `%s...`\n",
				console.Name, hours, minutes,
				float64(hours)*console.RentalPrice, shortID)

			rows = append(rows, tgbotapi.NewInlineKeyboardRow(
				tgbotapi.NewInlineKeyboardButtonData(
					fmt.Sprintf("🏁 Завершить %s", console.Name),
					"end_rental_"+rental.ID,
				),
			))
		}
		markup.InlineKeyboard = rows

		m := tgbotapi.NewMessage(msg.Chat.ID, resp)
		m.ParseMode = "Markdown"
		m.ReplyMarkup = markup
		_, _ = botInstance.Send(m)
		return
	}

	// Rental history (last 5 completed)
	var completedRentals []models.Rental
	for _, r := range userRentals {
		if r.Status == "completed" {
			completedRentals = append(completedRentals, r)
		}
	}
	// Sort by end time descending (simple: last added last, so reverse)
	if len(completedRentals) > 0 {
		consoles := storage.LoadConsoles()
		resp += "\n📋 *История аренд:*\n"
		shown := 0
		for i := len(completedRentals) - 1; i >= 0 && shown < 5; i-- {
			r := completedRentals[i]
			con := consoles[r.ConsoleID]
			endDate := r.EndTime
			if len(endDate) >= 10 {
				endDate = endDate[:10]
			}
			resp += fmt.Sprintf("• %s — %.0f лей (%s)\n", con.Name, r.TotalCost, endDate)
			shown++
		}
	}

	kb := getKeyboardForUser(userID)
	kb.ResizeKeyboard = true
	m := tgbotapi.NewMessage(msg.Chat.ID, resp)
	m.ParseMode = "Markdown"
	m.ReplyMarkup = kb
	_, _ = botInstance.Send(m)
}

func handleRentalMenu(msg *tgbotapi.Message) {
	userID := fmt.Sprintf("%d", msg.From.ID)

	if isUserBanned(userID) {
		sendMessage(msg.Chat.ID, "❌ Ваш аккаунт заблокирован.", nil)
		return
	}
	if !isUserRegistered(userID) {
		sendMessage(msg.Chat.ID, "❌ Пожалуйста, завершите регистрацию с помощью команды /start", nil)
		return
	}

	consoles := storage.LoadConsoles()
	if len(consoles) == 0 {
		kb := getKeyboardForUser(userID)
		kb.ResizeKeyboard = true
		m := tgbotapi.NewMessage(msg.Chat.ID, "📭 Консоли пока недоступны")
		m.ReplyMarkup = kb
		_, _ = botInstance.Send(m)
		return
	}

	var rows [][]tgbotapi.InlineKeyboardButton
	for consoleID, console := range consoles {
		if console.Status == "available" {
			isReserved, _ := isConsoleTempReserved(consoleID, userID)
			var btn tgbotapi.InlineKeyboardButton
			if isReserved {
				btn = tgbotapi.NewInlineKeyboardButtonData(
					fmt.Sprintf("⏳ %s - Временно недоступно", console.Name),
					"reserved_"+consoleID,
				)
			} else {
				btn = tgbotapi.NewInlineKeyboardButtonData(
					fmt.Sprintf("🟢 %s - %.0f лей/час", console.Name, console.RentalPrice),
					"console_"+consoleID,
				)
			}
			rows = append(rows, []tgbotapi.InlineKeyboardButton{btn})
		} else {
			info := getConsoleRentalInfo(consoleID)
			var label string
			if info != nil {
				start := info["start_time"].(time.Time)
				end := info["estimated_end_time"].(time.Time)
				label = fmt.Sprintf("🔴 %s - Занята с %s до %s",
					console.Name, start.Format("02.01"), end.Format("02.01"))
			} else {
				label = fmt.Sprintf("🔴 %s - Занята", console.Name)
			}
			rows = append(rows, []tgbotapi.InlineKeyboardButton{
				tgbotapi.NewInlineKeyboardButtonData(label, "console_unavailable_"+consoleID),
			})
		}
	}

	markup := tgbotapi.InlineKeyboardMarkup{InlineKeyboard: rows}
	m := tgbotapi.NewMessage(msg.Chat.ID, "Выберите консоль для аренды:\n\n")
	m.ReplyMarkup = markup
	_, _ = botInstance.Send(m)
}

func handleBuyMenu(msg *tgbotapi.Message) {
	userID := fmt.Sprintf("%d", msg.From.ID)

	if isUserBanned(userID) {
		sendMessage(msg.Chat.ID, "❌ Ваш аккаунт заблокирован.", nil)
		return
	}

	consoles := storage.LoadConsoles()
	var rows [][]tgbotapi.InlineKeyboardButton
	for id, c := range consoles {
		if c.SalePrice > 0 && c.Status == "available" {
			rows = append(rows, []tgbotapi.InlineKeyboardButton{
				tgbotapi.NewInlineKeyboardButtonData(
					fmt.Sprintf("%s - %.0f лей", c.Name, c.SalePrice),
					"buy_"+id,
				),
			})
		}
	}

	if len(rows) == 0 {
		kb := getKeyboardForUser(userID)
		kb.ResizeKeyboard = true
		m := tgbotapi.NewMessage(msg.Chat.ID, "😔 Сейчас нет консолей для продажи.")
		m.ReplyMarkup = kb
		_, _ = botInstance.Send(m)
		return
	}

	markup := tgbotapi.InlineKeyboardMarkup{InlineKeyboard: rows}
	m := tgbotapi.NewMessage(msg.Chat.ID, "💰 Выберите консоль для покупки:")
	m.ReplyMarkup = markup
	_, _ = botInstance.Send(m)
}

// ─── Create rental ───────────────────────────────────────────────────────────

func createRentalDirect(userID, consoleID string, selectedHours int, location *models.Location) string {
	rentals := storage.LoadRentals()
	consoles := storage.LoadConsoles()
	users := storage.LoadUsers()

	rentalID := uuid.New().String()
	now := time.Now()

	var expectedEnd string
	var expectedCost float64
	console := consoles[consoleID]

	if selectedHours > 0 {
		expectedEnd = now.Add(time.Duration(selectedHours) * time.Hour).Format(time.RFC3339)
		expectedCost = float64(selectedHours) * console.RentalPrice
	}

	rental := models.Rental{
		ID:              rentalID,
		UserID:          userID,
		ConsoleID:       consoleID,
		StartTime:       now.Format(time.RFC3339),
		ExpectedEndTime: expectedEnd,
		SelectedHours:   selectedHours,
		ExpectedCost:    expectedCost,
		Status:          "active",
		TotalCost:       0,
		Location:        location,
	}

	rentals[rentalID] = rental
	console.Status = "rented"
	consoles[consoleID] = console
	_ = storage.SaveRentals(rentals)
	_ = storage.SaveConsoles(consoles)

	user := users[userID]
	name := user.FullName
	if name == "" {
		name = user.FirstName
	}

	adminMsg := fmt.Sprintf("✅ *Аренда началась*\n\n👤 %s\n📱 %s\n🎮 %s\n💰 %.0f лей/час\n🕐 Время начала: %s\n🆔 ID аренды: `%s`",
		name, user.PhoneNumber, console.Name, console.RentalPrice,
		now.Format("2006-01-02 15:04"), rentalID)
	if selectedHours > 0 {
		days := selectedHours / 24
		adminMsg += fmt.Sprintf("\n⏰ Выбранное время: %d %s\n💵 Ожидаемая стоимость: %.0f лей",
			days, daysLabel(days), expectedCost)
	}
	notifyAdmin(adminMsg)

	return rentalID
}

func endRentalByID(userID, rentalID string) map[string]interface{} {
	rentals := storage.LoadRentals()
	consoles := storage.LoadConsoles()
	users := storage.LoadUsers()

	rental, ok := rentals[rentalID]
	if !ok {
		return map[string]interface{}{"success": false, "error": "Аренда не найдена"}
	}
	if rental.UserID != userID {
		return map[string]interface{}{"success": false, "error": "Это не ваша аренда"}
	}
	if rental.Status != "active" {
		return map[string]interface{}{"success": false, "error": "Аренда уже завершена"}
	}

	startTime, _ := time.Parse(time.RFC3339, rental.StartTime)
	if startTime.IsZero() {
		startTime, _ = time.Parse("2006-01-02T15:04:05", rental.StartTime)
	}
	dur := time.Since(startTime)
	hours := int(dur.Hours())
	if hours < 1 {
		hours = 1
	}

	console := consoles[rental.ConsoleID]
	totalCost := float64(hours) * console.RentalPrice

	rental.EndTime = time.Now().Format(time.RFC3339)
	rental.Status = "completed"
	rental.TotalCost = totalCost

	console.Status = "available"

	user := users[userID]
	user.TotalSpent += totalCost
	users[userID] = user

	rentals[rentalID] = rental
	consoles[rental.ConsoleID] = console

	_ = storage.SaveRentals(rentals)
	_ = storage.SaveConsoles(consoles)
	_ = storage.SaveUsers(users)

	updateRatingOnRentalCompletion(userID, true, "perfect")

	name := user.FullName
	if name == "" {
		name = user.FirstName
	}
	if name == "" {
		name = "Неизвестный"
	}

	return map[string]interface{}{
		"success":      true,
		"console_name": console.Name,
		"hours":        hours,
		"total_cost":   totalCost,
		"user_name":    name,
		"user_phone":   user.PhoneNumber,
	}
}
