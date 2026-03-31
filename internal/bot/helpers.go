package bot

import (
	"fmt"
	"math"
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

// ─── Messaging helpers ───────────────────────────────────────────────────────

func safeSendMessage(chatID int64, text string) bool {
	msg := tgbotapi.NewMessage(chatID, text)
	msg.ParseMode = "Markdown"
	_, err := botInstance.Send(msg)
	if err != nil {
		errStr := err.Error()
		if strings.Contains(errStr, "chat not found") || strings.Contains(errStr, "bot was blocked") {
			markUserUnavailable(fmt.Sprintf("%d", chatID))
		}
		return false
	}
	return true
}

func sendMessage(chatID int64, text string, markup interface{}) {
	msg := tgbotapi.NewMessage(chatID, text)
	msg.ParseMode = "Markdown"
	if markup != nil {
		msg.ReplyMarkup = markup
	}
	_, _ = botInstance.Send(msg)
}

func editMessageText(chatID int64, messageID int, text string, markup interface{}) {
	edit := tgbotapi.NewEditMessageText(chatID, messageID, text)
	edit.ParseMode = "Markdown"
	if markup != nil {
		switch m := markup.(type) {
		case tgbotapi.InlineKeyboardMarkup:
			edit.ReplyMarkup = &m
		}
	}
	_, _ = botInstance.Send(edit)
}

func answerCallback(callID, text string, alert bool) {
	cb := tgbotapi.NewCallback(callID, text)
	cb.ShowAlert = alert
	_, _ = botInstance.Request(cb)
}

func notifyAdmin(text string) {
	// Send to all Telegram admins
	admins := storage.LoadTelegramAdmins()
	if len(admins) == 0 {
		fmt.Println("⚠️  Нет Telegram админов для отправки уведомления")
		return
	}

	for _, admin := range admins {
		var chatID int64
		fmt.Sscanf(admin.TelegramID, "%d", &chatID)
		safeSendMessage(chatID, text)
	}
}

func markUserUnavailable(userID string) {
	users := storage.LoadUsers()
	if u, ok := users[userID]; ok {
		u.BotBlocked = true
		u.BotBlockedAt = time.Now().Format(time.RFC3339)
		users[userID] = u
		_ = storage.SaveUsers(users)
	}
}

// ─── User helpers ────────────────────────────────────────────────────────────

func isUserBanned(userID string) bool {
	users := storage.LoadUsers()
	return users[userID].IsBanned
}

func isUserAdmin(userID string) bool {
	// Check if user is in Telegram admins list
	isAdmin := storage.IsTelegramAdmin(userID)
	fmt.Printf("🔍 Admin check: userID=%s, isAdmin=%v\n", userID, isAdmin)
	return isAdmin
}

func isUserRegistered(userID string) bool {
	users := storage.LoadUsers()
	u := users[userID]
	return u.PhoneNumber != "" && u.FullName != ""
}

// ─── Keyboard builders ───────────────────────────────────────────────────────

func userKeyboard() tgbotapi.ReplyKeyboardMarkup {
	return tgbotapi.NewReplyKeyboard(
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton("📊 Мой кабинет"),
			tgbotapi.NewKeyboardButton("📝 Арендовать"),
		),
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton("ℹ️ Помощь"),
		),
	)
}

func adminKeyboard() tgbotapi.ReplyKeyboardMarkup {
	return tgbotapi.NewReplyKeyboard(
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton("⚙️ Админ панель"),
			tgbotapi.NewKeyboardButton("📈 Статистика"),
		),
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton("👥 Пользователи"),
			tgbotapi.NewKeyboardButton("🔔 Уведомления"),
		),
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton("📝 Арендовать"),
			tgbotapi.NewKeyboardButton("ℹ️ Помощь"),
		),
	)
}

func getKeyboardForUser(userID string) tgbotapi.ReplyKeyboardMarkup {
	if isUserAdmin(userID) {
		return adminKeyboard()
	}
	return userKeyboard()
}

// ─── Discount helpers ────────────────────────────────────────────────────────

func getDiscountForConsole(consoleID string) *models.Discount {
	discounts := storage.LoadDiscounts()
	now := time.Now()
	for _, d := range discounts {
		if d.ConsoleID == consoleID && d.Active {
			start, _ := time.Parse(time.RFC3339, d.StartDate)
			end, _ := time.Parse(time.RFC3339, d.EndDate)
			if start.IsZero() {
				start, _ = time.Parse("2006-01-02T15:04:05", d.StartDate)
			}
			if end.IsZero() {
				end, _ = time.Parse("2006-01-02T15:04:05", d.EndDate)
			}
			if now.After(start) && now.Before(end) {
				dCopy := d
				return &dCopy
			}
		}
	}
	return nil
}

func calculateDiscountedPrice(consoleID string, originalPrice float64, durationHours int) (float64, float64, *models.Discount) {
	d := getDiscountForConsole(consoleID)
	if d == nil {
		return originalPrice, 0, nil
	}
	if durationHours < d.MinHours {
		return originalPrice, 0, nil
	}

	var discountAmount float64
	switch d.Type {
	case "percentage":
		discountAmount = originalPrice * (d.Value / 100)
	case "fixed":
		discountAmount = d.Value
		if discountAmount > originalPrice {
			discountAmount = originalPrice
		}
	default:
		return originalPrice, 0, nil
	}

	finalPrice := math.Round(originalPrice - discountAmount)
	if finalPrice < 0 {
		finalPrice = 0
	}
	return finalPrice, math.Round(discountAmount), d
}

func checkDateHasDiscount(consoleID string, date time.Time) bool {
	discounts := storage.LoadDiscounts()
	for _, d := range discounts {
		if d.ConsoleID == consoleID && d.Active {
			start, _ := time.Parse(time.RFC3339, d.StartDate)
			end, _ := time.Parse(time.RFC3339, d.EndDate)
			if start.IsZero() {
				start, _ = time.Parse("2006-01-02T15:04:05", d.StartDate)
			}
			if end.IsZero() {
				end, _ = time.Parse("2006-01-02T15:04:05", d.EndDate)
			}
			if !date.Before(start) && !date.After(end) {
				return true
			}
		}
	}
	return false
}

// ─── Reservation helpers ─────────────────────────────────────────────────────

func createTempReservation(userID, consoleID string, timeoutMinutes int) string {
	reservations := storage.LoadReservations()

	// Remove old reservations for this user
	for id, r := range reservations {
		if r.UserID == userID {
			delete(reservations, id)
		}
	}

	id := uuid.New().String()
	now := time.Now()
	reservations[id] = models.TempReservation{
		ID:        id,
		UserID:    userID,
		ConsoleID: consoleID,
		CreatedAt: now.Format(time.RFC3339),
		ExpiresAt: now.Add(time.Duration(timeoutMinutes) * time.Minute).Format(time.RFC3339),
		Status:    "active",
	}
	_ = storage.SaveReservations(reservations)
	return id
}

func removeTempReservation(userID string) {
	reservations := storage.LoadReservations()
	for id, r := range reservations {
		if r.UserID == userID {
			delete(reservations, id)
		}
	}
	_ = storage.SaveReservations(reservations)
}

func cleanupExpiredReservations() {
	reservations := storage.LoadReservations()
	now := time.Now()
	for id, r := range reservations {
		exp, _ := time.Parse(time.RFC3339, r.ExpiresAt)
		if now.After(exp) {
			delete(reservations, id)
		}
	}
	_ = storage.SaveReservations(reservations)
}

func isConsoleTempReserved(consoleID, excludeUserID string) (bool, string) {
	cleanupExpiredReservations()
	reservations := storage.LoadReservations()
	for _, r := range reservations {
		if r.ConsoleID == consoleID && r.Status == "active" && r.UserID != excludeUserID {
			return true, r.UserID
		}
	}
	return false, ""
}

// ─── Console photo helpers ───────────────────────────────────────────────────

func getConsolePhotoPath(consoleID string, console *models.Console) string {
	if console != nil && console.PhotoPath != "" {
		localPath := strings.Replace(console.PhotoPath, "/static/", "static/", 1)
		if _, err := os.Stat(localPath); err == nil {
			return localPath
		}
	}

	dir := filepath.Join("static", "img", "console")
	for _, ext := range []string{"png", "jpg", "jpeg", "gif", "webp"} {
		p := filepath.Join(dir, consoleID+"."+ext)
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return ""
}

// ─── Rental helpers ──────────────────────────────────────────────────────────

func getConsoleRentalInfo(consoleID string) map[string]interface{} {
	rentals := storage.LoadRentals()
	users := storage.LoadUsers()

	for _, r := range rentals {
		if r.ConsoleID == consoleID && r.Status == "active" {
			startTime, _ := time.Parse(time.RFC3339, r.StartTime)
			if startTime.IsZero() {
				startTime, _ = time.Parse("2006-01-02T15:04:05", r.StartTime)
			}
			estimatedEnd, _ := time.Parse(time.RFC3339, r.ExpectedEndTime)
			if estimatedEnd.IsZero() {
				hours := r.SelectedHours
				if hours == 0 {
					hours = r.DurationHours
				}
				if hours == 0 {
					hours = 24
				}
				estimatedEnd = startTime.Add(time.Duration(hours) * time.Hour)
			}
			user := users[r.UserID]
			name := user.FullName
			if name == "" {
				name = user.FirstName
			}
			if name == "" {
				name = "Неизвестный"
			}
			return map[string]interface{}{
				"start_time":          startTime,
				"estimated_end_time":  estimatedEnd,
				"user_name":           name,
				"rental_id":           r.ID,
			}
		}
	}
	return nil
}

func isApprovalRequired() bool {
	return storage.LoadSettings().RequireApproval
}

// ─── Document helpers ────────────────────────────────────────────────────────

func checkUserDocuments(fullName string) map[string]interface{} {
	safeName := sanitizeName(fullName)
	userFolder := filepath.Join(config.App.PassportDir, safeName)

	docs := map[string]interface{}{
		"passport_front":      false,
		"passport_back":       false,
		"selfie_with_passport": false,
	}

	if _, err := os.Stat(userFolder); os.IsNotExist(err) {
		return docs
	}

	for docType := range docs {
		for _, ext := range []string{"jpg", "jpeg", "png", "webp"} {
			fp := filepath.Join(userFolder, docType+"."+ext)
			if _, err := os.Stat(fp); err == nil {
				docs[docType] = filepath.Join(safeName, docType+"."+ext)
				break
			}
		}
	}
	return docs
}

func sanitizeName(name string) string {
	var b strings.Builder
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == ' ' || r == '-' || r == '_' ||
			r > 127 { // allow unicode (Cyrillic etc.)
			b.WriteRune(r)
		}
	}
	return strings.TrimSpace(b.String())
}

// ─── Days label helpers ──────────────────────────────────────────────────────

func daysLabel(days int) string {
	switch {
	case days == 1:
		return "день"
	case days >= 2 && days <= 4:
		return "дня"
	default:
		return "дней"
	}
}
