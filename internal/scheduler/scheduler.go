package scheduler

import (
	"fmt"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"ps4-rental/internal/config"
	"ps4-rental/internal/models"
	"ps4-rental/internal/storage"
)

var botInstance *tgbotapi.BotAPI

// Start initializes the rental scheduler goroutine
func Start(bot *tgbotapi.BotAPI) {
	botInstance = bot
	go run()
	fmt.Println("🕒 Планировщик аренды запущен")
}

func run() {
	for {
		s := storage.LoadSettings()
		interval := time.Duration(s.NotificationFrequency) * time.Minute
		if interval < time.Minute {
			interval = 5 * time.Minute
		}

		checkExpiredRentals()
		sendRentalReminders()

		time.Sleep(interval)
	}
}

// checkExpiredRentals auto-completes rentals whose ExpectedEndTime has passed
func checkExpiredRentals() {
	s := storage.LoadSettings()
	maxHours := s.MaxRentalHours
	if maxHours == 0 {
		maxHours = 24
	}

	rentals := storage.LoadRentals()
	consoles := storage.LoadConsoles()
	users := storage.LoadUsers()
	now := time.Now()

	modified := false

	for rentalID, rental := range rentals {
		if rental.Status != "active" {
			continue // skip booked, completed, etc.
		}

		startTime, _ := time.Parse(time.RFC3339, rental.StartTime)
		if startTime.IsZero() {
			startTime, _ = time.Parse("2006-01-02T15:04:05", rental.StartTime)
		}
		if startTime.IsZero() {
			continue
		}

		// Determine deadline: use ExpectedEndTime if set, else maxHours from start
		var deadline time.Time
		if rental.ExpectedEndTime != "" {
			deadline, _ = time.Parse(time.RFC3339, rental.ExpectedEndTime)
		}
		if deadline.IsZero() {
			deadline = startTime.Add(time.Duration(maxHours) * time.Hour)
		}

		if now.Before(deadline) {
			continue
		}

		// Auto-complete
		c := consoles[rental.ConsoleID]
		u := users[rental.UserID]

		totalHours := int(now.Sub(startTime).Hours())
		if totalHours < 1 {
			totalHours = 1
		}
		totalCost := float64(totalHours) * c.RentalPrice

		rental.EndTime = now.Format(time.RFC3339)
		rental.Status = "completed"
		rental.TotalCost = totalCost
		rental.EndedBy = "system_timeout"
		rentals[rentalID] = rental

		// Free console
		if con, ok := consoles[rental.ConsoleID]; ok {
			con.Status = "available"
			consoles[rental.ConsoleID] = con
		}

		// Update user spending
		if usr, ok := users[rental.UserID]; ok {
			usr.TotalSpent += totalCost
			users[rental.UserID] = usr
		}

		modified = true
		fmt.Printf("⏰ Автоматически завершена аренда %s\n", rentalID)

		notifyAutoEnd(rental.UserID, c.Name, u, totalCost, totalHours)

		// Notify waitlist users for this console
		notifyWaitlist(rental.ConsoleID, c.Name)
	}

	if modified {
		_ = storage.SaveRentals(rentals)
		_ = storage.SaveConsoles(consoles)
		_ = storage.SaveUsers(users)
	}
}

func notifyWaitlist(consoleID, consoleName string) {
	entries := storage.GetWaitlistForConsole(consoleID)
	if len(entries) == 0 {
		return
	}
	for _, e := range entries {
		var chatID int64
		fmt.Sscanf(e.UserID, "%d", &chatID)
		msg := tgbotapi.NewMessage(chatID, fmt.Sprintf(
			"🟢 *Консоль освободилась!*\n\n🎮 %s теперь доступна для аренды.\n\nНажмите 📝 Арендовать чтобы забронировать!",
			consoleName,
		))
		msg.ParseMode = "Markdown"
		_, _ = botInstance.Send(msg)
	}
	storage.ClearWaitlistForConsole(consoleID)
	fmt.Printf("📢 Уведомлено %d человек из очереди для консоли %s\n", len(entries), consoleName)
}

// sendRentalReminders sends push notifications before rental expires
func sendRentalReminders() {
	s := storage.LoadSettings()
	if !s.PushNotificationsEnabled {
		return
	}

	maxHours := s.MaxRentalHours
	if maxHours == 0 {
		maxHours = 24
	}

	rentals := storage.LoadRentals()
	consoles := storage.LoadConsoles()
	now := time.Now()
	modified := false

	for rentalID, rental := range rentals {
		if rental.Status != "active" {
			continue
		}

		startTime, _ := time.Parse(time.RFC3339, rental.StartTime)
		if startTime.IsZero() {
			startTime, _ = time.Parse("2006-01-02T15:04:05", rental.StartTime)
		}
		if startTime.IsZero() {
			continue
		}

		// Use ExpectedEndTime if set
		var deadline time.Time
		if rental.ExpectedEndTime != "" {
			deadline, _ = time.Parse(time.RFC3339, rental.ExpectedEndTime)
		}
		if deadline.IsZero() {
			deadline = startTime.Add(time.Duration(maxHours) * time.Hour)
		}

		remaining := deadline.Sub(now).Hours()
		c := consoles[rental.ConsoleID]

		if remaining <= 2 && remaining > 1.5 && !rental.Reminder2hSent {
			sendPushNotification(rental.UserID, rentalID, c.Name, c.RentalPrice, startTime, "2_hours")
			rental.Reminder2hSent = true
			rentals[rentalID] = rental
			modified = true
		} else if remaining <= 1 && remaining > 0.5 && !rental.Reminder1hSent {
			sendPushNotification(rental.UserID, rentalID, c.Name, c.RentalPrice, startTime, "1_hour")
			rental.Reminder1hSent = true
			rentals[rentalID] = rental
			modified = true
		} else if remaining <= 0.5 && remaining > 0.25 && !rental.Reminder30mSent {
			sendPushNotification(rental.UserID, rentalID, c.Name, c.RentalPrice, startTime, "30_minutes")
			rental.Reminder30mSent = true
			rentals[rentalID] = rental
			modified = true
		} else if remaining <= 0.17 && remaining > 0.08 && !rental.Reminder10mSent && s.CriticalNotificationsEnabled {
			sendPushNotification(rental.UserID, rentalID, c.Name, c.RentalPrice, startTime, "10_minutes")
			rental.Reminder10mSent = true
			rentals[rentalID] = rental
			modified = true
		}
	}

	if modified {
		_ = storage.SaveRentals(rentals)
	}
}

type notifConfig struct {
	emoji    string
	title    string
	urgency  string
	timeText string
}

var notifConfigs = map[string]notifConfig{
	"2_hours":    {"⏰", "Напоминание об аренде", "info", "2 часа"},
	"1_hour":     {"🕐", "Скоро завершение аренды", "warning", "1 час"},
	"30_minutes": {"⚠️", "ВНИМАНИЕ! Скоро завершение", "high", "30 минут"},
	"10_minutes": {"🚨", "КРИТИЧЕСКОЕ! Последние минуты", "critical", "10 минут"},
}

func sendPushNotification(userID, rentalID, consoleName string, pricePerHour float64, startTime time.Time, notifType string) {
	cfg, ok := notifConfigs[notifType]
	if !ok {
		cfg = notifConfigs["1_hour"]
	}

	// Calculate current cost
	hours := int(time.Since(startTime).Hours())
	if hours < 1 {
		hours = 1
	}
	currentCost := float64(hours) * pricePerHour

	var msg string
	switch cfg.urgency {
	case "critical":
		msg = fmt.Sprintf("%s *%s* %s\n\n🎮 Консоль: *%s*\n⏳ До завершения: *%s*\n\n🚨 *СРОЧНО ЗАВЕРШИТЕ АРЕНДУ!*\n💰 Текущая стоимость: %.0f лей\n\n",
			cfg.emoji, cfg.title, cfg.emoji, consoleName, cfg.timeText, currentCost)
	case "high":
		msg = fmt.Sprintf("%s *%s*\n\n🎮 Консоль: %s\n⏳ До завершения: *%s*\n\n⚡ Рекомендуем завершить аренду заранее\n💰 Текущая стоимость: %.0f лей\n\n",
			cfg.emoji, cfg.title, consoleName, cfg.timeText, currentCost)
	default:
		msg = fmt.Sprintf("%s *%s*\n\n🎮 Консоль: %s\n⏳ До завершения: %s\n\n",
			cfg.emoji, cfg.title, consoleName, cfg.timeText)
	}

	msg += fmt.Sprintf("💡 *Способы завершения:*\n• Команда: `/end %s`\n• Кнопка в \"📊 Мой кабинет\"\n• Веб-панель администратора\n\n📞 Помощь: напишите администратору", rentalID)

	var chatID int64
	fmt.Sscanf(userID, "%d", &chatID)

	if cfg.urgency == "critical" {
		critMsg := tgbotapi.NewMessage(chatID, "🚨🚨🚨 КРИТИЧЕСКОЕ УВЕДОМЛЕНИЕ 🚨🚨🚨")
		_, _ = botInstance.Send(critMsg)
	}

	m := tgbotapi.NewMessage(chatID, msg)
	m.ParseMode = "Markdown"
	_, _ = botInstance.Send(m)

	fmt.Printf("🔔 Push-уведомление (%s) отправлено для аренды %s\n", notifType, rentalID)
}

func notifyAutoEnd(userID, consoleName string, user models.User, totalCost float64, hours int) {
	var chatID int64
	fmt.Sscanf(userID, "%d", &chatID)

	userMsg := fmt.Sprintf("⏰ *Аренда автоматически завершена*\n\n🎮 Консоль: %s\n⏰ Длительность: %d ч.\n💰 К оплате: %.0f лей\n\n🕒 Аренда была завершена автоматически по истечении максимального времени.\nСпасибо за использование нашего сервиса!",
		consoleName, hours, totalCost)

	m := tgbotapi.NewMessage(chatID, userMsg)
	m.ParseMode = "Markdown"
	_, _ = botInstance.Send(m)

	// Notify all Telegram admins
	admins := storage.LoadTelegramAdmins()
	adminMsg := fmt.Sprintf("⏰ *Аренда автоматически завершена*\n\n🎮 %s\n⏰ %d ч.\n💰 %.0f лей\n🤖 Завершено системой (превышен лимит времени)",
		consoleName, hours, totalCost)

	for _, admin := range admins {
		var adminChatID int64
		fmt.Sscanf(admin.TelegramID, "%d", &adminChatID)
		am := tgbotapi.NewMessage(adminChatID, adminMsg)
		am.ParseMode = "Markdown"
		_, _ = botInstance.Send(am)
	}
}

// GetBotAPI exposes the bot for scheduler use
func GetBotAPI() *tgbotapi.BotAPI {
	return botInstance
}

// StartWithToken creates a bot instance specifically for the scheduler
func StartWithToken(token string) {
	bot, err := tgbotapi.NewBotAPI(token)
	if err != nil {
		fmt.Printf("❌ Планировщик: ошибка инициализации бота: %v\n", err)
		return
	}
	_ = config.App // ensure config is loaded
	botInstance = bot
	go run()
	fmt.Println("🕒 Планировщик аренды запущен")
}
