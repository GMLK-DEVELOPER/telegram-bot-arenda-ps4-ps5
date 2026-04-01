package bot

import (
	"fmt"
	"log"
	"strings"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"ps4-rental/internal/config"
	"ps4-rental/internal/storage"
)

var botInstance *tgbotapi.BotAPI

// Start initializes and starts the Telegram bot polling loop
func Start() {
	var err error
	botInstance, err = tgbotapi.NewBotAPI(config.App.TelegramBotToken)
	if err != nil {
		log.Fatalf("❌ Ошибка инициализации бота: %v", err)
	}

	fmt.Printf("🤖 Бот запущен как @%s\n", botInstance.Self.UserName)

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60

	updates := botInstance.GetUpdatesChan(u)

	for update := range updates {
		if update.CallbackQuery != nil {
			go processCallback(update.CallbackQuery)
			continue
		}

		if update.Message == nil {
			continue
		}

		go processMessage(update.Message)
	}
}

func processMessage(msg *tgbotapi.Message) {
	userID := fmt.Sprintf("%d", msg.From.ID)

	// Handle commands
	if msg.IsCommand() {
		switch msg.Command() {
		case "start":
			handleStart(msg)
		case "end":
			handleEnd(msg)
		case "help":
			handleHelp(msg)
		default:
			handleHelp(msg)
		}
		return
	}

	// Handle special content types
	if msg.Contact != nil {
		handleContact(msg)
		return
	}
	if msg.Location != nil {
		handleLocation(msg)
		return
	}
	if msg.Photo != nil {
		handlePhoto(msg)
		return
	}

	// Text messages
	text := msg.Text

	// Check if user is in registration flow (entering full name)
	users := storage.LoadUsers()
	if u, ok := users[userID]; ok && u.RegistrationStep == "full_name" {
		handleFullName(msg)
		return
	}

	switch text {
	case "📊 Мой кабинет":
		handleMyProfile(msg)
	case "📝 Арендовать":
		handleRentalMenu(msg)
	case "💰 Купить":
		handleBuyMenu(msg)
	case "ℹ️ Помощь":
		handleHelp(msg)
	case "⚙️ Админ панель":
		handleAdminPanel(msg)
	case "📈 Статистика":
		handleStatistics(msg)
	case "👥 Пользователи":
		handleUsersAdmin(msg)
	case "🔔 Уведомления":
		handleNotifications(msg)
	default:
		if strings.HasPrefix(text, "/") {
			handleHelp(msg)
		} else {
			kb := getKeyboardForUser(userID)
			kb.ResizeKeyboard = true
			m := tgbotapi.NewMessage(msg.Chat.ID, "🤔 Не понимаю эту команду. Используйте меню или /help")
			m.ReplyMarkup = kb
			_, _ = botInstance.Send(m)
		}
	}
}

func processCallback(call *tgbotapi.CallbackQuery) {
	handleCallback(call)
}

// GetBotAPI returns the bot instance (may be nil if not yet initialized).
func GetBotAPI() *tgbotapi.BotAPI {
	return botInstance
}

// SendToUser sends a Telegram message to a user by their string user ID.
func SendToUser(userID string, text string) {
	if botInstance == nil {
		return
	}
	var chatID int64
	fmt.Sscanf(userID, "%d", &chatID)
	if chatID == 0 {
		return
	}
	msg := tgbotapi.NewMessage(chatID, text)
	msg.ParseMode = "Markdown"
	_, _ = botInstance.Send(msg)
}

// SendLocationRequest sends a message asking the user to share their location.
// All regular keyboard buttons are replaced with a single "Share location" button.
func SendLocationRequest(userID string) {
	if botInstance == nil {
		return
	}
	var chatID int64
	fmt.Sscanf(userID, "%d", &chatID)
	if chatID == 0 {
		return
	}

	locationBtn := tgbotapi.KeyboardButton{
		Text:            "📍 Поделиться геолокацией",
		RequestLocation: true,
	}
	kb := tgbotapi.NewReplyKeyboard(tgbotapi.NewKeyboardButtonRow(locationBtn))
	kb.ResizeKeyboard = true
	kb.OneTimeKeyboard = true

	msg := tgbotapi.NewMessage(chatID,
		"🏠 *Когда будете дома — нажмите кнопку ниже чтобы поделиться геолокацией.*\n\nПосле этого аренда начнётся и таймер запустится. 🎮")
	msg.ParseMode = "Markdown"
	msg.ReplyMarkup = kb
	_, _ = botInstance.Send(msg)
}

// SendLocationToUser sends a venue (location + title + address) to a user.
func SendLocationToUser(userID string, lat, lng float64, title, address string) {
	if botInstance == nil {
		return
	}
	var chatID int64
	fmt.Sscanf(userID, "%d", &chatID)
	if chatID == 0 {
		return
	}
	venue := tgbotapi.NewVenue(chatID, title, address, lat, lng)
	_, _ = botInstance.Send(venue)
}
