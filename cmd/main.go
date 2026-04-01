package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"ps4-rental/internal/bot"
	"ps4-rental/internal/config"
	"ps4-rental/internal/db"
	"ps4-rental/internal/scheduler"
	"ps4-rental/internal/storage"
	"ps4-rental/internal/web"
)

func main() {
	fmt.Println("🎮 Запуск системы аренды PlayStation консолей...")
	fmt.Println("==================================================")

	// Load config
	cfg := config.Load()

	// Connect to PostgreSQL
	if cfg.DatabaseURL == "" {
		fmt.Println("❌ DATABASE_URL не задан. Укажите его в .env или переменных окружения.")
		os.Exit(1)
	}
	if err := db.Init(cfg.DatabaseURL); err != nil {
		fmt.Printf("❌ Ошибка подключения к БД: %v\n", err)
		os.Exit(1)
	}

	// Init storage (create dirs, seed default data)
	storage.Init()

	// If bot token not set via env, try loading from DB settings
	if cfg.TelegramBotToken == "" {
		s := storage.LoadSettings()
		if s.BotToken != "" {
			cfg.TelegramBotToken = s.BotToken
		}
		if cfg.AdminTelegramID == "" && s.AdminChatID != "" {
			cfg.AdminTelegramID = s.AdminChatID
		}
	}

	// Start web server in background
	go func() {
		fmt.Printf("🌐 Запуск веб-панели на порту %s...\n", cfg.WebPort)
		web.Start()
	}()

	// Handle OS signals for graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// Try to start Telegram bot (optional — web panel works without it)
	if cfg.TelegramBotToken == "" || cfg.TelegramBotToken == "ВАШ_ТОКЕН_ЗДЕСЬ" {
		fmt.Println("⚠️  Токен Telegram не задан — бот не запущен. Только веб-панель.")
	} else {
		// bot.Start() creates the BotAPI and polls. Get instance back for scheduler.
		go func() {
			bot.Start()
		}()
		// Wait a moment for bot to initialize, then pass its instance to scheduler
		go func() {
			for i := 0; i < 50; i++ {
				if botAPI := bot.GetBotAPI(); botAPI != nil {
					scheduler.Start(botAPI)
					return
				}
				time.Sleep(100 * time.Millisecond)
			}
			fmt.Println("⚠️  Планировщик: бот не инициализирован за 5 секунд")
		}()
	}

	fmt.Println("✅ Система запущена. Нажмите Ctrl+C для остановки.")
	<-quit
	fmt.Println("\n🛑 Система остановлена")
}
