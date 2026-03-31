package config

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	TelegramBotToken string
	AdminTelegramID  string
	SecretKey        string
	WebPort          string
	DataDir          string
	PassportDir      string
	DatabaseURL      string
}

var App *Config

func Load() *Config {
	_ = godotenv.Load()

	secretKey := getEnv("SECRET_KEY", "")
	if secretKey == "" {
		b := make([]byte, 32)
		_, _ = rand.Read(b)
		secretKey = hex.EncodeToString(b)
		fmt.Println("⚠️ Используется автоматически сгенерированный SECRET_KEY. Установите свой в переменных окружения.")
	}

	App = &Config{
		TelegramBotToken: getEnv("TELEGRAM_BOT_TOKEN", ""),
		AdminTelegramID:  getEnv("ADMIN_TELEGRAM_ID", ""),
		SecretKey:        secretKey,
		WebPort:          getEnv("WEB_PORT", "5000"),
		DataDir:          getEnv("DATA_DIR", "data"),
		PassportDir:      getEnv("PASSPORT_DIR", "passport"),
		DatabaseURL:      getEnv("DATABASE_URL", ""),
	}

	return App
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
