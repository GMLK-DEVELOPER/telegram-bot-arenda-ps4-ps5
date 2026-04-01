package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"ps4-rental/internal/config"
	"ps4-rental/internal/storage"
)

// CompleteSetup handles POST /api/setup — saves initial configuration
func CompleteSetup(c *gin.Context) {
	var data struct {
		ShopTitle   string `json:"shop_title"`
		ShopAddress string `json:"shop_address"`
		BotToken    string `json:"bot_token"`
		AdminID     string `json:"admin_telegram_id"`
		Username    string `json:"username"`
		Password    string `json:"password"`
	}
	if err := c.BindJSON(&data); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "Неверный запрос"})
		return
	}

	if data.ShopTitle == "" {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "Введите название компании"})
		return
	}
	if data.BotToken == "" {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "Введите токен Telegram бота"})
		return
	}
	if data.Username == "" || data.Password == "" {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "Введите логин и пароль администратора"})
		return
	}

	// Save settings
	s := storage.LoadSettings()
	s.ShopTitle      = data.ShopTitle
	s.ShopAddress    = data.ShopAddress
	s.BotToken       = data.BotToken
	s.AdminChatID    = data.AdminID
	s.SetupCompleted = true
	if err := storage.SaveSettings(s); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": "Ошибка сохранения настроек"})
		return
	}

	// Update config in memory so bot can start without restart
	config.App.TelegramBotToken = data.BotToken
	if data.AdminID != "" {
		config.App.AdminTelegramID = data.AdminID
	}

	// Create admin account
	if err := storage.CreateAdmin(data.Username, data.Password); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": "Ошибка создания аккаунта: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "success"})
}
