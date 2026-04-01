package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"ps4-rental/internal/bot"
	"ps4-rental/internal/db"
	"ps4-rental/internal/web/middleware"
)

// BroadcastMessage sends a message to all users or filtered group
func BroadcastMessage(c *gin.Context) {
	var data struct {
		Message string `json:"message"`
		Filter  string `json:"filter"` // all, active, banned
	}
	if err := c.BindJSON(&data); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if data.Message == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Сообщение не может быть пустым"})
		return
	}

	var users []db.User
	query := db.DB

	switch data.Filter {
	case "banned":
		query = query.Where("is_banned = ?", true)
	case "active":
		query = query.Where("is_banned = ?", false)
	default:
		// all users
	}

	query.Find(&users)

	sent := 0
	for _, u := range users {
		if u.BotBlocked {
			continue
		}
		bot.SendToUser(u.ID, data.Message)
		sent++
	}

	middleware.LogActionJSON(c, "broadcast_message", "", gin.H{
		"filter": data.Filter,
		"sent":   sent,
		"total":  len(users),
	})

	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"sent":   sent,
		"total":  len(users),
	})
}
