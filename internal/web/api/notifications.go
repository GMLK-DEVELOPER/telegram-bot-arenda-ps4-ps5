package api

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"ps4-rental/internal/db"
)

// GetNotifications returns recent system events for the notification center
func GetNotifications(c *gin.Context) {
	limit := 20

	// Get recent audit logs as notifications
	var logs []db.AuditLog
	db.DB.Order("created_at DESC").Limit(limit).Find(&logs)

	type Notification struct {
		ID        uint      `json:"id"`
		Type      string    `json:"type"` // info, success, warning
		Title     string    `json:"title"`
		Message   string    `json:"message"`
		Timestamp time.Time `json:"timestamp"`
		Read      bool      `json:"read"`
	}

	notifications := make([]Notification, 0, len(logs))
	for _, log := range logs {
		notif := Notification{
			ID:        log.ID,
			Timestamp: log.CreatedAt,
			Read:      false,
		}

		switch log.Action {
		case "approved_rental":
			notif.Type = "success"
			notif.Title = "Заявка одобрена"
			notif.Message = "Администратор " + log.AdminUser + " одобрил заявку"
		case "rejected_rental":
			notif.Type = "warning"
			notif.Title = "Заявка отклонена"
			notif.Message = "Администратор " + log.AdminUser + " отклонил заявку"
		case "ended_rental":
			notif.Type = "info"
			notif.Title = "Аренда завершена"
			notif.Message = "Администратор " + log.AdminUser + " завершил аренду"
		case "created_console":
			notif.Type = "success"
			notif.Title = "Консоль добавлена"
			notif.Message = "Администратор " + log.AdminUser + " добавил консоль"
		case "deleted_console":
			notif.Type = "warning"
			notif.Title = "Консоль удалена"
			notif.Message = "Администратор " + log.AdminUser + " удалил консоль"
		case "broadcast_message":
			notif.Type = "info"
			notif.Title = "Рассылка отправлена"
			notif.Message = "Администратор " + log.AdminUser + " отправил рассылку"
		default:
			notif.Type = "info"
			notif.Title = log.Action
			notif.Message = "Администратор " + log.AdminUser
		}

		notifications = append(notifications, notif)
	}

	c.JSON(http.StatusOK, gin.H{
		"notifications": notifications,
		"unread_count":  len(notifications),
	})
}
