package middleware

import (
	"encoding/json"

	"github.com/gin-gonic/gin"
	"ps4-rental/internal/db"
)

// LogAction logs an admin action to the audit_logs table.
// Call this after successful operations that modify data.
func LogAction(c *gin.Context, action, target, details string) {
	username, _ := c.Get("username")
	user := ""
	if username != nil {
		user = username.(string)
	}

	ip := c.ClientIP()

	log := db.AuditLog{
		AdminUser: user,
		Action:    action,
		Target:    target,
		Details:   details,
		IPAddress: ip,
	}

	db.DB.Create(&log)
}

// LogActionJSON logs with JSON-encoded details
func LogActionJSON(c *gin.Context, action, target string, detailsObj interface{}) {
	b, _ := json.Marshal(detailsObj)
	LogAction(c, action, target, string(b))
}
