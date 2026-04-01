package api

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"ps4-rental/internal/db"
)

// GetAuditLogs returns paginated audit logs
func GetAuditLogs(c *gin.Context) {
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
	if page < 1 {
		page = 1
	}
	if limit < 1 || limit > 200 {
		limit = 50
	}
	offset := (page - 1) * limit

	var logs []db.AuditLog
	var total int64

	db.DB.Model(&db.AuditLog{}).Count(&total)
	db.DB.Order("created_at DESC").Limit(limit).Offset(offset).Find(&logs)

	c.JSON(http.StatusOK, gin.H{
		"logs":  logs,
		"total": total,
		"page":  page,
		"limit": limit,
	})
}
