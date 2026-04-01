package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"ps4-rental/internal/db"
)

// GetTopClients returns top clients by total spent
func GetTopClients(c *gin.Context) {
	var users []db.User
	db.DB.Order("total_spent DESC").Limit(10).Find(&users)

	type TopClient struct {
		UserID     string  `json:"user_id"`
		FullName   string  `json:"full_name"`
		Username   string  `json:"username"`
		TotalSpent float64 `json:"total_spent"`
	}

	clients := make([]TopClient, 0, len(users))
	for _, u := range users {
		clients = append(clients, TopClient{
			UserID:     u.ID,
			FullName:   u.FullName,
			Username:   u.Username,
			TotalSpent: u.TotalSpent,
		})
	}

	c.JSON(http.StatusOK, gin.H{"clients": clients})
}
