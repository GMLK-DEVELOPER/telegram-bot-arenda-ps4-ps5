package api

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"ps4-rental/internal/db"
)

// GetChartData returns data for dashboard charts
func GetChartData(c *gin.Context) {
	// Rentals by day (last 7 days)
	now := time.Now()
	sevenDaysAgo := now.AddDate(0, 0, -7)

	var rentals []db.Rental
	db.DB.Where("created_at >= ?", sevenDaysAgo).Find(&rentals)

	// Group by date
	rentalsByDay := make(map[string]int)
	for i := 0; i < 7; i++ {
		date := now.AddDate(0, 0, -i).Format("2006-01-02")
		rentalsByDay[date] = 0
	}

	for _, r := range rentals {
		date := r.CreatedAt.Format("2006-01-02")
		rentalsByDay[date]++
	}

	// Popular consoles (by rental count)
	type ConsoleStats struct {
		ConsoleID string
		Name      string
		Count     int64
	}

	var consoleStats []ConsoleStats
	db.DB.Raw(`
		SELECT r.console_id, c.name, COUNT(*) as count
		FROM rentals r
		JOIN consoles c ON r.console_id = c.id
		GROUP BY r.console_id, c.name
		ORDER BY count DESC
		LIMIT 5
	`).Scan(&consoleStats)

	c.JSON(http.StatusOK, gin.H{
		"rentals_by_day":    rentalsByDay,
		"popular_consoles":  consoleStats,
	})
}
