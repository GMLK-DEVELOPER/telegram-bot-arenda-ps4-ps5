package api

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"ps4-rental/internal/db"
)

// GetReport generates a report for a date range
func GetReport(c *gin.Context) {
	startDate := c.Query("start_date") // format: 2006-01-02
	endDate := c.Query("end_date")

	if startDate == "" || endDate == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "start_date и end_date обязательны"})
		return
	}

	start, err1 := time.Parse("2006-01-02", startDate)
	end, err2 := time.Parse("2006-01-02", endDate)
	if err1 != nil || err2 != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Неверный формат даты (используйте YYYY-MM-DD)"})
		return
	}

	// Set end to end of day
	end = end.Add(23*time.Hour + 59*time.Minute + 59*time.Second)

	// Total rentals
	var totalRentals int64
	db.DB.Model(&db.Rental{}).Where("created_at BETWEEN ? AND ?", start, end).Count(&totalRentals)

	// Completed rentals
	var completedRentals int64
	db.DB.Model(&db.Rental{}).Where("created_at BETWEEN ? AND ? AND status = ?", start, end, "completed").Count(&completedRentals)

	// Total revenue
	var totalRevenue float64
	db.DB.Model(&db.Rental{}).
		Where("created_at BETWEEN ? AND ? AND status = ?", start, end, "completed").
		Select("COALESCE(SUM(total_cost), 0)").
		Scan(&totalRevenue)

	// New users
	var newUsers int64
	db.DB.Model(&db.User{}).Where("joined_at BETWEEN ? AND ?", start, end).Count(&newUsers)

	// Top console
	type ConsoleRevenue struct {
		ConsoleID string
		Name      string
		Revenue   float64
		Count     int64
	}
	var topConsole ConsoleRevenue
	db.DB.Raw(`
		SELECT r.console_id, c.name, COALESCE(SUM(r.total_cost), 0) as revenue, COUNT(*) as count
		FROM rentals r
		JOIN consoles c ON r.console_id = c.id
		WHERE r.created_at BETWEEN ? AND ? AND r.status = 'completed'
		GROUP BY r.console_id, c.name
		ORDER BY revenue DESC
		LIMIT 1
	`, start, end).Scan(&topConsole)

	// Average rental duration
	var avgDuration float64
	db.DB.Model(&db.Rental{}).
		Where("created_at BETWEEN ? AND ? AND status = ?", start, end, "completed").
		Select("COALESCE(AVG(duration_hours), 0)").
		Scan(&avgDuration)

	c.JSON(http.StatusOK, gin.H{
		"period": gin.H{
			"start": startDate,
			"end":   endDate,
		},
		"total_rentals":     totalRentals,
		"completed_rentals": completedRentals,
		"total_revenue":     totalRevenue,
		"new_users":         newUsers,
		"top_console":       topConsole,
		"avg_duration":      avgDuration,
	})
}
