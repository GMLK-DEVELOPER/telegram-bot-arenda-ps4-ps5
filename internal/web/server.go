package web

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/gin-contrib/sessions"
	"github.com/gin-contrib/sessions/cookie"
	"github.com/gin-gonic/gin"
	"ps4-rental/internal/config"
	"ps4-rental/internal/storage"
	"ps4-rental/internal/web/api"
	"ps4-rental/internal/web/middleware"
)

// Start initializes and starts the Gin web server
func Start() {
	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()

	// Session middleware
	store := cookie.NewStore([]byte(config.App.SecretKey))
	r.Use(sessions.Sessions("ps4session", store))

	// Serve static files
	r.Static("/static", "./static")

	// Load HTML templates
	r.LoadHTMLGlob("templates/*.html")

	// Wire up real-time event hook — when bot saves a new request, push SSE to admin browsers
	storage.OnNewRequest = func() {
		api.BroadcastEvent("new_request", "1")
	}

	// Setup middleware — redirect to /setup if first run
	r.Use(setupMiddleware())

	// ─── Setup routes (public) ────────────────────────────────────────────────
	r.GET("/setup", showSetup)
	r.POST("/api/setup", api.CompleteSetup)

	// ─── Public routes ────────────────────────────────────────────────────────
	r.GET("/", func(c *gin.Context) {
		c.HTML(http.StatusOK, "index.html", nil)
	})
	r.GET("/login", showLogin)
	r.POST("/login", handleLogin)
	r.GET("/logout", handleLogout)

	// ─── Admin routes (protected) ─────────────────────────────────────────────
	auth := r.Group("/")
	auth.Use(authMiddleware())
	{
		auth.GET("/admin", showAdmin)

		// Consoles API
		auth.GET("/api/consoles", api.GetConsoles)
		auth.POST("/api/consoles", middleware.RequireRole("admin", "operator"), api.CreateConsole)
		auth.PUT("/api/consoles", middleware.RequireRole("admin", "operator"), api.UpdateConsole)
		auth.DELETE("/api/consoles", middleware.RequireRole("admin"), api.DeleteConsole)
		auth.POST("/api/consoles/photo", middleware.RequireRole("admin", "operator"), api.UploadConsolePhoto)

		// Users API
		auth.GET("/api/users", api.GetUsers)
		auth.PUT("/api/users", middleware.RequireRole("admin", "operator"), api.UpdateUser)
		auth.POST("/api/users/ban", middleware.RequireRole("admin"), api.BanUser)
		auth.POST("/api/users/unban", middleware.RequireRole("admin"), api.UnbanUser)
		auth.POST("/api/users/request-location", middleware.RequireRole("admin", "operator"), api.RequestUserLocation)

		// Rentals API
		auth.GET("/api/rentals", api.GetRentals)
		auth.POST("/api/rentals/end", middleware.RequireRole("admin", "operator"), api.EndRental)
		auth.POST("/api/rentals/extend", middleware.RequireRole("admin", "operator"), api.ExtendRental)
		auth.POST("/api/rentals/start", middleware.RequireRole("admin", "operator"), api.StartRental)

		// Rental requests API
		auth.GET("/api/rental-requests", api.GetRentalRequests)
		auth.POST("/api/rental-requests/approve", middleware.RequireRole("admin", "operator"), api.ApproveRentalRequest)
		auth.POST("/api/rental-requests/reject", middleware.RequireRole("admin", "operator"), api.RejectRentalRequest)

		// Discounts API
		auth.GET("/api/discounts", api.GetDiscounts)
		auth.POST("/api/discounts", middleware.RequireRole("admin"), api.CreateDiscount)
		auth.PUT("/api/discounts", middleware.RequireRole("admin"), api.UpdateDiscount)
		auth.DELETE("/api/discounts", middleware.RequireRole("admin"), api.DeleteDiscount)

		// Settings API
		auth.GET("/api/settings", api.GetSettings)
		auth.POST("/api/settings", middleware.RequireRole("admin"), api.UpdateSettings)

		// Ratings API
		auth.GET("/api/ratings", api.GetRatings)
		auth.GET("/api/ratings/:user_id", api.GetUserRating)
		auth.POST("/api/ratings/discipline", api.AddDisciplinePoints)
		auth.POST("/api/ratings/loyalty", api.AddLoyaltyPoints)

		// Calendar API
		auth.GET("/api/calendar", api.GetCalendar)
		auth.POST("/api/calendar/blocked-dates", api.AddBlockedDate)
		auth.DELETE("/api/calendar/blocked-dates", api.RemoveBlockedDate)
		auth.POST("/api/calendar/console-blocked", api.AddConsoleBlockedDate)
		auth.DELETE("/api/calendar/console-blocked", api.RemoveConsoleBlockedDate)
		auth.POST("/api/calendar/holiday", api.AddHoliday)
		auth.PUT("/api/calendar/settings", api.UpdateCalendarSettings)

		// Documents API
		auth.GET("/api/documents/:user_name", api.GetUserDocuments)
		auth.GET("/api/documents/serve/:user_name/:doc_type", api.ServeDocument)

		// Performance/stats API
		auth.GET("/api/performance", api.GetPerformance)

		// Telegram Admins API
		auth.GET("/api/telegram-admins", api.GetTelegramAdmins)
		auth.POST("/api/telegram-admins", middleware.RequireRole("admin"), api.AddTelegramAdmin)
		auth.DELETE("/api/telegram-admins", middleware.RequireRole("admin"), api.RemoveTelegramAdmin)

		// Backup & Restore
		auth.GET("/api/backup", middleware.RequireRole("admin"), api.DownloadBackup)
		auth.POST("/api/restore", middleware.RequireRole("admin"), api.RestoreBackup)

		// SSE real-time events
		auth.GET("/api/events", api.SSEStream)

		// Audit logs
		auth.GET("/api/audit-logs", middleware.RequireRole("admin"), api.GetAuditLogs)

		// User notes
		auth.GET("/api/user-notes", api.GetUserNotes)
		auth.POST("/api/user-notes", middleware.RequireRole("admin", "operator"), api.AddUserNote)
		auth.DELETE("/api/user-notes", middleware.RequireRole("admin", "operator"), api.DeleteUserNote)

		// Top clients
		auth.GET("/api/top-clients", api.GetTopClients)

		// Chart data
		auth.GET("/api/chart-data", api.GetChartData)

		// Broadcast
		auth.POST("/api/broadcast", middleware.RequireRole("admin"), api.BroadcastMessage)

		// Reports
		auth.GET("/api/report", api.GetReport)

		// Notifications
		auth.GET("/api/notifications", api.GetNotifications)
	}

	addr := "0.0.0.0:" + config.App.WebPort
	fmt.Printf("🌐 Веб-панель запущена на http://%s\n", addr)
	if err := r.Run(addr); err != nil {
		fmt.Printf("❌ Ошибка веб-сервера: %v\n", err)
	}
}

// ─── Auth handlers ────────────────────────────────────────────────────────────

func showLogin(c *gin.Context) {
	c.HTML(http.StatusOK, "login.html", nil)
}

func handleLogin(c *gin.Context) {
	username := c.PostForm("username")
	password := c.PostForm("password")

	if username == "" || password == "" {
		c.HTML(http.StatusOK, "login.html", gin.H{"error": "Введите логин и пароль"})
		return
	}

	if !storage.AdminExists(username, password) {
		c.HTML(http.StatusOK, "login.html", gin.H{"error": "Неверный логин или пароль"})
		return
	}

	session := sessions.Default(c)
	session.Set("user", username)
	_ = session.Save()

	c.Redirect(http.StatusFound, "/admin")
}

func handleLogout(c *gin.Context) {
	session := sessions.Default(c)
	session.Clear()
	_ = session.Save()
	c.Redirect(http.StatusFound, "/")
}

func authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		session := sessions.Default(c)
		user := session.Get("user")
		if user == nil {
			if strings.HasPrefix(c.Request.URL.Path, "/api/") {
				c.JSON(http.StatusUnauthorized, gin.H{"error": "Необходима авторизация"})
				c.Abort()
				return
			}
			c.Redirect(http.StatusFound, "/login")
			c.Abort()
			return
		}
		username := user.(string)
		c.Set("username", username)

		// Load admin role and set in context
		role := storage.GetAdminRole(username)
		if role == "" {
			role = "admin" // fallback
		}
		c.Set("admin_role", role)

		c.Next()
	}
}

func setupMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		path := c.Request.URL.Path
		// Allow setup routes and static files
		if path == "/setup" || path == "/api/setup" || strings.HasPrefix(path, "/static") {
			c.Next()
			return
		}
		if !storage.IsSetupCompleted() {
			c.Redirect(http.StatusFound, "/setup")
			c.Abort()
			return
		}
		c.Next()
	}
}

func showSetup(c *gin.Context) {
	if storage.IsSetupCompleted() {
		c.Redirect(http.StatusFound, "/login")
		return
	}
	c.HTML(http.StatusOK, "setup.html", nil)
}

func showAdmin(c *gin.Context) {
	consoles := storage.LoadConsoles()
	users := storage.LoadUsers()
	rentals := storage.LoadRentals()
	requests := storage.LoadRequests()
	settings := storage.LoadSettings()
	discounts := storage.LoadDiscounts()

	// Add photo paths for consoles
	for id, console := range consoles {
		photoPath := getConsolePhotoURL(id)
		if photoPath != "" {
			console.PhotoPath = photoPath
			consoles[id] = console
		}
	}

	c.HTML(http.StatusOK, "admin.html", gin.H{
		"consoles":        consoles,
		"users":           users,
		"rentals":         rentals,
		"rental_requests": requests,
		"admin_settings":  settings,
		"discounts":       discounts,
	})
}

func getConsolePhotoURL(consoleID string) string {
	dir := filepath.Join("static", "img", "console")
	for _, ext := range []string{"png", "jpg", "jpeg", "gif", "webp"} {
		p := filepath.Join(dir, consoleID+"."+ext)
		if _, err := os.Stat(p); err == nil {
			return "/static/img/console/" + consoleID + "." + ext
		}
	}
	return ""
}
