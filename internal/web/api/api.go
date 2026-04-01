package api

import (
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"ps4-rental/internal/bot"
	"ps4-rental/internal/config"
	"ps4-rental/internal/models"
	"ps4-rental/internal/storage"
)

// ─── Consoles ─────────────────────────────────────────────────────────────────

func GetConsoles(c *gin.Context) {
	consoles := storage.LoadConsoles()
	c.JSON(http.StatusOK, gin.H{"status": "success", "consoles": consoles})
}

func CreateConsole(c *gin.Context) {
	var data map[string]interface{}
	if err := c.BindJSON(&data); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": err.Error()})
		return
	}

	consoles := storage.LoadConsoles()
	id := uuid.New().String()

	games := []string{}
	if g, ok := data["games"].([]interface{}); ok {
		for _, v := range g {
			if s, ok := v.(string); ok {
				games = append(games, s)
			}
		}
	}

	console := models.Console{
		ID:             id,
		Name:           strVal(data, "name"),
		Model:          strVal(data, "model"),
		Games:          games,
		RentalPrice:    floatVal(data, "rental_price"),
		SalePrice:      floatVal(data, "sale_price"),
		ShowPhotoInBot: boolVal(data, "show_photo_in_bot", true),
		Status:         "available",
		CreatedAt:      time.Now().Format(time.RFC3339),
	}
	consoles[id] = console
	_ = storage.SaveConsoles(consoles)
	c.JSON(http.StatusOK, gin.H{"status": "success", "console": console})
}

func UpdateConsole(c *gin.Context) {
	var data map[string]interface{}
	if err := c.BindJSON(&data); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": err.Error()})
		return
	}

	consoleID := strVal(data, "console_id")
	if consoleID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "ID консоли не указан"})
		return
	}

	consoles := storage.LoadConsoles()
	console, ok := consoles[consoleID]
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"status": "error", "message": "Консоль не найдена"})
		return
	}

	if v := strVal(data, "name"); v != "" { console.Name = v }
	if v := strVal(data, "model"); v != "" { console.Model = v }
	if v, ok := data["rental_price"]; ok { console.RentalPrice = toFloat(v) }
	if v, ok := data["sale_price"]; ok { console.SalePrice = toFloat(v) }
	if v, ok := data["show_photo_in_bot"]; ok {
		if b, ok := v.(bool); ok { console.ShowPhotoInBot = b }
	}
	if v, ok := data["photo_path"]; ok {
		if s, ok := v.(string); ok { console.PhotoPath = s }
	}
	if g, ok := data["games"].([]interface{}); ok {
		games := []string{}
		for _, v := range g {
			if s, ok := v.(string); ok { games = append(games, s) }
		}
		console.Games = games
	}
	console.UpdatedAt = time.Now().Format(time.RFC3339)

	consoles[consoleID] = console
	_ = storage.SaveConsoles(consoles)
	c.JSON(http.StatusOK, gin.H{"status": "success", "console": console})
}

func DeleteConsole(c *gin.Context) {
	consoleID := c.Query("console_id")
	if consoleID == "" {
		var data map[string]interface{}
		_ = c.BindJSON(&data)
		consoleID = strVal(data, "console_id")
	}
	if consoleID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "ID не указан"})
		return
	}

	consoles := storage.LoadConsoles()
	if _, ok := consoles[consoleID]; !ok {
		c.JSON(http.StatusNotFound, gin.H{"status": "error", "message": "Консоль не найдена"})
		return
	}
	delete(consoles, consoleID)
	_ = storage.SaveConsoles(consoles)
	c.JSON(http.StatusOK, gin.H{"status": "success"})
}

func UploadConsolePhoto(c *gin.Context) {
	consoleID := c.PostForm("console_id")
	if consoleID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "ID не указан"})
		return
	}

	file, header, err := c.Request.FormFile("photo")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": err.Error()})
		return
	}
	defer file.Close()

	ext := filepath.Ext(header.Filename)
	if ext == "" {
		ext = ".jpg"
	}

	dir := filepath.Join("static", "img", "console")
	_ = os.MkdirAll(dir, 0755)
	savePath := filepath.Join(dir, consoleID+ext)

	dst, err := os.Create(savePath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": err.Error()})
		return
	}
	defer dst.Close()
	_, _ = io.Copy(dst, file)

	photoURL := "/static/img/console/" + consoleID + ext

	// Update console record
	consoles := storage.LoadConsoles()
	if con, ok := consoles[consoleID]; ok {
		con.PhotoPath = photoURL
		consoles[consoleID] = con
		_ = storage.SaveConsoles(consoles)
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "photo_path": photoURL})
}

// ─── Users ────────────────────────────────────────────────────────────────────

func GetUsers(c *gin.Context) {
	users := storage.LoadUsers()
	c.JSON(http.StatusOK, gin.H{"status": "success", "users": users})
}

func UpdateUser(c *gin.Context) {
	var data map[string]interface{}
	if err := c.BindJSON(&data); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": err.Error()})
		return
	}
	userID := strVal(data, "user_id")
	users := storage.LoadUsers()
	u, ok := users[userID]
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"status": "error", "message": "Пользователь не найден"})
		return
	}
	if v := strVal(data, "full_name"); v != "" { u.FullName = v }
	if v := strVal(data, "phone_number"); v != "" { u.PhoneNumber = v }
	users[userID] = u
	_ = storage.SaveUsers(users)
	c.JSON(http.StatusOK, gin.H{"status": "success", "user": u})
}

func BanUser(c *gin.Context) {
	var data map[string]interface{}
	_ = c.BindJSON(&data)
	userID := strVal(data, "user_id")
	users := storage.LoadUsers()
	if u, ok := users[userID]; ok {
		u.IsBanned = true
		users[userID] = u
		_ = storage.SaveUsers(users)
		c.JSON(http.StatusOK, gin.H{"status": "success"})
	} else {
		c.JSON(http.StatusNotFound, gin.H{"status": "error", "message": "Пользователь не найден"})
	}
}

func UnbanUser(c *gin.Context) {
	var data map[string]interface{}
	_ = c.BindJSON(&data)
	userID := strVal(data, "user_id")
	users := storage.LoadUsers()
	if u, ok := users[userID]; ok {
		u.IsBanned = false
		users[userID] = u
		_ = storage.SaveUsers(users)
		c.JSON(http.StatusOK, gin.H{"status": "success"})
	} else {
		c.JSON(http.StatusNotFound, gin.H{"status": "error", "message": "Пользователь не найден"})
	}
}

func RequestUserLocation(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "success", "message": "Запрос геолокации отправлен"})
}

// ─── Rentals ──────────────────────────────────────────────────────────────────

func GetRentals(c *gin.Context) {
	rentals := storage.LoadRentals()
	c.JSON(http.StatusOK, gin.H{"status": "success", "rentals": rentals})
}

func EndRental(c *gin.Context) {
	var data map[string]interface{}
	if err := c.BindJSON(&data); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": err.Error()})
		return
	}

	rentalID := strVal(data, "rental_id")
	rentals := storage.LoadRentals()
	rental, ok := rentals[rentalID]
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"status": "error", "message": "Аренда не найдена"})
		return
	}
	if rental.Status != "active" {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "Аренда уже завершена"})
		return
	}

	startTime, _ := time.Parse(time.RFC3339, rental.StartTime)
	if startTime.IsZero() {
		startTime, _ = time.Parse("2006-01-02T15:04:05", rental.StartTime)
	}
	dur := time.Since(startTime)
	hours := int(dur.Hours())
	if hours < 1 { hours = 1 }

	consoles := storage.LoadConsoles()
	users := storage.LoadUsers()
	console := consoles[rental.ConsoleID]
	totalCost := float64(hours) * console.RentalPrice

	rental.EndTime = time.Now().Format(time.RFC3339)
	rental.Status = "completed"
	rental.TotalCost = totalCost
	rental.EndedBy = "admin"
	rentals[rentalID] = rental

	console.Status = "available"
	consoles[rental.ConsoleID] = console

	if u, ok := users[rental.UserID]; ok {
		u.TotalSpent += totalCost
		users[rental.UserID] = u
	}

	_ = storage.SaveRentals(rentals)
	_ = storage.SaveConsoles(consoles)
	_ = storage.SaveUsers(users)

	c.JSON(http.StatusOK, gin.H{"status": "success", "total_cost": totalCost, "hours": hours})
}

func StartRental(c *gin.Context) {
	var data map[string]interface{}
	if err := c.BindJSON(&data); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": err.Error()})
		return
	}

	rentalID := strVal(data, "rental_id")
	rentals := storage.LoadRentals()
	rental, ok := rentals[rentalID]
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"status": "error", "message": "Аренда не найдена"})
		return
	}
	if rental.Status != "booked" {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "Аренда не в статусе бронирования"})
		return
	}

	// Set status to awaiting_location — rental starts when user shares location
	rental.Status = "awaiting_location"
	rentals[rentalID] = rental
	_ = storage.SaveRentals(rentals)

	// Send location request to user
	bot.SendLocationRequest(rental.UserID)

	c.JSON(http.StatusOK, gin.H{"status": "success"})
}

func ExtendRental(c *gin.Context) {
	var data map[string]interface{}
	if err := c.BindJSON(&data); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": err.Error()})
		return
	}

	rentalID := strVal(data, "rental_id")
	hours := int(floatVal(data, "hours"))
	if hours < 1 || hours > 72 {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "Часы должны быть от 1 до 72"})
		return
	}

	rentals := storage.LoadRentals()
	rental, ok := rentals[rentalID]
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"status": "error", "message": "Аренда не найдена"})
		return
	}
	if rental.Status != "active" {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "Аренда не активна"})
		return
	}

	// Extend ExpectedEndTime
	var currentEnd time.Time
	if rental.ExpectedEndTime != "" {
		currentEnd, _ = time.Parse(time.RFC3339, rental.ExpectedEndTime)
	}
	if currentEnd.IsZero() || currentEnd.Before(time.Now()) {
		currentEnd = time.Now()
	}
	newEnd := currentEnd.Add(time.Duration(hours) * time.Hour)
	rental.ExpectedEndTime = newEnd.Format(time.RFC3339)
	rental.SelectedHours += hours
	rental.DurationHours += hours

	consoles := storage.LoadConsoles()
	console := consoles[rental.ConsoleID]
	extraCost := float64(hours) * console.RentalPrice
	rental.ExpectedCost += extraCost

	rentals[rentalID] = rental
	_ = storage.SaveRentals(rentals)

	// Notify user
	msg := fmt.Sprintf("⏰ *Аренда продлена!*\n\n🎮 Консоль: *%s*\n➕ Добавлено: *%d ч.*\n📅 Новое время окончания: *%s*\n💰 Доп. стоимость: *%.0f лей*",
		console.Name, hours, newEnd.Format("02.01.2006 15:04"), extraCost)
	bot.SendToUser(rental.UserID, msg)

	c.JSON(http.StatusOK, gin.H{"status": "success", "new_end": newEnd.Format("02.01.2006 15:04"), "extra_cost": extraCost})
}

// ─── Rental Requests ──────────────────────────────────────────────────────────

func GetRentalRequests(c *gin.Context) {
	requests := storage.LoadRequests()
	c.JSON(http.StatusOK, gin.H{"status": "success", "requests": requests})
}

func ApproveRentalRequest(c *gin.Context) {
	var data map[string]interface{}
	_ = c.BindJSON(&data)
	requestID := strVal(data, "request_id")

	requests := storage.LoadRequests()
	req, ok := requests[requestID]
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"status": "error", "message": "Заявка не найдена"})
		return
	}
	req.Status = "approved"
	requests[requestID] = req
	_ = storage.SaveRequests(requests)

	// Create rental record and mark console as rented
	consoles := storage.LoadConsoles()
	console := consoles[req.ConsoleID]

	now := time.Now()
	rentalID := uuid.New().String()
	hours := req.SelectedHours
	if hours == 0 {
		hours = req.DurationHours
	}
	if hours == 0 {
		hours = 1
	}

	rentals := storage.LoadRentals()
	rentals[rentalID] = models.Rental{
		ID:            rentalID,
		UserID:        req.UserID,
		ConsoleID:     req.ConsoleID,
		SelectedHours: hours,
		DurationHours: hours,
		ExpectedCost:  req.TotalCost,
		TotalCost:     req.TotalCost,
		Status:        "booked",
		CreatedAt:     now.Format(time.RFC3339),
	}
	_ = storage.SaveRentals(rentals)

	// Mark console as rented
	console.Status = "rented"
	consoles[req.ConsoleID] = console
	_ = storage.SaveConsoles(consoles)

	settings := storage.LoadSettings()

	msg := fmt.Sprintf(
		"📌 *Консоль забронирована!*\n\n🎮 Консоль: *%s*\n💰 Стоимость: *%.0f лей*\n⏳ Аренда начнётся когда вы заберёте консоль\n\n📍 Приходите забирать по адресу ниже 👇",
		console.Name, req.TotalCost,
	)
	bot.SendToUser(req.UserID, msg)

	// Send shop location if configured
	if settings.ShopLatitude != 0 && settings.ShopLongitude != 0 {
		title := settings.ShopTitle
		if title == "" {
			title = "Пункт выдачи"
		}
		address := settings.ShopAddress
		if address == "" {
			address = "Адрес не указан"
		}
		bot.SendLocationToUser(req.UserID, settings.ShopLatitude, settings.ShopLongitude, title, address)
	}

	c.JSON(http.StatusOK, gin.H{"status": "success"})
}

func RejectRentalRequest(c *gin.Context) {
	var data map[string]interface{}
	_ = c.BindJSON(&data)
	requestID := strVal(data, "request_id")

	requests := storage.LoadRequests()
	req, ok := requests[requestID]
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"status": "error", "message": "Заявка не найдена"})
		return
	}
	req.Status = "rejected"
	requests[requestID] = req
	_ = storage.SaveRequests(requests)

	// Notify user via Telegram
	consoles := storage.LoadConsoles()
	console := consoles[req.ConsoleID]
	msg := fmt.Sprintf(
		"❌ *Ваша заявка отклонена.*\n\n🎮 Консоль: *%s*\n\nЕсли есть вопросы — обратитесь к администратору.",
		console.Name,
	)
	bot.SendToUser(req.UserID, msg)

	c.JSON(http.StatusOK, gin.H{"status": "success"})
}

// ─── Discounts ────────────────────────────────────────────────────────────────

func GetDiscounts(c *gin.Context) {
	discounts := storage.LoadDiscounts()
	c.JSON(http.StatusOK, gin.H{"status": "success", "discounts": discounts})
}

func CreateDiscount(c *gin.Context) {
	var data map[string]interface{}
	if err := c.BindJSON(&data); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": err.Error()})
		return
	}
	discounts := storage.LoadDiscounts()
	id := uuid.New().String()
	discount := models.Discount{
		ID:        id,
		ConsoleID: strVal(data, "console_id"),
		Type:      strVal(data, "type"),
		Value:     floatVal(data, "value"),
		StartDate: strVal(data, "start_date"),
		EndDate:   strVal(data, "end_date"),
		Active:    boolVal(data, "active", true),
	}
	discounts[id] = discount
	_ = storage.SaveDiscounts(discounts)
	c.JSON(http.StatusOK, gin.H{"status": "success", "discount": discount})
}

func UpdateDiscount(c *gin.Context) {
	var data map[string]interface{}
	_ = c.BindJSON(&data)
	id := strVal(data, "discount_id")
	discounts := storage.LoadDiscounts()
	d, ok := discounts[id]
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"status": "error", "message": "Скидка не найдена"})
		return
	}
	if v, ok := data["active"].(bool); ok { d.Active = v }
	if v := strVal(data, "start_date"); v != "" { d.StartDate = v }
	if v := strVal(data, "end_date"); v != "" { d.EndDate = v }
	if v, ok := data["value"]; ok { d.Value = toFloat(v) }
	discounts[id] = d
	_ = storage.SaveDiscounts(discounts)
	c.JSON(http.StatusOK, gin.H{"status": "success", "discount": d})
}

func DeleteDiscount(c *gin.Context) {
	id := c.Query("discount_id")
	discounts := storage.LoadDiscounts()
	delete(discounts, id)
	_ = storage.SaveDiscounts(discounts)
	c.JSON(http.StatusOK, gin.H{"status": "success"})
}

// ─── Settings ─────────────────────────────────────────────────────────────────

func GetSettings(c *gin.Context) {
	s := storage.LoadSettings()
	c.JSON(http.StatusOK, gin.H{"status": "success", "settings": s})
}

func UpdateSettings(c *gin.Context) {
	var s models.AdminSettings
	if err := c.BindJSON(&s); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": err.Error()})
		return
	}
	_ = storage.SaveSettings(s)
	c.JSON(http.StatusOK, gin.H{"status": "success", "settings": s})
}

// ─── Ratings ──────────────────────────────────────────────────────────────────

func GetRatings(c *gin.Context) {
	users := storage.LoadUsers()
	ratings := make(map[string]interface{})

	for userID := range users {
		rating := calcUserRating(userID)
		if rating != nil {
			ratings[userID] = rating
		}
	}
	c.JSON(http.StatusOK, gin.H{"status": "success", "ratings": ratings})
}

func GetUserRating(c *gin.Context) {
	userID := c.Param("user_id")
	rating := calcUserRating(userID)
	if rating == nil {
		c.JSON(http.StatusNotFound, gin.H{"status": "error", "message": "Рейтинг не найден"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "success", "rating": rating})
}

func AddDisciplinePoints(c *gin.Context) {
	var data map[string]interface{}
	_ = c.BindJSON(&data)
	userID := strVal(data, "user_id")
	points := int(toFloat(data["points"]))
	comment := strVal(data, "comment")

	r := storage.LoadRatings()
	if r.Transactions == nil {
		r.Transactions = make(map[string][]models.RatingTransaction)
	}
	r.Transactions[userID] = append(r.Transactions[userID], models.RatingTransaction{
		Type:    "discipline_manual",
		Points:  points,
		Comment: comment,
		Date:    time.Now().Format(time.RFC3339),
	})
	_ = storage.SaveRatings(r)
	c.JSON(http.StatusOK, gin.H{"status": "success"})
}

func AddLoyaltyPoints(c *gin.Context) {
	var data map[string]interface{}
	_ = c.BindJSON(&data)
	userID := strVal(data, "user_id")
	points := int(toFloat(data["points"]))

	users := storage.LoadUsers()
	if u, ok := users[userID]; ok {
		u.LoyaltyBonus += points
		users[userID] = u
		_ = storage.SaveUsers(users)
	}
	c.JSON(http.StatusOK, gin.H{"status": "success"})
}

// ─── Calendar ─────────────────────────────────────────────────────────────────

func GetCalendar(c *gin.Context) {
	cal := storage.LoadCalendar()
	c.JSON(http.StatusOK, gin.H{"status": "success", "calendar": cal})
}

func AddBlockedDate(c *gin.Context) {
	var data map[string]interface{}
	_ = c.BindJSON(&data)
	date := strVal(data, "date")
	cal := storage.LoadCalendar()
	for _, d := range cal.SystemBlockedDates {
		if d == date { c.JSON(http.StatusOK, gin.H{"status": "success"}); return }
	}
	cal.SystemBlockedDates = append(cal.SystemBlockedDates, date)
	_ = storage.SaveCalendar(cal)
	c.JSON(http.StatusOK, gin.H{"status": "success"})
}

func RemoveBlockedDate(c *gin.Context) {
	date := c.Query("date")
	cal := storage.LoadCalendar()
	var updated []string
	for _, d := range cal.SystemBlockedDates {
		if d != date { updated = append(updated, d) }
	}
	cal.SystemBlockedDates = updated
	_ = storage.SaveCalendar(cal)
	c.JSON(http.StatusOK, gin.H{"status": "success"})
}

func AddConsoleBlockedDate(c *gin.Context) {
	var data map[string]interface{}
	_ = c.BindJSON(&data)
	consoleID := strVal(data, "console_id")
	date := strVal(data, "date")
	cal := storage.LoadCalendar()
	if cal.ConsoleBlockedDates == nil {
		cal.ConsoleBlockedDates = make(map[string][]string)
	}
	cal.ConsoleBlockedDates[consoleID] = append(cal.ConsoleBlockedDates[consoleID], date)
	_ = storage.SaveCalendar(cal)
	c.JSON(http.StatusOK, gin.H{"status": "success"})
}

func RemoveConsoleBlockedDate(c *gin.Context) {
	consoleID := c.Query("console_id")
	date := c.Query("date")
	cal := storage.LoadCalendar()
	if dates, ok := cal.ConsoleBlockedDates[consoleID]; ok {
		var updated []string
		for _, d := range dates {
			if d != date { updated = append(updated, d) }
		}
		cal.ConsoleBlockedDates[consoleID] = updated
	}
	_ = storage.SaveCalendar(cal)
	c.JSON(http.StatusOK, gin.H{"status": "success"})
}

func AddHoliday(c *gin.Context) {
	var h models.Holiday
	if err := c.BindJSON(&h); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": err.Error()})
		return
	}
	cal := storage.LoadCalendar()
	cal.Holidays = append(cal.Holidays, h)
	_ = storage.SaveCalendar(cal)
	c.JSON(http.StatusOK, gin.H{"status": "success"})
}

func UpdateCalendarSettings(c *gin.Context) {
	var settings models.CalendarSettings
	if err := c.BindJSON(&settings); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": err.Error()})
		return
	}
	cal := storage.LoadCalendar()
	cal.Settings = settings
	_ = storage.SaveCalendar(cal)
	c.JSON(http.StatusOK, gin.H{"status": "success"})
}

// ─── Documents ────────────────────────────────────────────────────────────────

func GetUserDocuments(c *gin.Context) {
	userName := c.Param("user_name")
	userFolder := filepath.Join(config.App.PassportDir, userName)

	docs := map[string]interface{}{
		"passport_front":       false,
		"passport_back":        false,
		"selfie_with_passport": false,
	}

	for docType := range docs {
		for _, ext := range []string{"jpg", "jpeg", "png", "webp"} {
			fp := filepath.Join(userFolder, docType+"."+ext)
			if _, err := os.Stat(fp); err == nil {
				docs[docType] = "/api/documents/serve/" + userName + "/" + docType + "." + ext
				break
			}
		}
	}
	c.JSON(http.StatusOK, gin.H{"status": "success", "documents": docs})
}

func ServeDocument(c *gin.Context) {
	userName := c.Param("user_name")
	docType := c.Param("doc_type")

	fp := filepath.Join(config.App.PassportDir, userName, docType)
	if _, err := os.Stat(fp); err != nil {
		// Try without extension
		for _, ext := range []string{"jpg", "jpeg", "png", "webp"} {
			base := strings.TrimSuffix(docType, "."+ext)
			fp2 := filepath.Join(config.App.PassportDir, userName, base+"."+ext)
			if _, err := os.Stat(fp2); err == nil {
				c.File(fp2)
				return
			}
		}
		c.JSON(http.StatusNotFound, gin.H{"error": "Файл не найден"})
		return
	}
	c.File(fp)
}

// ─── Performance ──────────────────────────────────────────────────────────────

func GetPerformance(c *gin.Context) {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	rentals := storage.LoadRentals()
	users := storage.LoadUsers()
	consoles := storage.LoadConsoles()

	activeRentals := 0
	for _, r := range rentals {
		if r.Status == "active" {
			activeRentals++
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"performance": gin.H{
			"memory_mb":       math.Round(float64(memStats.Alloc)/1024/1024*100) / 100,
			"goroutines":      runtime.NumGoroutine(),
			"total_users":     len(users),
			"total_consoles":  len(consoles),
			"active_rentals":  activeRentals,
			"total_rentals":   len(rentals),
			"go_version":      runtime.Version(),
			"os":              runtime.GOOS,
			"arch":            runtime.GOARCH,
		},
	})
}

// ─── Helper functions ─────────────────────────────────────────────────────────

func strVal(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func floatVal(m map[string]interface{}, key string) float64 {
	if v, ok := m[key]; ok {
		return toFloat(v)
	}
	return 0
}

func toFloat(v interface{}) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int:
		return float64(n)
	case int64:
		return float64(n)
	}
	return 0
}

func boolVal(m map[string]interface{}, key string, defaultVal bool) bool {
	if v, ok := m[key]; ok {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return defaultVal
}

func calcUserRating(userID string) *models.UserRatingResult {
	r := storage.LoadRatings()
	users := storage.LoadUsers()
	user, ok := users[userID]
	if !ok {
		return nil
	}

	transactions := r.Transactions[userID]
	settings := r.Settings

	dw := settings.DisciplineWeight
	lw := settings.LoyaltyWeight
	if dw == 0 { dw = 0.6 }
	if lw == 0 { lw = 0.4 }

	// Discipline
	discipline := 50
	if len(transactions) > 0 {
		window := settings.TransactionsWindow
		if window == 0 { window = 5 }
		recent := transactions
		if len(recent) > window { recent = recent[len(recent)-window:] }
		sum := 0
		for _, tx := range recent {
			s := 100 + tx.Points
			if s < 0 { s = 0 }
			if s > 100 { s = 100 }
			sum += s
		}
		discipline = sum / len(recent)
	}

	// Loyalty
	loyalty := 0
	rentals := storage.LoadRentals()
	count := 0
	for _, rental := range rentals {
		if rental.UserID == userID { count++ }
	}
	loyalty += min(count*5, 30)
	if user.PromotionParticipation { loyalty += 10 }
	if user.JoinedAt != "" {
		joinDate, _ := time.Parse(time.RFC3339, user.JoinedAt)
		if !joinDate.IsZero() {
			days := int(time.Since(joinDate).Hours() / 24)
			if days >= 365 { loyalty += 20 } else if days >= 180 { loyalty += 10 }
		}
	}
	loyalty += user.LoyaltyBonus
	if loyalty > 100 { loyalty = 100 }
	if loyalty < 0 { loyalty = 0 }

	finalScore := int(math.Round(float64(discipline)*dw + float64(loyalty)*lw))
	if finalScore < 0 { finalScore = 0 }
	if finalScore > 100 { finalScore = 100 }

	thresholds := settings.StatusThresholds
	premium := 80
	regular := 50
	if v, ok := thresholds["premium"]; ok { premium = v }
	if v, ok := thresholds["regular"]; ok { regular = v }

	status := "risk"
	statusName := "Риск ⚠️"
	if finalScore >= premium {
		status = "premium"
		statusName = "Premium ⭐"
	} else if finalScore >= regular {
		status = "regular"
		statusName = "Обычный 👤"
	}

	return &models.UserRatingResult{
		UserID:       userID,
		FinalScore:   finalScore,
		Discipline:   discipline,
		Loyalty:      loyalty,
		Status:       status,
		StatusName:   statusName,
		CalculatedAt: time.Now(),
	}
}


// ─── Telegram Admins ──────────────────────────────────────────────────────────

func GetTelegramAdmins(c *gin.Context) {
	admins := storage.LoadTelegramAdmins()
	c.JSON(http.StatusOK, gin.H{"status": "success", "admins": admins})
}

func AddTelegramAdmin(c *gin.Context) {
	var data map[string]interface{}
	if err := c.BindJSON(&data); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": err.Error()})
		return
	}

	telegramID := strVal(data, "telegram_id")
	if telegramID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "telegram_id обязателен"})
		return
	}

	// Check if user exists
	users := storage.LoadUsers()
	user, userExists := users[telegramID]

	admin := models.TelegramAdmin{
		TelegramID: telegramID,
		Username:   strVal(data, "username"),
		FullName:   strVal(data, "full_name"),
		AddedAt:    time.Now().Format(time.RFC3339),
		AddedBy:    c.GetString("username"),
	}

	// If user exists, use their data
	if userExists {
		if admin.Username == "" {
			admin.Username = user.Username
		}
		if admin.FullName == "" {
			admin.FullName = user.FullName
		}
	}

	if err := storage.AddTelegramAdmin(admin); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "message": "Telegram админ добавлен", "admin": admin})
}

func RemoveTelegramAdmin(c *gin.Context) {
	var data map[string]interface{}
	if err := c.BindJSON(&data); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": err.Error()})
		return
	}

	telegramID := strVal(data, "telegram_id")
	if telegramID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "telegram_id обязателен"})
		return
	}

	if err := storage.RemoveTelegramAdmin(telegramID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "message": "Telegram админ удалён"})
}
