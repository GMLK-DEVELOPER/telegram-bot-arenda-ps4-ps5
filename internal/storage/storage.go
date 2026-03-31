package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"ps4-rental/internal/config"
	"ps4-rental/internal/db"
	"ps4-rental/internal/models"
)

// ── Helpers ───────────────────────────────────────────────────────────────────

func parseTime(s string) *time.Time {
	if s == "" {
		return nil
	}
	for _, layout := range []string{time.RFC3339, "2006-01-02T15:04:05Z", "2006-01-02 15:04:05"} {
		if t, err := time.Parse(layout, s); err == nil {
			return &t
		}
	}
	return nil
}

func timeStr(t *time.Time) string {
	if t == nil || t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}

// ── Consoles ──────────────────────────────────────────────────────────────────

func LoadConsoles() map[string]models.Console {
	var rows []db.Console
	db.DB.Find(&rows)
	result := make(map[string]models.Console)
	for _, r := range rows {
		result[r.ID] = models.Console{
			ID: r.ID, Name: r.Name, Model: r.Model,
			Games: []string(r.Games), RentalPrice: r.RentalPrice,
			SalePrice: r.SalePrice, Status: r.Status,
			PhotoPath: r.PhotoPath, ShowPhotoInBot: r.ShowPhotoInBot,
			CreatedAt: r.CreatedAt.Format(time.RFC3339),
			UpdatedAt: r.UpdatedAt.Format(time.RFC3339),
		}
	}
	return result
}

func SaveConsoles(consoles map[string]models.Console) error {
	return db.DB.Transaction(func(tx *gorm.DB) error {
		for _, c := range consoles {
			row := db.Console{
				ID: c.ID, Name: c.Name, Model: c.Model,
				Games: db.StringSlice(c.Games), RentalPrice: c.RentalPrice,
				SalePrice: c.SalePrice, Status: c.Status,
				PhotoPath: c.PhotoPath, ShowPhotoInBot: c.ShowPhotoInBot,
			}
			if err := tx.Clauses(clause.OnConflict{UpdateAll: true}).Create(&row).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

func SaveConsole(c models.Console) error {
	row := db.Console{
		ID: c.ID, Name: c.Name, Model: c.Model,
		Games: db.StringSlice(c.Games), RentalPrice: c.RentalPrice,
		SalePrice: c.SalePrice, Status: c.Status,
		PhotoPath: c.PhotoPath, ShowPhotoInBot: c.ShowPhotoInBot,
	}
	return db.DB.Clauses(clause.OnConflict{UpdateAll: true}).Create(&row).Error
}

func DeleteConsole(id string) error {
	return db.DB.Delete(&db.Console{}, "id = ?", id).Error
}

// ── Users ─────────────────────────────────────────────────────────────────────

func LoadUsers() map[string]models.User {
	var rows []db.User
	db.DB.Find(&rows)
	result := make(map[string]models.User)
	for _, r := range rows {
		result[r.ID] = dbUserToModel(r)
	}
	return result
}

func SaveUsers(users map[string]models.User) error {
	return db.DB.Transaction(func(tx *gorm.DB) error {
		for _, u := range users {
			row := modelUserToDB(u)
			if err := tx.Clauses(clause.OnConflict{UpdateAll: true}).Create(&row).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

func LoadUser(userID string) (models.User, bool) {
	var row db.User
	if err := db.DB.First(&row, "id = ?", userID).Error; err != nil {
		return models.User{}, false
	}
	return dbUserToModel(row), true
}

func SaveUser(user models.User) error {
	row := modelUserToDB(user)
	return db.DB.Clauses(clause.OnConflict{UpdateAll: true}).Create(&row).Error
}

func dbUserToModel(r db.User) models.User {
	return models.User{
		ID: r.ID, Username: r.Username, FirstName: r.FirstName,
		LastName: r.LastName, FullName: r.FullName,
		PhoneNumber: r.PhoneNumber, IsBanned: r.IsBanned,
		TotalSpent: r.TotalSpent, JoinedAt: r.JoinedAt.Format(time.RFC3339),
		RegistrationStep: r.RegistrationStep, VerificationStep: r.VerificationStep,
		PendingRentalID:  r.PendingRentalID,
		PassportFrontFile: r.PassportFrontFile, PassportBackFile: r.PassportBackFile,
		SelfieFile: r.SelfieFile, BotBlocked: r.BotBlocked,
		BotBlockedAt: timeStr(r.BotBlockedAt),
		LoyaltyBonus: r.LoyaltyBonus, PromotionParticipation: r.PromotionParticipation,
	}
}

func modelUserToDB(u models.User) db.User {
	row := db.User{
		ID: u.ID, Username: u.Username, FirstName: u.FirstName,
		LastName: u.LastName, FullName: u.FullName,
		PhoneNumber: u.PhoneNumber, IsBanned: u.IsBanned,
		TotalSpent: u.TotalSpent, RegistrationStep: u.RegistrationStep,
		VerificationStep: u.VerificationStep, PendingRentalID: u.PendingRentalID,
		PassportFrontFile: u.PassportFrontFile, PassportBackFile: u.PassportBackFile,
		SelfieFile: u.SelfieFile, BotBlocked: u.BotBlocked,
		LoyaltyBonus: u.LoyaltyBonus, PromotionParticipation: u.PromotionParticipation,
	}
	if t := parseTime(u.JoinedAt); t != nil {
		row.JoinedAt = *t
	}
	row.BotBlockedAt = parseTime(u.BotBlockedAt)
	return row
}

// ── Rentals ───────────────────────────────────────────────────────────────────

func LoadRentals() map[string]models.Rental {
	var rows []db.Rental
	db.DB.Find(&rows)
	result := make(map[string]models.Rental)
	for _, r := range rows {
		result[r.ID] = dbRentalToModel(r)
	}
	return result
}

func SaveRentals(rentals map[string]models.Rental) error {
	return db.DB.Transaction(func(tx *gorm.DB) error {
		for _, r := range rentals {
			row := modelRentalToDB(r)
			if err := tx.Clauses(clause.OnConflict{UpdateAll: true}).Create(&row).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

func LoadRental(rentalID string) (models.Rental, bool) {
	var row db.Rental
	if err := db.DB.First(&row, "id = ?", rentalID).Error; err != nil {
		return models.Rental{}, false
	}
	return dbRentalToModel(row), true
}

func SaveRental(rental models.Rental) error {
	row := modelRentalToDB(rental)
	return db.DB.Clauses(clause.OnConflict{UpdateAll: true}).Create(&row).Error
}

func dbRentalToModel(r db.Rental) models.Rental {
	m := models.Rental{
		ID: r.ID, UserID: r.UserID, ConsoleID: r.ConsoleID,
		StartTime: timeStr(r.StartTime), ExpectedEndTime: timeStr(r.ExpectedEndTime),
		EndTime: timeStr(r.EndTime), SelectedHours: r.SelectedHours,
		DurationHours: r.DurationHours, ExpectedCost: r.ExpectedCost,
		TotalCost: r.TotalCost, Status: r.Status, EndedBy: r.EndedBy,
		CreatedAt: r.CreatedAt.Format(time.RFC3339),
		Reminder2hSent: r.Reminder2hSent, Reminder1hSent: r.Reminder1hSent,
		Reminder30mSent: r.Reminder30mSent, Reminder10mSent: r.Reminder10mSent,
	}
	if r.LocationJSON != "" {
		var loc models.Location
		if err := json.Unmarshal([]byte(r.LocationJSON), &loc); err == nil {
			m.Location = &loc
		}
	}
	return m
}

func modelRentalToDB(r models.Rental) db.Rental {
	row := db.Rental{
		ID: r.ID, UserID: r.UserID, ConsoleID: r.ConsoleID,
		SelectedHours: r.SelectedHours, DurationHours: r.DurationHours,
		ExpectedCost: r.ExpectedCost, TotalCost: r.TotalCost,
		Status: r.Status, EndedBy: r.EndedBy,
		Reminder2hSent: r.Reminder2hSent, Reminder1hSent: r.Reminder1hSent,
		Reminder30mSent: r.Reminder30mSent, Reminder10mSent: r.Reminder10mSent,
	}
	row.StartTime = parseTime(r.StartTime)
	row.ExpectedEndTime = parseTime(r.ExpectedEndTime)
	row.EndTime = parseTime(r.EndTime)
	if r.Location != nil {
		if b, err := json.Marshal(r.Location); err == nil {
			row.LocationJSON = string(b)
		}
	}
	return row
}

// ── Rental Requests ───────────────────────────────────────────────────────────

func LoadRequests() map[string]models.RentalRequest {
	var rows []db.RentalRequest
	db.DB.Find(&rows)
	result := make(map[string]models.RentalRequest)
	for _, r := range rows {
		result[r.ID] = models.RentalRequest{
			ID: r.ID, UserID: r.UserID, ConsoleID: r.ConsoleID,
			SelectedHours: r.SelectedHours, DurationHours: r.DurationHours,
			ExpectedCost: r.ExpectedCost, TotalCost: r.TotalCost,
			RequestTime: timeStr(r.RequestTime),
			CreatedAt:   r.CreatedAt.Format(time.RFC3339),
			Status: r.Status, StartTime: timeStr(r.StartTime),
			EstimatedEndTime: timeStr(r.EstimatedEndTime), RentalID: r.RentalID,
		}
	}
	return result
}

func SaveRequests(requests map[string]models.RentalRequest) error {
	return db.DB.Transaction(func(tx *gorm.DB) error {
		for _, r := range requests {
			row := db.RentalRequest{
				ID: r.ID, UserID: r.UserID, ConsoleID: r.ConsoleID,
				SelectedHours: r.SelectedHours, DurationHours: r.DurationHours,
				ExpectedCost: r.ExpectedCost, TotalCost: r.TotalCost,
				Status: r.Status, RentalID: r.RentalID,
			}
			row.RequestTime = parseTime(r.RequestTime)
			if row.RequestTime == nil {
				row.RequestTime = parseTime(r.CreatedAt)
			}
			row.StartTime = parseTime(r.StartTime)
			row.EstimatedEndTime = parseTime(r.EstimatedEndTime)
			if err := tx.Clauses(clause.OnConflict{UpdateAll: true}).Create(&row).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

// ── Admins (web panel) ────────────────────────────────────────────────────────

func LoadAdmins() map[string]models.Admin {
	var rows []db.Admin
	db.DB.Find(&rows)
	result := make(map[string]models.Admin)
	for _, r := range rows {
		result[r.Username] = models.Admin{
			Username: r.Username, Password: r.Password,
			Role: r.Role, CreatedAt: r.CreatedAt.Format(time.RFC3339),
		}
	}
	return result
}

func SaveAdmins(admins map[string]models.Admin) error {
	return db.DB.Transaction(func(tx *gorm.DB) error {
		for _, a := range admins {
			row := db.Admin{Username: a.Username, Password: a.Password, Role: a.Role}
			if err := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "username"}},
				DoUpdates: clause.AssignmentColumns([]string{"password", "role"}),
			}).Create(&row).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

func AdminExists(username, password string) bool {
	var row db.Admin
	if err := db.DB.Where("username = ?", username).First(&row).Error; err != nil {
		return false
	}
	// bcrypt check
	if err := bcrypt.CompareHashAndPassword([]byte(row.Password), []byte(password)); err == nil {
		return true
	}
	// legacy plaintext fallback
	return row.Password == password
}

// ── Settings ──────────────────────────────────────────────────────────────────

func LoadSettings() models.AdminSettings {
	var cfg db.SystemConfig
	if err := db.DB.First(&cfg, "key = ?", "admin_settings").Error; err != nil {
		s := models.DefaultAdminSettings(config.App.AdminTelegramID)
		_ = SaveSettings(s)
		return s
	}
	var s models.AdminSettings
	if err := json.Unmarshal([]byte(cfg.Value), &s); err != nil {
		return models.DefaultAdminSettings(config.App.AdminTelegramID)
	}
	if s.MaxRentalHours == 0 {
		s.MaxRentalHours = 24
	}
	if s.NotificationFrequency == 0 {
		s.NotificationFrequency = 5
	}
	if len(s.WorkingDays) == 0 {
		s.WorkingDays = []int{1, 2, 3, 4, 5, 6, 7}
	}
	return s
}

func SaveSettings(s models.AdminSettings) error {
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return db.DB.Clauses(clause.OnConflict{UpdateAll: true}).
		Create(&db.SystemConfig{Key: "admin_settings", Value: string(b)}).Error
}

func GetAdminChatID() string {
	s := LoadSettings()
	if s.AdminChatID != "" {
		return s.AdminChatID
	}
	return config.App.AdminTelegramID
}

// ── Discounts ─────────────────────────────────────────────────────────────────

func LoadDiscounts() map[string]models.Discount {
	var rows []db.Discount
	db.DB.Find(&rows)
	result := make(map[string]models.Discount)
	for _, r := range rows {
		result[r.ID] = models.Discount{
			ID: r.ID, ConsoleID: r.ConsoleID, Type: r.Type,
			Value: r.Value, Amount: r.Amount,
			StartDate: timeStr(r.StartDate), EndDate: timeStr(r.EndDate),
			Active: r.Active, MinHours: r.MinHours,
		}
	}
	return result
}

func SaveDiscounts(discounts map[string]models.Discount) error {
	return db.DB.Transaction(func(tx *gorm.DB) error {
		for _, d := range discounts {
			row := db.Discount{
				ID: d.ID, ConsoleID: d.ConsoleID, Type: d.Type,
				Value: d.Value, Amount: d.Amount, Active: d.Active, MinHours: d.MinHours,
			}
			row.StartDate = parseTime(d.StartDate)
			row.EndDate = parseTime(d.EndDate)
			if err := tx.Clauses(clause.OnConflict{UpdateAll: true}).Create(&row).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

// ── Ratings ───────────────────────────────────────────────────────────────────

func LoadRatings() models.Ratings {
	r := models.DefaultRatings()

	var cfg db.SystemConfig
	if err := db.DB.First(&cfg, "key = ?", "rating_settings").Error; err == nil {
		_ = json.Unmarshal([]byte(cfg.Value), &r.Settings)
	}

	var txns []db.RatingTransaction
	db.DB.Find(&txns)
	for _, t := range txns {
		r.Transactions[t.UserID] = append(r.Transactions[t.UserID], models.RatingTransaction{
			Type: t.Type, Points: t.Points, Comment: t.Comment,
			Date: t.Date.Format(time.RFC3339), AutoGenerated: t.AutoGenerated,
		})
	}

	var ratings []db.UserRating
	db.DB.Find(&ratings)
	for _, ur := range ratings {
		r.UserRatings[ur.UserID] = models.UserRating{
			UserID: ur.UserID, FinalScore: ur.FinalScore,
			Discipline: ur.Discipline, Loyalty: ur.Loyalty,
			Status: ur.Status, StatusName: ur.StatusName,
			CalculatedAt: ur.CalculatedAt.Format(time.RFC3339),
		}
	}
	return r
}

func SaveRatings(r models.Ratings) error {
	return db.DB.Transaction(func(tx *gorm.DB) error {
		if b, err := json.Marshal(r.Settings); err == nil {
			tx.Clauses(clause.OnConflict{UpdateAll: true}).
				Create(&db.SystemConfig{Key: "rating_settings", Value: string(b)})
		}
		tx.Where("1 = 1").Delete(&db.RatingTransaction{})
		for userID, txnList := range r.Transactions {
			for _, t := range txnList {
				row := db.RatingTransaction{
					UserID: userID, Type: t.Type, Points: t.Points,
					Comment: t.Comment, AutoGenerated: t.AutoGenerated,
				}
				if d := parseTime(t.Date); d != nil {
					row.Date = *d
				}
				tx.Create(&row)
			}
		}
		tx.Where("1 = 1").Delete(&db.UserRating{})
		for _, ur := range r.UserRatings {
			row := db.UserRating{
				UserID: ur.UserID, FinalScore: ur.FinalScore,
				Discipline: ur.Discipline, Loyalty: ur.Loyalty,
				Status: ur.Status, StatusName: ur.StatusName,
			}
			if t := parseTime(ur.CalculatedAt); t != nil {
				row.CalculatedAt = *t
			}
			tx.Create(&row)
		}
		return nil
	})
}

// ── Temp Reservations ─────────────────────────────────────────────────────────

func LoadReservations() map[string]models.TempReservation {
	var rows []db.TempReservation
	db.DB.Find(&rows)
	result := make(map[string]models.TempReservation)
	for _, r := range rows {
		result[r.ID] = models.TempReservation{
			ID: r.ID, UserID: r.UserID, ConsoleID: r.ConsoleID,
			CreatedAt: r.CreatedAt.Format(time.RFC3339),
			ExpiresAt: timeStr(r.ExpiresAt), Status: r.Status,
		}
	}
	return result
}

func SaveReservations(reservations map[string]models.TempReservation) error {
	return db.DB.Transaction(func(tx *gorm.DB) error {
		tx.Where("1 = 1").Delete(&db.TempReservation{})
		for _, r := range reservations {
			row := db.TempReservation{
				ID: r.ID, UserID: r.UserID, ConsoleID: r.ConsoleID,
				Status: r.Status,
			}
			row.ExpiresAt = parseTime(r.ExpiresAt)
			tx.Create(&row)
		}
		return nil
	})
}

// ── Calendar ──────────────────────────────────────────────────────────────────

func LoadCalendar() models.CalendarData {
	var cfg db.SystemConfig
	if err := db.DB.First(&cfg, "key = ?", "calendar").Error; err != nil {
		cal := defaultCalendar()
		_ = SaveCalendar(cal) // auto-save so it stops being "not found"
		return cal
	}
	var c models.CalendarData
	if err := json.Unmarshal([]byte(cfg.Value), &c); err != nil {
		return defaultCalendar()
	}
	if c.ConsoleBlockedDates == nil {
		c.ConsoleBlockedDates = make(map[string][]string)
	}
	if len(c.WorkingDays) == 0 {
		c.WorkingDays = []int{1, 2, 3, 4, 5, 6, 7}
	}
	return c
}

func SaveCalendar(c models.CalendarData) error {
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	return db.DB.Clauses(clause.OnConflict{UpdateAll: true}).
		Create(&db.SystemConfig{Key: "calendar", Value: string(b)}).Error
}

func defaultCalendar() models.CalendarData {
	return models.CalendarData{
		ConsoleBlockedDates: make(map[string][]string),
		WorkingDays:         []int{1, 2, 3, 4, 5, 6, 7},
		Settings: models.CalendarSettings{
			TimeSlots: []string{
				"09:00", "10:00", "11:00", "12:00", "13:00", "14:00",
				"15:00", "16:00", "17:00", "18:00", "19:00", "20:00", "21:00",
			},
		},
	}
}

// ── Telegram Admins ───────────────────────────────────────────────────────────

func LoadTelegramAdmins() map[string]models.TelegramAdmin {
	var rows []db.TelegramAdmin
	db.DB.Find(&rows)
	result := make(map[string]models.TelegramAdmin)
	for _, r := range rows {
		result[r.TelegramID] = models.TelegramAdmin{
			TelegramID: r.TelegramID, Username: r.Username,
			FullName: r.FullName, AddedAt: r.AddedAt.Format(time.RFC3339),
			AddedBy: r.AddedBy,
		}
	}
	return result
}

func SaveTelegramAdmins(admins map[string]models.TelegramAdmin) error {
	return db.DB.Transaction(func(tx *gorm.DB) error {
		tx.Where("1 = 1").Delete(&db.TelegramAdmin{})
		for _, a := range admins {
			row := db.TelegramAdmin{
				TelegramID: a.TelegramID, Username: a.Username,
				FullName: a.FullName, AddedBy: a.AddedBy,
			}
			if t := parseTime(a.AddedAt); t != nil {
				row.AddedAt = *t
			}
			tx.Create(&row)
		}
		return nil
	})
}

func IsTelegramAdmin(telegramID string) bool {
	var row db.TelegramAdmin
	return db.DB.First(&row, "telegram_id = ?", telegramID).Error == nil
}

func AddTelegramAdmin(admin models.TelegramAdmin) error {
	row := db.TelegramAdmin{
		TelegramID: admin.TelegramID, Username: admin.Username,
		FullName: admin.FullName, AddedBy: admin.AddedBy,
	}
	if t := parseTime(admin.AddedAt); t != nil {
		row.AddedAt = *t
	}
	return db.DB.Clauses(clause.OnConflict{UpdateAll: true}).Create(&row).Error
}

func RemoveTelegramAdmin(telegramID string) error {
	return db.DB.Delete(&db.TelegramAdmin{}, "telegram_id = ?", telegramID).Error
}

// ── Init ──────────────────────────────────────────────────────────────────────

func Init() {
	dirs := []string{config.App.PassportDir, "static/img/console", "templates"}
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0755); err != nil {
			fmt.Printf("Ошибка создания директории %s: %v\n", d, err)
		}
	}

	// Create default web admin if none exists
	var count int64
	db.DB.Model(&db.Admin{}).Count(&count)
	if count == 0 {
		hash, _ := bcrypt.GenerateFromPassword([]byte("admin123"), bcrypt.DefaultCost)
		db.DB.Create(&db.Admin{
			Username: "admin",
			Password: string(hash),
			Role:     "superadmin",
		})
		fmt.Println("✅ Создан администратор по умолчанию: admin / admin123")
	}

	s := LoadSettings()
	if s.AdminChatID == "" {
		s = models.DefaultAdminSettings(config.App.AdminTelegramID)
	}
	_ = SaveSettings(s)

	r := LoadRatings()
	_ = SaveRatings(r)

	cal := LoadCalendar()
	if cal.Settings.TimeSlots == nil {
		cal = defaultCalendar()
		_ = SaveCalendar(cal)
	}

	fmt.Println("✅ Инициализация хранилища завершена")
}
