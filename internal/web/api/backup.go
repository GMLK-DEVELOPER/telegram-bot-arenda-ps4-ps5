package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"ps4-rental/internal/models"
	"ps4-rental/internal/storage"
)

type BackupData struct {
	Version        string                          `json:"version"`
	CreatedAt      string                          `json:"created_at"`
	Consoles       map[string]models.Console       `json:"consoles"`
	Users          map[string]models.User          `json:"users"`
	Rentals        map[string]models.Rental        `json:"rentals"`
	Requests       map[string]models.RentalRequest `json:"requests"`
	Discounts      map[string]models.Discount      `json:"discounts"`
	Settings       models.AdminSettings            `json:"settings"`
	TelegramAdmins map[string]models.TelegramAdmin `json:"telegram_admins"`
	Ratings        models.Ratings                  `json:"ratings"`
	Calendar       models.CalendarData             `json:"calendar"`
}

// DownloadBackup exports all data as a JSON file
func DownloadBackup(c *gin.Context) {
	backup := BackupData{
		Version:        "1.0",
		CreatedAt:      time.Now().Format("2006-01-02T15:04:05"),
		Consoles:       storage.LoadConsoles(),
		Users:          storage.LoadUsers(),
		Rentals:        storage.LoadRentals(),
		Requests:       storage.LoadRequests(),
		Discounts:      storage.LoadDiscounts(),
		Settings:       storage.LoadSettings(),
		TelegramAdmins: storage.LoadTelegramAdmins(),
		Ratings:        storage.LoadRatings(),
		Calendar:       storage.LoadCalendar(),
	}

	b, err := json.MarshalIndent(backup, "", "  ")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": "Ошибка создания бекапа"})
		return
	}

	filename := fmt.Sprintf("backup_%s.json", time.Now().Format("2006-01-02_15-04-05"))
	c.Header("Content-Disposition", "attachment; filename="+filename)
	c.Header("Content-Type", "application/json")
	c.Data(http.StatusOK, "application/json", b)
}

// RestoreBackup imports data from an uploaded JSON backup file
func RestoreBackup(c *gin.Context) {
	file, err := c.FormFile("backup")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "Файл не выбран"})
		return
	}

	f, err := file.Open()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"status": "error", "message": "Ошибка открытия файла"})
		return
	}
	defer f.Close()

	var backup BackupData
	if err := json.NewDecoder(f).Decode(&backup); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"status": "error", "message": "Неверный формат файла бекапа"})
		return
	}

	errs := []string{}

	if len(backup.Consoles) > 0 {
		if err := storage.SaveConsoles(backup.Consoles); err != nil {
			errs = append(errs, "consoles: "+err.Error())
		}
	}
	if len(backup.Users) > 0 {
		if err := storage.SaveUsers(backup.Users); err != nil {
			errs = append(errs, "users: "+err.Error())
		}
	}
	if len(backup.Rentals) > 0 {
		if err := storage.SaveRentals(backup.Rentals); err != nil {
			errs = append(errs, "rentals: "+err.Error())
		}
	}
	if len(backup.Requests) > 0 {
		if err := storage.SaveRequests(backup.Requests); err != nil {
			errs = append(errs, "requests: "+err.Error())
		}
	}
	if len(backup.Discounts) > 0 {
		if err := storage.SaveDiscounts(backup.Discounts); err != nil {
			errs = append(errs, "discounts: "+err.Error())
		}
	}
	if len(backup.TelegramAdmins) > 0 {
		if err := storage.SaveTelegramAdmins(backup.TelegramAdmins); err != nil {
			errs = append(errs, "telegram_admins: "+err.Error())
		}
	}
	if backup.Settings.AdminChatID != "" || backup.Settings.ShopTitle != "" {
		backup.Settings.SetupCompleted = true
		if err := storage.SaveSettings(backup.Settings); err != nil {
			errs = append(errs, "settings: "+err.Error())
		}
	}
	if err := storage.SaveRatings(backup.Ratings); err != nil {
		errs = append(errs, "ratings: "+err.Error())
	}
	if err := storage.SaveCalendar(backup.Calendar); err != nil {
		errs = append(errs, "calendar: "+err.Error())
	}

	if len(errs) > 0 {
		c.JSON(http.StatusOK, gin.H{
			"status":   "partial",
			"message":  "Восстановлено с ошибками: " + fmt.Sprintf("%v", errs),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "success", "message": "Данные успешно восстановлены"})
}
