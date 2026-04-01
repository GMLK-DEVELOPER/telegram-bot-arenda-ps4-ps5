package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"ps4-rental/internal/db"
	"ps4-rental/internal/web/middleware"
)

// GetUserNotes returns all notes for a specific user
func GetUserNotes(c *gin.Context) {
	userID := c.Query("user_id")
	if userID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "user_id обязателен"})
		return
	}

	var notes []db.UserNote
	db.DB.Where("user_id = ?", userID).Order("created_at DESC").Find(&notes)

	c.JSON(http.StatusOK, gin.H{"notes": notes})
}

// AddUserNote creates a new note for a user
func AddUserNote(c *gin.Context) {
	var data struct {
		UserID string `json:"user_id"`
		Note   string `json:"note"`
	}
	if err := c.BindJSON(&data); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if data.UserID == "" || data.Note == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "user_id и note обязательны"})
		return
	}

	username, _ := c.Get("username")
	adminUser := ""
	if username != nil {
		adminUser = username.(string)
	}

	note := db.UserNote{
		UserID:    data.UserID,
		AdminUser: adminUser,
		Note:      data.Note,
	}

	if err := db.DB.Create(&note).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	middleware.LogAction(c, "added_user_note", data.UserID, data.Note)

	c.JSON(http.StatusOK, gin.H{"status": "success", "note": note})
}

// DeleteUserNote removes a note
func DeleteUserNote(c *gin.Context) {
	var data struct {
		NoteID uint `json:"note_id"`
	}
	if err := c.BindJSON(&data); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	var note db.UserNote
	if err := db.DB.First(&note, data.NoteID).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Заметка не найдена"})
		return
	}

	db.DB.Delete(&note)

	middleware.LogAction(c, "deleted_user_note", note.UserID, note.Note)

	c.JSON(http.StatusOK, gin.H{"status": "success"})
}
