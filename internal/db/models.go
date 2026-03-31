package db

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"
)

// ── Custom JSON Types ─────────────────────────────────────────────────────────

type StringSlice []string

func (s StringSlice) Value() (driver.Value, error) {
	if s == nil {
		return "[]", nil
	}
	b, err := json.Marshal(s)
	return string(b), err
}
func (s *StringSlice) Scan(src interface{}) error {
	switch v := src.(type) {
	case []byte:
		return json.Unmarshal(v, s)
	case string:
		return json.Unmarshal([]byte(v), s)
	case nil:
		*s = StringSlice{}
		return nil
	}
	return fmt.Errorf("cannot scan %T into StringSlice", src)
}

type IntSlice []int

func (s IntSlice) Value() (driver.Value, error) {
	if s == nil {
		return "[1,2,3,4,5,6,7]", nil
	}
	b, _ := json.Marshal(s)
	return string(b), nil
}
func (s *IntSlice) Scan(src interface{}) error {
	switch v := src.(type) {
	case []byte:
		return json.Unmarshal(v, s)
	case string:
		return json.Unmarshal([]byte(v), s)
	case nil:
		*s = IntSlice{1, 2, 3, 4, 5, 6, 7}
		return nil
	}
	return fmt.Errorf("cannot scan %T into IntSlice", src)
}

// ── GORM Models ───────────────────────────────────────────────────────────────

type Admin struct {
	ID        uint      `gorm:"primaryKey;autoIncrement"`
	Username  string    `gorm:"uniqueIndex;not null;size:255"`
	Password  string    `gorm:"not null;size:255"`
	Role      string    `gorm:"size:50;default:'admin'"`
	CreatedAt time.Time `gorm:"autoCreateTime"`
}

func (Admin) TableName() string { return "admins" }

type TelegramAdmin struct {
	TelegramID string    `gorm:"primaryKey;size:255"`
	Username   string    `gorm:"size:255"`
	FullName   string    `gorm:"size:255"`
	AddedAt    time.Time `gorm:"autoCreateTime"`
	AddedBy    string    `gorm:"size:255"`
}

func (TelegramAdmin) TableName() string { return "telegram_admins" }

type Console struct {
	ID             string      `gorm:"primaryKey;size:255"`
	Name           string      `gorm:"not null;size:255"`
	Model          string      `gorm:"size:255"`
	Games          StringSlice `gorm:"type:text"`
	RentalPrice    float64     `gorm:"default:0"`
	SalePrice      float64     `gorm:"default:0"`
	Status         string      `gorm:"size:50;default:'available'"`
	PhotoPath      string      `gorm:"size:500"`
	ShowPhotoInBot bool        `gorm:"default:true"`
	CreatedAt      time.Time   `gorm:"autoCreateTime"`
	UpdatedAt      time.Time   `gorm:"autoUpdateTime"`
}

func (Console) TableName() string { return "consoles" }

type User struct {
	ID                     string     `gorm:"primaryKey;size:255"`
	Username               string     `gorm:"size:255"`
	FirstName              string     `gorm:"size:255"`
	LastName               string     `gorm:"size:255"`
	FullName               string     `gorm:"size:255"`
	PhoneNumber            string     `gorm:"size:50"`
	IsBanned               bool       `gorm:"default:false"`
	TotalSpent             float64    `gorm:"default:0"`
	JoinedAt               time.Time  `gorm:"autoCreateTime"`
	RegistrationStep       string     `gorm:"size:50"`
	VerificationStep       string     `gorm:"size:50"`
	PendingRentalID        string     `gorm:"size:255"`
	PassportFrontFile      string     `gorm:"size:500"`
	PassportBackFile       string     `gorm:"size:500"`
	SelfieFile             string     `gorm:"size:500"`
	BotBlocked             bool       `gorm:"default:false"`
	BotBlockedAt           *time.Time
	LoyaltyBonus           int        `gorm:"default:0"`
	PromotionParticipation bool       `gorm:"default:false"`
}

func (User) TableName() string { return "users" }

type Rental struct {
	ID              string     `gorm:"primaryKey;size:255"`
	UserID          string     `gorm:"index;not null;size:255"`
	ConsoleID       string     `gorm:"index;not null;size:255"`
	StartTime       *time.Time
	ExpectedEndTime *time.Time
	EndTime         *time.Time
	SelectedHours   int        `gorm:"default:0"`
	DurationHours   int        `gorm:"default:0"`
	ExpectedCost    float64    `gorm:"default:0"`
	TotalCost       float64    `gorm:"default:0"`
	Status          string     `gorm:"size:50;default:'active'"`
	EndedBy         string     `gorm:"size:50"`
	CreatedAt       time.Time  `gorm:"autoCreateTime"`
	LocationJSON    string     `gorm:"column:location;type:text"`
	Reminder2hSent  bool       `gorm:"default:false"`
	Reminder1hSent  bool       `gorm:"default:false"`
	Reminder30mSent bool       `gorm:"default:false"`
	Reminder10mSent bool       `gorm:"default:false"`
}

func (Rental) TableName() string { return "rentals" }

type RentalRequest struct {
	ID               string     `gorm:"primaryKey;size:255"`
	UserID           string     `gorm:"index;not null;size:255"`
	ConsoleID        string     `gorm:"index;not null;size:255"`
	SelectedHours    int        `gorm:"default:0"`
	DurationHours    int        `gorm:"default:0"`
	ExpectedCost     float64    `gorm:"default:0"`
	TotalCost        float64    `gorm:"default:0"`
	RequestTime      *time.Time
	CreatedAt        time.Time  `gorm:"autoCreateTime"`
	Status           string     `gorm:"size:50;default:'pending'"`
	StartTime        *time.Time
	EstimatedEndTime *time.Time
	RentalID         string     `gorm:"size:255"`
}

func (RentalRequest) TableName() string { return "rental_requests" }

type Discount struct {
	ID        string     `gorm:"primaryKey;size:255"`
	ConsoleID string     `gorm:"index;not null;size:255"`
	Type      string     `gorm:"size:50"`
	Value     float64    `gorm:"default:0"`
	Amount    float64    `gorm:"default:0"`
	StartDate *time.Time
	EndDate   *time.Time
	Active    bool       `gorm:"default:true"`
	MinHours  int        `gorm:"default:0"`
}

func (Discount) TableName() string { return "discounts" }

type TempReservation struct {
	ID        string     `gorm:"primaryKey;size:255"`
	UserID    string     `gorm:"index;not null;size:255"`
	ConsoleID string     `gorm:"index;not null;size:255"`
	CreatedAt time.Time  `gorm:"autoCreateTime"`
	ExpiresAt *time.Time
	Status    string     `gorm:"size:50;default:'active'"`
}

func (TempReservation) TableName() string { return "temp_reservations" }

type UserRating struct {
	UserID       string    `gorm:"primaryKey;size:255"`
	FinalScore   int       `gorm:"default:0"`
	Discipline   int       `gorm:"default:0"`
	Loyalty      int       `gorm:"default:0"`
	Status       string    `gorm:"size:50;default:'regular'"`
	StatusName   string    `gorm:"size:100"`
	CalculatedAt time.Time `gorm:"autoUpdateTime"`
}

func (UserRating) TableName() string { return "user_ratings" }

type RatingTransaction struct {
	ID            uint      `gorm:"primaryKey;autoIncrement"`
	UserID        string    `gorm:"index;not null;size:255"`
	Type          string    `gorm:"size:50"`
	Points        int       `gorm:"default:0"`
	Comment       string    `gorm:"type:text"`
	Date          time.Time `gorm:"autoCreateTime"`
	AutoGenerated bool      `gorm:"default:false"`
}

func (RatingTransaction) TableName() string { return "rating_transactions" }

// SystemConfig stores key-value JSON blobs (ratings_settings, calendar, admin_settings)
type SystemConfig struct {
	Key   string `gorm:"primaryKey;size:100"`
	Value string `gorm:"type:text"`
}

func (SystemConfig) TableName() string { return "system_config" }
