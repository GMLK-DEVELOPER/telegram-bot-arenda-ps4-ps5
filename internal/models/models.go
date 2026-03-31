package models

import "time"

// Console represents a PlayStation console available for rent
type Console struct {
	ID             string   `json:"id"`
	Name           string   `json:"name"`
	Model          string   `json:"model"`
	Games          []string `json:"games"`
	RentalPrice    float64  `json:"rental_price"`
	SalePrice      float64  `json:"sale_price"`
	Status         string   `json:"status"` // available, rented
	PhotoPath      string   `json:"photo_path,omitempty"`
	ShowPhotoInBot bool     `json:"show_photo_in_bot"`
	CreatedAt      string   `json:"created_at"`
	UpdatedAt      string   `json:"updated_at,omitempty"`
}

// User represents a registered Telegram user
type User struct {
	ID               string `json:"id"`
	Username         string `json:"username,omitempty"`
	FirstName        string `json:"first_name,omitempty"`
	LastName         string `json:"last_name,omitempty"`
	FullName         string `json:"full_name,omitempty"`
	PhoneNumber      string `json:"phone_number,omitempty"`
	IsBanned         bool   `json:"is_banned"`
	TotalSpent       float64 `json:"total_spent"`
	JoinedAt         string `json:"joined_at"`
	RegistrationStep string `json:"registration_step,omitempty"` // phone, full_name, completed
	VerificationStep string `json:"verification_step,omitempty"` // passport_front, passport_back, selfie_with_passport, location_request, completed
	PendingRentalID  string `json:"pending_rental_id,omitempty"`
	PassportFrontFile string `json:"passport_front_file,omitempty"`
	PassportBackFile  string `json:"passport_back_file,omitempty"`
	SelfieFile        string `json:"selfie_file,omitempty"`
	BotBlocked       bool   `json:"bot_blocked,omitempty"`
	BotBlockedAt     string `json:"bot_blocked_at,omitempty"`
	LoyaltyBonus     int    `json:"loyalty_bonus,omitempty"`
	PromotionParticipation bool `json:"promotion_participation,omitempty"`
}

// Rental represents an active or completed rental
type Rental struct {
	ID                string   `json:"id"`
	UserID            string   `json:"user_id"`
	ConsoleID         string   `json:"console_id"`
	StartTime         string   `json:"start_time"`
	ExpectedEndTime   string   `json:"expected_end_time,omitempty"`
	EndTime           string   `json:"end_time,omitempty"`
	SelectedHours     int      `json:"selected_hours,omitempty"`
	DurationHours     int      `json:"duration_hours,omitempty"`
	ExpectedCost      float64  `json:"expected_cost,omitempty"`
	TotalCost         float64  `json:"total_cost"`
	Status            string   `json:"status"` // active, completed, pending_approval
	EndedBy           string   `json:"ended_by,omitempty"`
	CreatedAt         string   `json:"created_at,omitempty"`
	Location          *Location `json:"location,omitempty"`
	Reminder2hSent    bool     `json:"reminder_2h_sent,omitempty"`
	Reminder1hSent    bool     `json:"reminder_1h_sent,omitempty"`
	Reminder30mSent   bool     `json:"reminder_30m_sent,omitempty"`
	Reminder10mSent   bool     `json:"reminder_10m_sent,omitempty"`
}

type Location struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

// RentalRequest represents a pending rental request awaiting admin approval
type RentalRequest struct {
	ID              string   `json:"id"`
	UserID          string   `json:"user_id"`
	ConsoleID       string   `json:"console_id"`
	SelectedHours   int      `json:"selected_hours,omitempty"`
	DurationHours   int      `json:"duration_hours,omitempty"`
	ExpectedCost    float64  `json:"expected_cost,omitempty"`
	TotalCost       float64  `json:"total_cost,omitempty"`
	RequestTime     string   `json:"request_time,omitempty"`
	CreatedAt       string   `json:"created_at,omitempty"`
	Status          string   `json:"status"` // pending, approved, rejected, completed, pending_approval
	StartTime       string   `json:"start_time,omitempty"`
	EstimatedEndTime string  `json:"estimated_end_time,omitempty"`
	RentalID        string   `json:"rental_id,omitempty"`
}

// Admin represents a web admin user
type Admin struct {
	Username  string `json:"username"`
	Password  string `json:"password"`
	Role      string `json:"role"`
	CreatedAt string `json:"created_at"`
}

// TelegramAdmin represents a Telegram user with admin privileges
type TelegramAdmin struct {
	TelegramID string `json:"telegram_id"`
	Username   string `json:"username,omitempty"`
	FullName   string `json:"full_name,omitempty"`
	AddedAt    string `json:"added_at"`
	AddedBy    string `json:"added_by,omitempty"`
}

// AdminSettings represents system configuration
type AdminSettings struct {
	AdminChatID                  string            `json:"admin_chat_id"`
	MaxRentalHours               int               `json:"max_rental_hours"`
	ReminderHours                int               `json:"reminder_hours"`
	RequireApproval              bool              `json:"require_approval"`
	NotificationsEnabled         bool              `json:"notifications_enabled"`
	PushNotificationsEnabled     bool              `json:"push_notifications_enabled"`
	CriticalNotificationsEnabled bool              `json:"critical_notifications_enabled"`
	NotificationFrequency        int               `json:"notification_frequency"`
	ShowConsolePhotos            bool              `json:"show_console_photos"`
	WorkingDays                  []int             `json:"working_days"`
	WorkingHours                 map[string]string `json:"working_hours,omitempty"`
	// Shop location — sent to user when rental is approved
	ShopLatitude  float64 `json:"shop_latitude,omitempty"`
	ShopLongitude float64 `json:"shop_longitude,omitempty"`
	ShopAddress   string  `json:"shop_address,omitempty"`
	ShopTitle     string  `json:"shop_title,omitempty"`
}

func DefaultAdminSettings(adminID string) AdminSettings {
	return AdminSettings{
		AdminChatID:                  adminID,
		MaxRentalHours:               24,
		ReminderHours:                23,
		RequireApproval:              true,
		NotificationsEnabled:         true,
		PushNotificationsEnabled:     true,
		CriticalNotificationsEnabled: true,
		NotificationFrequency:        5,
		ShowConsolePhotos:            true,
		WorkingDays:                  []int{1, 2, 3, 4, 5, 6, 7},
	}
}

// Discount represents a time-limited discount for a console
type Discount struct {
	ID        string  `json:"id"`
	ConsoleID string  `json:"console_id"`
	Type      string  `json:"type"` // percentage, fixed
	Value     float64 `json:"value"`
	Amount    float64 `json:"amount,omitempty"`
	StartDate string  `json:"start_date"`
	EndDate   string  `json:"end_date"`
	Active    bool    `json:"active"`
	MinHours  int     `json:"min_hours,omitempty"`
}

// TempReservation represents a temporary hold on a console
type TempReservation struct {
	ID        string `json:"id"`
	UserID    string `json:"user_id"`
	ConsoleID string `json:"console_id"`
	CreatedAt string `json:"created_at"`
	ExpiresAt string `json:"expires_at"`
	Status    string `json:"status"` // active
}

// Rating represents a user's rating data
type Ratings struct {
	Settings     RatingSettings             `json:"settings"`
	Transactions map[string][]RatingTransaction `json:"transactions"`
	UserRatings  map[string]UserRating      `json:"user_ratings,omitempty"`
}

type RatingSettings struct {
	DisciplineWeight  float64                       `json:"discipline_weight"`
	LoyaltyWeight     float64                       `json:"loyalty_weight"`
	StatusThresholds  map[string]int                `json:"status_thresholds"`
	TransactionsWindow int                          `json:"transactions_window"`
	DisciplineRules   map[string]map[string]int     `json:"discipline_rules"`
	LoyaltyRules      map[string]interface{}        `json:"loyalty_rules"`
}

type RatingTransaction struct {
	Type          string    `json:"type"`
	Points        int       `json:"points"`
	Comment       string    `json:"comment"`
	Date          string    `json:"date"`
	AutoGenerated bool      `json:"auto_generated"`
}

type UserRating struct {
	UserID      string  `json:"user_id"`
	FinalScore  int     `json:"final_score"`
	Discipline  int     `json:"discipline"`
	Loyalty     int     `json:"loyalty"`
	Status      string  `json:"status"` // premium, regular, risk
	StatusName  string  `json:"status_name"`
	CalculatedAt string `json:"calculated_at"`
}

// CalendarData represents calendar settings and blocked dates
type CalendarData struct {
	Settings            CalendarSettings          `json:"settings"`
	BookingRules        map[string]interface{}    `json:"booking_rules,omitempty"`
	SystemBlockedDates  []string                  `json:"system_blocked_dates"`
	ConsoleBlockedDates map[string][]string       `json:"console_blocked_dates"`
	Holidays            []Holiday                 `json:"holidays"`
	WorkingDays         []int                     `json:"working_days"`
	Reservations        map[string][]SlotReservation `json:"reservations,omitempty"`
}

type CalendarSettings struct {
	TimeSlots []string `json:"time_slots"`
}

type Holiday struct {
	Date    string `json:"date"`
	Name    string `json:"name"`
	Working bool   `json:"working"`
}

type SlotReservation struct {
	TimeSlot string `json:"time_slot"`
	Status   string `json:"status"`
	UserID   string `json:"user_id,omitempty"`
}

// BlockedDates for legacy support
type BlockedDates struct {
	SystemBlockedDates  []string            `json:"system_blocked_dates"`
	ConsoleBlockedDates map[string][]string `json:"console_blocked_dates"`
}

// UserRatingResult is returned from rating calculation
type UserRatingResult struct {
	UserID       string
	FinalScore   int
	Discipline   int
	Loyalty      int
	Status       string
	StatusName   string
	CalculatedAt time.Time
}

func DefaultRatings() Ratings {
	return Ratings{
		Settings: RatingSettings{
			DisciplineWeight:   0.6,
			LoyaltyWeight:      0.4,
			TransactionsWindow: 5,
			StatusThresholds: map[string]int{
				"premium": 80,
				"regular": 50,
				"risk":    0,
			},
			DisciplineRules: map[string]map[string]int{
				"return_timing": {
					"early":   10,
					"on_time": 0,
					"late_1h": -10,
					"late_3h": -20,
					"late":    -30,
				},
				"item_condition": {
					"perfect":       10,
					"minor_defects": -15,
					"major_defects": -30,
				},
				"rule_compliance": {
					"no_violations":   0,
					"minor_violation": -10,
					"major_violation": -25,
				},
			},
		},
		Transactions: make(map[string][]RatingTransaction),
		UserRatings:  make(map[string]UserRating),
	}
}
