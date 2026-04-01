package db

import (
	"fmt"
	"log"
	"os"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var newLogger = logger.New(
	log.New(os.Stdout, "\r\n", log.LstdFlags),
	logger.Config{
		SlowThreshold:             200 * time.Millisecond,
		LogLevel:                  logger.Warn,
		IgnoreRecordNotFoundError: true, // suppress "record not found" noise
		Colorful:                  false,
	},
)

var DB *gorm.DB

func Init(dsn string) error {
	var err error
	DB, err = gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: newLogger,
	})
	if err != nil {
		return fmt.Errorf("подключение к БД: %w", err)
	}

	sqlDB, err := DB.DB()
	if err != nil {
		return err
	}
	sqlDB.SetMaxOpenConns(25)
	sqlDB.SetMaxIdleConns(10)

	if err := DB.AutoMigrate(
		&Admin{},
		&TelegramAdmin{},
		&Console{},
		&User{},
		&Rental{},
		&RentalRequest{},
		&Discount{},
		&TempReservation{},
		&UserRating{},
		&RatingTransaction{},
		&SystemConfig{},
		&Waitlist{},
	); err != nil {
		return fmt.Errorf("автомиграция: %w", err)
	}

	fmt.Println("✅ База данных PostgreSQL подключена")
	return nil
}
