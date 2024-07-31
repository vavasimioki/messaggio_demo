package driver

import (
	"database/sql"
	"fmt"
	"log"
	"messaggio_demo/config"

	_ "github.com/lib/pq" // Убедитесь, что драйвер PostgreSQL подключен
)

func GetDB(config *config.Config) (*sql.DB, error) {
	fmt.Printf("user=%s dbname=%s sslmode=%s password=%s host=%s",
		config.User, config.DBname, config.Sslmode, config.Password, config.Host)
	db, err := sql.Open("postgres", fmt.Sprintf("user=%s dbname=%s sslmode=%s password=%s host=%s",
		config.User, config.DBname, config.Sslmode, config.Password, config.Host))
	if err != nil {
		return nil, fmt.Errorf("error opening database connection: %v", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("error pinging database: %v", err)
	}

	_, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            text TEXT NOT NULL
        )
    `)
	if err != nil {
		return nil, fmt.Errorf("error creating users table: %v", err)
	}

	fmt.Println("Users table created")
	log.Println("Successfully connected to the database")

	return db, nil
}
