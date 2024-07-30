package driver

import (
	"database/sql"
	"fmt"
	"log"
	"messaggio_demo/config"
)

type DB interface {
	Get(cnf *config.Config) (*sql.DB, error)
}

func GetDB(config *config.Config) (*sql.DB, error) {
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
			name TEXT NOT NULL,
			text TEXT NOT NULL,
			processed BOOLEAN DEFAULT false
			
		)
	`)

	if err != nil {
		return nil, fmt.Errorf("error creating users table")
	}

	fmt.Println("Users table created")
	log.Println("Successfully connected to the database")

	return db, nil
}
