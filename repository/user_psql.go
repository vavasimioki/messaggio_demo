package repository

import (
	"database/sql"
	"log"
	"messaggio_demo/model"
)

func logFatal(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func InsertMsg(db *sql.DB, msg model.Message) model.Message {
	stmt := "INSERT INTO users (text) VALUES($1) RETURNING id;"
	err := db.QueryRow(stmt, msg.Text).Scan(&msg.ID)

	logFatal(err)

	msg.Text = ""

	return msg
}
