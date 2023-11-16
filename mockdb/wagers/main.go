package main

import (
	"fmt"
	"log"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

var wagers = map[string]string{
	"allbet_wager":         "bet_time",
	"asiagaming_wager":     "bet_time",
	"sagaming_wager":       "bet_time",
	"simpleplay_wager":     "bet_time",
	"pgsoft_wager":         "bet_time",
	"ebet_wager":           "create_time",
	"bti_wager":            "creation_date",
	"sabacv_wager":         "transaction_time",
	"saba_wager":           "transaction_time",
	"saba_number":          "transaction_time",
	"saba_virtual":         "transaction_time",
	"tfgaming_wager":       "date_created",
	"evolution_wager":      "placed_on",
	"genesis_wager":        "create_time",
	"weworld_wager":        "bet_datetime",
	"digitain_order_wager": "fill_date",
}

func main() {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=collectorDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	newTime := time.Date(2023, 11, 13, 20, 34, 58, 54612, time.UTC)

	for table, col := range wagers {

		rawSql := ` UPDATE ` + table + ` SET ` + col + ` = ? `

		_, err := db.Exec(db.Rebind(rawSql), newTime.Format(time.RFC3339Nano))
		if err != nil {
			fmt.Println(err)
		}
	}

}
