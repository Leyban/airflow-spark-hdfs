package wagers

import (
	. "commission/random"
	"fmt"
	"log"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type EvolutionData struct {
	Id            interface{} `db:"id"`
	TransactionId interface{} `db:"transaction_id"`
	Stake         interface{} `db:"stake"`
	PlacedOn      interface{} `db:"placed_on"`
	Payout        interface{} `db:"payout"`
	Description   interface{} `db:"description"`
	Status        interface{} `db:"status"`
	Code          interface{} `db:"code"`
	Channel       interface{} `db:"channel"`
	Currency      interface{} `db:"currency"`
	Device        interface{} `db:"device"`
	Os            interface{} `db:"os"`
	PlayerId      interface{} `db:"player_id"`
	GameType      interface{} `db:"game_type"`
	SettledAt     interface{} `db:"settled_at"`
	StartedAt     interface{} `db:"started_at"`
	CreateAt      interface{} `db:"create_at"`
	UpdateAt      interface{} `db:"update_at"`
	GameId        interface{} `db:"game_id"`
}

func CopyEvolution(timeEnd time.Time) {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=collectorDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	insertSQL := `INSERT INTO evolution_wager (
        transaction_id,
        stake,
        placed_on,
        payout,
        description,
        status,
        code,
        channel,
        currency,
        device,
        os,
        player_id,
        game_type,
        settled_at,
        started_at,
        create_at,
        update_at,
        game_id
    ) VALUES (
        :transaction_id,
        :stake,
        :placed_on,
        :payout,
        :description,
        :status,
        :code,
        :channel,
        :currency,
        :device,
        :os,
        :player_id,
        :game_type,
        :settled_at,
        :started_at,
        :create_at,
        :update_at,
        :game_id
    )`

	selectSQL := `SELECT * FROM evolution_wager LIMIT 1000`

	var original []EvolutionData

	err = db.Select(&original, selectSQL)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Found ", len(original), " Data")

	for i := 1; i <= 31; i++ {
		fmt.Println("Inserting Evolution for day", i)
		for _, data := range original {
			data.TransactionId = RandInt64(9223372036854775806)
			data.PlacedOn = timeEnd.Add(time.Hour*time.Duration(RandInt(23))).AddDate(0, 0, 1-i).Format(time.RFC3339Nano)
			_, err := db.NamedExec(db.Rebind(insertSQL), data)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}
