package wagers

import (
	. "commission/random"
	"fmt"
	"log"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type SagamingData struct {
	Id            interface{} `db:"id"`
	BetId         interface{} `db:"bet_id"`
	BetTime       interface{} `db:"bet_time"`
	PayoutTime    interface{} `db:"payout_time"`
	Username      interface{} `db:"username"`
	HostId        interface{} `db:"host_id"`
	Detail        interface{} `db:"detail"`
	GameId        interface{} `db:"game_id"`
	Round         interface{} `db:"round"`
	Set           interface{} `db:"set"`
	BetAmount     interface{} `db:"bet_amount"`
	Rolling       interface{} `db:"rolling"`
	ResultAmount  interface{} `db:"result_amount"`
	Balance       interface{} `db:"balance"`
	GameType      interface{} `db:"game_type"`
	BetType       interface{} `db:"bet_type"`
	BetSource     interface{} `db:"bet_source"`
	TransactionId interface{} `db:"transaction_id"`
	State         interface{} `db:"state"`
	CreateAt      interface{} `db:"create_at"`
	UpdateAt      interface{} `db:"update_at"`
}

func CopySagaming(timeEnd time.Time) {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=collectorDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	insertSQL := `INSERT INTO sagaming_wager (
        bet_id,
        bet_time,
        payout_time,
        username,
        host_id,
        detail,
        game_id,
        round,
        set,
        bet_amount,
        rolling,
        result_amount,
        balance,
        game_type,
        bet_type,
        bet_source,
        transaction_id,
        state,
        create_at,
        update_at
    ) VALUES (
        :bet_id,
        :bet_time,
        :payout_time,
        :username,
        :host_id,
        :detail,
        :game_id,
        :round,
        :set,
        :bet_amount,
        :rolling,
        :result_amount,
        :balance,
        :game_type,
        :bet_type,
        :bet_source,
        :transaction_id,
        :state,
        :create_at,
        :update_at
    )`

	selectSQL := `SELECT * FROM sagaming_wager LIMIT 1000`

	var original []SagamingData

	err = db.Select(&original, selectSQL)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Found ", len(original), " Data")

	for i := 1; i <= 31; i++ {
		fmt.Println("Inserting Sagaming for day", i)
		for _, data := range original {
			data.BetId = RandInt64(9223372036854775806)
			data.BetTime = timeEnd.Add(time.Hour*time.Duration(RandInt(23))).AddDate(0, 0, 1-i).Format(time.RFC3339Nano)
			_, err := db.NamedExec(db.Rebind(insertSQL), data)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}
