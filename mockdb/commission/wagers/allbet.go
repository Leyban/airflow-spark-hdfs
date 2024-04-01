package wagers

import (
	. "commission/random"
	"fmt"
	"log"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type AllbetData struct {
	Id                 interface{} `db:"id"`
	BetNum             interface{} `db:"bet_num"`
	GameRoundID        interface{} `db:"game_round_id"`
	Player             interface{} `db:"player"`
	Status             interface{} `db:"status"`
	Currency           interface{} `db:"currency"`
	BetAmount          interface{} `db:"bet_amount"`
	Deposit            interface{} `db:"deposit"`
	GameType           interface{} `db:"game_type"`
	BetType            interface{} `db:"bet_type"`
	Commission         interface{} `db:"commission"`
	ExchangeRate       interface{} `db:"exchange_rate"`
	GameResult         interface{} `db:"game_result"`
	GameResult2        interface{} `db:"game_result2"`
	WinOrLossAmount    interface{} `db:"win_or_loss_amount"`
	ValidAmount        interface{} `db:"valid_amount"`
	BetTime            interface{} `db:"bet_time"`
	TableName          interface{} `db:"table_name"`
	BetMethod          interface{} `db:"bet_method"`
	AppType            interface{} `db:"app_type"`
	GameRoundEndTime   interface{} `db:"game_round_end_time"`   //(GMT+8)
	GameRoundStartTime interface{} `db:"game_round_start_time"` //(GMT+8)
	Ip                 interface{} `db:"ip"`
	CreateAt           interface{} `db:"create_at"`
	UpdateAt           interface{} `db:"update_at"`
	PlayerName         interface{} `db:"login_name"`
}

func CopyAllBet(timeEnd time.Time) {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=collectorDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	insertSQL := `INSERT INTO allbet_wager (
        bet_num,
        game_round_id,
        player,
        status,
        currency,
        bet_amount,
        deposit,
        game_type,
        bet_type,
        commission,
        exchange_rate,
        game_result,
        game_result2,
        win_or_loss_amount,
        valid_amount,
        bet_time,
        table_name,
        bet_method,
        game_round_end_time,
        game_round_start_time,
        ip,
        create_at,
        update_at,
        login_name
    ) VALUES (
        :bet_num,
        :game_round_id,
        :player,
        :status,
        :currency,
        :bet_amount,
        :deposit,
        :game_type,
        :bet_type,
        :commission,
        :exchange_rate,
        :game_result,
        :game_result2,
        :win_or_loss_amount,
        :valid_amount,
        :bet_time,
        :table_name,
        :bet_method,
        :game_round_end_time,
        :game_round_start_time,
        :ip,
        :create_at,
        :update_at,
        :login_name
    )`

	selectSQL := `SELECT * FROM allbet_wager LIMIT 1000`

	var original []AllbetData

	err = db.Select(&original, selectSQL)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Found ", len(original), " Data")

	for i := 1; i <= 31; i++ {
		fmt.Println("Inserting Allbet for day", i)
		for _, data := range original {
			data.BetNum = RandInt64(9223372036854775806)
			data.BetTime = timeEnd.Add(time.Hour*time.Duration(RandInt(23))).AddDate(0, 0, 1-i).Format(time.RFC3339Nano)
			_, err := db.NamedExec(db.Rebind(insertSQL), data)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}
