package wagers

import (
	. "commission/random"
	"fmt"
	"log"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type WeworldData struct {
	Id             interface{} `db:"id"`
	BetId          interface{} `db:"bet_id"`
	OperatorId     interface{} `db:"operator_id"`
	PlayerId       interface{} `db:"player_id"`
	WeplayerId     interface{} `db:"weplayer_id"`
	BetDatetime    interface{} `db:"bet_datetime"`
	SettlementTime interface{} `db:"settlement_time"`
	BetStatus      interface{} `db:"bet_status"`
	BetCode        interface{} `db:"bet_code"`
	ValidBetAmount interface{} `db:"valid_bet_amount"`
	GameResult     interface{} `db:"game_result"`
	Device         interface{} `db:"device"`
	BetAmount      interface{} `db:"bet_amount"`
	WinlossAmount  interface{} `db:"winloss_amount"`
	Category       interface{} `db:"category"`
	GameType       interface{} `db:"game_type"`
	GameRoundId    interface{} `db:"game_round_id"`
	TableId        interface{} `db:"table_id"`
	Ip             interface{} `db:"ip"`
	Card_result    interface{} `db:"card_result"`
	CreateAt       interface{} `db:"create_at"`
	UpdateAt       interface{} `db:"update_at"`
}

func CopyWeworld(timeEnd time.Time) {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=collectorDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	insertSQL := `INSERT INTO weworld_wager (
        id,
        bet_id,
        operator_id,
        player_id,
        weplayer_id,
        bet_datetime,
        settlement_time,
        bet_status,
        bet_code,
        valid_bet_amount,
        game_result,
        device,
        bet_amount,
        winloss_amount,
        category,
        game_type,
        game_round_id,
        table_id,
        ip,
        card_result,
        create_at,
        update_at
    ) VALUES (
        :id,
        :bet_id,
        :operator_id,
        :player_id,
        :weplayer_id,
        :bet_datetime,
        :settlement_time,
        :bet_status,
        :bet_code,
        :valid_bet_amount,
        :game_result,
        :device,
        :bet_amount,
        :winloss_amount,
        :category,
        :game_type,
        :game_round_id,
        :table_id,
        :ip,
        :card_result,
        :create_at,
        :update_at
    )`

	selectSQL := `SELECT * FROM weworld_wager LIMIT 1000`

	var original []WeworldData

	err = db.Select(&original, selectSQL)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Found ", len(original), " Data")

	for i := 1; i <= 31; i++ {
		fmt.Println("Inserting Weworld for day", i)
		for _, data := range original {
			data.Id = RandInt64(9223372036854775806) // I don't know. Don't Ask
			data.BetId = RandInt64(9223372036854775806)
			data.BetDatetime = timeEnd.Add(time.Hour*time.Duration(RandInt(23))).AddDate(0, 0, 1-i).Format(time.RFC3339Nano)
			_, err := db.NamedExec(db.Rebind(insertSQL), data)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}
