package wagers

import (
	. "commission/random"
	"fmt"
	"log"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type GenesisData struct {
	Id                     interface{} `db:"id"`
	BetHistoryId           interface{} `db:"bet_history_id"`
	GameType               interface{} `db:"game_type"`
	BetMap                 interface{} `db:"bet_map"`
	JudgeResult            interface{} `db:"judge_result"`
	RoundNo                interface{} `db:"round_no"`
	Bet                    interface{} `db:"bet"`
	Payout                 interface{} `db:"payout"`
	BankerCards            interface{} `db:"banker_cards"`
	PlayerCards            interface{} `db:"player_cards"`
	AllDices               interface{} `db:"all_dices"`
	DragonCard             interface{} `db:"dragon_card"`
	TigerCard              interface{} `db:"tiger_card"`
	Number                 interface{} `db:"number"`
	CreateTime             interface{} `db:"create_time"`
	PayoutTime             interface{} `db:"payout_time"`
	ValidBet               interface{} `db:"valid_bet"`
	UserName               interface{} `db:"user_name"`
	UserId                 interface{} `db:"user_id"`
	GameName               interface{} `db:"game_name"`
	Platform               interface{} `db:"platform"`
	PlayerResult           interface{} `db:"player_result"`
	BankerResult           interface{} `db:"banker_result"`
	NiuNiuWithHoldingTotal interface{} `db:"niu_niu_with_holding_total"`
	CreateAt               interface{} `db:"create_at"`
	UpdateAt               interface{} `db:"update_at"`
}

func CopyGenesis(timeEnd time.Time) {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=collectorDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	insertSQL := `INSERT INTO genesis_wager (
        bet_history_id,
        game_type,
        bet_map,
        judge_result,
        round_no,
        bet,
        payout,
        banker_cards,
        player_cards,
        all_dices,
        dragon_card,
        tiger_card,
        number,
        create_time,
        payout_time,
        valid_bet,
        user_name,
        user_id,
        game_name,
        platform,
        player_result,
        banker_result,
        niu_niu_with_holding_total,
        create_at,
        update_at
    ) VALUES (
        :bet_history_id,
        :game_type,
        :bet_map,
        :judge_result,
        :round_no,
        :bet,
        :payout,
        :banker_cards,
        :player_cards,
        :all_dices,
        :dragon_card,
        :tiger_card,
        :number,
        :create_time,
        :payout_time,
        :valid_bet,
        :user_name,
        :user_id,
        :game_name,
        :platform,
        :player_result,
        :banker_result,
        :niu_niu_with_holding_total,
        :create_at,
        :update_at
    )`

	selectSQL := `SELECT * FROM genesis_wager LIMIT 1000`

	var original []GenesisData

	err = db.Select(&original, selectSQL)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Found ", len(original), " Data")

	for i := 1; i <= 31; i++ {
		fmt.Println("Inserting Genesis for day", i)
		for _, data := range original {
			data.BetHistoryId = RandInt64(9223372036854775806)
			data.CreateTime = timeEnd.Add(time.Hour*time.Duration(RandInt(23))).AddDate(0, 0, 1-i).Format(time.RFC3339Nano)
			_, err := db.NamedExec(db.Rebind(insertSQL), data)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}
