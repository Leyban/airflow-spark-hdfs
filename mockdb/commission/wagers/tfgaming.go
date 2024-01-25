package wagers

import (
	. "commission/random"
	"fmt"
	"log"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type TfGamingBet struct {
	Id                    interface{} `db:"id"`
	Bet_history_id        interface{} `db:"bet_history_id"`
	Odds                  interface{} `db:"odds"`
	Malay_odds            interface{} `db:"malay_odds"`
	Euro_odds             interface{} `db:"euro_odds"`
	Member_odds           interface{} `db:"member_odds"`
	Member_odds_style     interface{} `db:"member_odds_style"`
	Game_type_id          interface{} `db:"game_type_id"`
	Game_type_name        interface{} `db:"game_type_name"`
	Game_market_name      interface{} `db:"game_market_name"`
	Market_option         interface{} `db:"market_option"`
	Map_num               interface{} `db:"map_num"`
	Bet_type_name         interface{} `db:"bet_type_name"`
	Competition_name      interface{} `db:"competition_name"`
	Event_id              interface{} `db:"event_id"`
	Event_name            interface{} `db:"event_name"`
	Event_datetime        interface{} `db:"event_datetime"`
	Date_created          interface{} `db:"date_created"`
	Settlement_datetime   interface{} `db:"settlement_datetime"`
	Modified_datetime     interface{} `db:"modified_datetime"`
	Bet_selection         interface{} `db:"bet_selection"`
	Currency              interface{} `db:"currency"`
	Amount                interface{} `db:"amount"`
	Settlement_status     interface{} `db:"settlement_status"`
	Is_unsettled          interface{} `db:"is_unsettled"`
	Result_status         interface{} `db:"result_status"`
	Result                interface{} `db:"result"`
	Earnings              interface{} `db:"earnings"`
	Handicap              interface{} `db:"handicap"`
	Member_code           interface{} `db:"member_code"`
	Request_source        interface{} `db:"request_source"`
	Is_combo              interface{} `db:"is_combo"`
	Ticket_type           interface{} `db:"ticket_type"`
	Parent_bet_history_id interface{} `db:"parent_bet_history_id"`
	Create_at             interface{} `db:"create_at"`
	Update_at             interface{} `db:"update_at"`
}

func CopyTfGaming(timeEnd time.Time) {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=collectorDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	insertSQL := `INSERT INTO tfgaming_wager (
        bet_history_id,
        odds,
        malay_odds,
        euro_odds,
        member_odds,
        member_odds_style,
        game_type_id,
        game_type_name,
        game_market_name,
        market_option,
        map_num,
        bet_type_name,
        competition_name,
        event_id,
        event_name,
        event_datetime,
        date_created,
        settlement_datetime,
        modified_datetime,
        bet_selection,
        currency,
        amount,
        settlement_status,
        is_unsettled,
        result_status,
        result,
        earnings,
        handicap,
        member_code,
        request_source,
        is_combo,
        ticket_type,
        parent_bet_history_id,
        create_at,
        update_at
    ) VALUES (
        :bet_history_id,
        :odds,
        :malay_odds,
        :euro_odds,
        :member_odds,
        :member_odds_style,
        :game_type_id,
        :game_type_name,
        :game_market_name,
        :market_option,
        :map_num,
        :bet_type_name,
        :competition_name,
        :event_id,
        :event_name,
        :event_datetime,
        :date_created,
        :settlement_datetime,
        :modified_datetime,
        :bet_selection,
        :currency,
        :amount,
        :settlement_status,
        :is_unsettled,
        :result_status,
        :result,
        :earnings,
        :handicap,
        :member_code,
        :request_source,
        :is_combo,
        :ticket_type,
        :parent_bet_history_id,
        :create_at,
        :update_at
    )`

	selectSQL := `SELECT * FROM tfgaming_wager LIMIT 1000`

	var original []TfGamingBet

	err = db.Select(&original, selectSQL)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Found ", len(original), " Data")

	for i := 1; i <= 31; i++ {
		fmt.Println("Inserting TFGaming for day", i)
		for _, data := range original {
			data.Bet_history_id = RandInt64(9223372036854775806)
			data.Date_created = timeEnd.Add(time.Hour*time.Duration(RandInt(23))).AddDate(0, 0, 1-i).Format(time.RFC3339Nano)
			_, err := db.NamedExec(db.Rebind(insertSQL), data)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}
