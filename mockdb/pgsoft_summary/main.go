package main

import (
	"fmt"
	"log"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type BetDetail struct {
	ID                           int64   `db:"id"`
	BetID                        int64   `db:"bet_id"`
	ParentBetID                  int64   `db:"parent_bet_id"`
	PlayerName                   string  `db:"player_name"`
	Currency                     string  `db:"currency"`
	GameID                       int     `db:"game_id"`
	Platform                     int     `db:"platform"`
	BetType                      int     `db:"bet_type"`
	TransactionType              int     `db:"transaction_type"`
	BetAmount                    float64 `db:"bet_amount"`
	WinAmount                    float64 `db:"win_amount"`
	JackpotRtpContributionAmount float64 `db:"jackpot_rtp_contribution_amount"`
	JackpotWinAmount             float64 `db:"jackpot_win_amount"`
	BalanceBefore                float64 `db:"balance_before"`
	BalanceAfter                 float64 `db:"balance_after"`
	RowVersion                   int64   `db:"row_version"`
	BetTime                      string  `db:"bet_time"`
	CreateAt                     string  `db:"create_at"`
	UpdateAt                     string  `db:"update_at"`
}

func get_wagers() []BetDetail {

	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=collectorDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rawSql := `SELECT * FROM pgsoft_wager LIMIT 1000`

	var result []BetDetail

	err = db.Select(&result, rawSql)
	if err != nil {
		log.Fatal(err)
	}

	return result
}

func create_wagers(betDetails []BetDetail, dateStr string) {

	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=collectorDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rawSql := `INSERT INTO pgsoft_wager (
        bet_id,
        parent_bet_id,
        player_name,
        currency,
        game_id,
        platform,
        bet_type,
        transaction_type,
        bet_amount,
        win_amount,
        jackpot_rtp_contribution_amount,
        jackpot_win_amount,
        balance_before,
        balance_after,
        row_version,
        bet_time,
        create_at,
        update_at
    ) VALUES (
        :bet_id,
        :parent_bet_id,
        :player_name,
        :currency,
        :game_id,
        :platform,
        :bet_type,
        :transaction_type,
        :bet_amount,
        :win_amount,
        :jackpot_rtp_contribution_amount,
        :jackpot_win_amount,
        :balance_before,
        :balance_after,
        :row_version,
        :bet_time,
        :create_at,
        :update_at
    );
    `

	for _, bd := range betDetails {
		bd.BetTime = dateStr
		_, err = db.NamedExec(db.Rebind(rawSql), bd)
		if err != nil {
			log.Fatal(err)
		}
	}

	fmt.Println("Inserted 1000 wagers on ", dateStr)
}

func main() {
	dateStart := time.Date(2021, 1, 1, 12, 0, 0, 0, time.UTC)
	dateEnd := time.Date(2023, 12, 31, 23, 0, 0, 0, time.UTC)

	wagers := get_wagers()

	for dateIter := dateStart; dateIter.Before(dateEnd); dateIter = dateIter.Add(time.Hour * 24) {
		create_wagers(wagers, dateIter.Format(time.RFC3339Nano))
	}
}
