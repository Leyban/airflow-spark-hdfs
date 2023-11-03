package pgsoft

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type BetDetail struct {
	ID                           *int64   `db:"id" json:"id"`
	BetID                        *int64   `db:"bet_id" json:"betId"`
	ParentBetID                  *int64   `db:"parent_bet_id" json:"parentBetId"`
	PlayerName                   *string  `db:"player_name" json:"playerName"`
	Currency                     *string  `db:"currency" json:"currency"`
	GameID                       *int     `db:"game_id" json:"gameId"`
	Platform                     *int     `db:"platform" json:"platform"`
	BetType                      *int     `db:"bet_type" json:"betType"`
	TransactionType              *int     `db:"transaction_type" json:"transactionType"`
	BetAmount                    *float64 `db:"bet_amount" json:"betAmount"`
	WinAmount                    *float64 `db:"win_amount" json:"winAmount"`
	JackpotRtpContributionAmount *float64 `db:"jackpot_rtp_contribution_amount" json:"jackpotRtpContributionAmount"`
	JackpotWinAmount             *float64 `db:"jackpot_win_amount" json:"jackpotWinAmount"`
	BalanceBefore                *float64 `db:"balance_before" json:"balanceBefore"`
	BalanceAfter                 *float64 `db:"balance_after" json:"balanceAfter"`
	RowVersion                   *int64   `db:"row_version" json:"rowVersion"`
	BetTime                      *string  `db:"bet_time" json:"betTime"`
	CreateAt                     *string  `db:"create_at" json:"-"`
	UpdateAt                     *string  `db:"update_at" json:"-"`
}

type Response struct {
	Data []BetDetail `json:"data"`
}

func runQuery() []BetDetail {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=dummyDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	var records []BetDetail

	query := "SELECT * FROM pg_dummy"

	err = db.Select(&records, query)
	if err != nil {
		log.Fatal(err)
	}

	return records
}

func HandlePGSoft(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Pgsoft")
	result := runQuery()
	response := Response{
		Data: result,
	}

	jsonResult, err := json.Marshal(response)
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	w.Write(jsonResult)
}
