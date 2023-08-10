package pgsoft

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type BetDetail struct {
	ID                           int64   `db:"id" json:"id"`
	BetID                        int64   `db:"bet_id" json:"betId"`
	ParentBetID                  int64   `db:"parent_bet_id" json:"parentBetId"`
	PlayerName                   string  `db:"player_name" json:"playerName"`
	Currency                     string  `db:"currency" json:"currency"`
	GameID                       int     `db:"game_id" json:"gameId"`
	Platform                     int     `db:"platform" json:"platform"`
	BetType                      int     `db:"bet_type" json:"betType"`
	TransactionType              int     `db:"transaction_type" json:"transactionType"`
	BetAmount                    float64 `db:"bet_amount" json:"betAmount"`
	WinAmount                    float64 `db:"win_amount" json:"winAmount"`
	JackpotRtpContributionAmount float64 `db:"jackpot_rtp_contribution_amount" json:"jackpotRtpContributionAmount"`
	JackpotWinAmount             float64 `db:"jackpot_win_amount" json:"jackpotWinAmount"`
	BalanceBefore                float64 `db:"balance_before" json:"balanceBefore"`
	BalanceAfter                 float64 `db:"balance_after" json:"balanceAfter"`
	RowVersion                   int64   `db:"row_version" json:"rowVersion"`
	BetTime                      string  `db:"bet_time" json:"betTime"`
	CreateAt                     string  `db:"create_at" json:"create_at"`
	UpdateAt                     string  `db:"update_at" json:"update_at"`
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
	result := runQuery()

	// Convert the result to JSON format
	jsonResult, err := json.Marshal(result)
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	// Set the content type header to indicate JSON response
	w.Header().Set("Content-Type", "application/json")

	// Write the JSON data to the response writer
	w.Write(jsonResult)
}
