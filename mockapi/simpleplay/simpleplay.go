package simpleplay

import (
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/julienschmidt/httprouter"
	_ "github.com/lib/pq"
)

type HistoryResponse struct {
	ErrorMsgID    string        `xml:"ErrorMsgId"` // ErrorMsgId
	ErrorMsg      string        `xml:"ErrorMsg"`   // ErrorMsg
	BetDetailList BetDetailList `xml:"BetDetailList"`
}

type BetDetailList struct {
	BetDetail []BetDetail `xml:"BetDetail"`
}

type BetDetail struct {
	BetID         int64       `db:"bet_id" xml:"BetID"`
	BetTime       string      `db:"bet_time" xml:"BetTime"`
	PayoutTime    string      `db:"payout_time" xml:"PayoutTime"`
	Username      string      `db:"username" xml:"Username"`
	HostID        int16       `db:"host_id" xml:"HostID"`
	Detail        string      `db:"detail" xml:"Detail"`
	GameID        string      `db:"game_id" xml:"GameID"`
	Round         int         `db:"round" xml:"Round"`
	Set           int         `db:"set" xml:"Set"`
	BetAmount     float64     `db:"bet_amount" xml:"BetAmount"`
	Rolling       float64     `db:"rolling" xml:"Rolling"`
	ResultAmount  float64     `db:"result_amount" xml:"ResultAmount"`
	Balance       float64     `db:"balance" xml:"Balance"`
	GameType      string      `db:"game_type" xml:"GameType"`
	BetType       int         `db:"bet_type" xml:"BetType"`
	BetSource     int         `db:"bet_source" xml:"BetSource"`
	TransactionID int64       `db:"transaction_id" xml:"TransactionID"`
	GameResult    interface{} `db:"game_result" xml:"GameResult"`
	State         bool        `db:"state" xml:"State"`
}

func runQuery() []BetDetail {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=dummyDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	var records []BetDetail

	query := `SELECT 
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
        state
    FROM simpleplay_wager`

	err = db.Select(&records, query)
	if err != nil {
		log.Fatal(err)
	}

	for i := range records {

		s := rand.NewSource(time.Now().UnixNano())
		r := rand.New(s)

		if r.Intn(2) == 1 {
			records[i].GameResult = "Win"
		} else {
			records[i].GameResult = "Lose"
		}

		records[i].BetTime = time.Now().Format("2006-01-02T15:04:05.00")
		records[i].PayoutTime = time.Now().Format("2006-01-02T15:04:05.00")
	}

	return records

}

func HandleSimplePlay(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	fmt.Println("Simpleplay")

	// Read the raw body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}

	// Print the raw body
	fmt.Printf("Raw request body: %s\n", string(body))

	for key, values := range r.Form { // range over map
		for _, value := range values { // range over []string
			fmt.Println(key, value)
		}
	}

	// if err := r.ParseForm(); err != nil {
	// 	http.Error(w, "Error parsing form data", http.StatusBadRequest)
	// 	return
	// }

	// Get the form values
	// q := r.FormValue("q")
	// s := r.FormValue("s")

	// Print the form values
	// fmt.Println("Received q: ", q)
	// fmt.Println("Received s: ", s)

	betDetail := runQuery()

	betDetailList := BetDetailList{
		BetDetail: betDetail,
	}

	response := HistoryResponse{
		ErrorMsg:      "Success",
		ErrorMsgID:    "0",
		BetDetailList: betDetailList,
	}

	// {
	// 	{
	// 		BetTime:       "2023-08-01 12:00:00",
	// 		PayoutTime:    "2023-08-01 13:00:00",
	// 		Username:      "user1",
	// 		HostID:        1,
	// 		Detail:        "Bet on slot machine",
	// 		GameID:        "slot123",
	// 		Round:         1,
	// 		Set:           1,
	// 		BetID:         123456789,
	// 		BetAmount:     100.00,
	// 		Rolling:       50.00,
	// 		ResultAmount:  150.00,
	// 		Balance:       500.00,
	// 		GameType:      "slot",
	// 		BetType:       1,
	// 		BetSource:     1,
	// 		TransactionID: 987654321,
	// 		GameResult:    "Win",
	// 		State:         true,
	// 	},
	// 	{
	// 		BetTime:       "2023-08-01 12:00:00",
	// 		PayoutTime:    "2023-08-01 13:00:00",
	// 		Username:      "user2",
	// 		HostID:        2,
	// 		Detail:        "Bet on slot machine",
	// 		GameID:        "fishing123",
	// 		Round:         1,
	// 		Set:           1,
	// 		BetID:         123456789,
	// 		BetAmount:     100.00,
	// 		Rolling:       50.00,
	// 		ResultAmount:  150.00,
	// 		Balance:       500.00,
	// 		GameType:      "fishing",
	// 		BetType:       1,
	// 		BetSource:     1,
	// 		TransactionID: 987654321,
	// 		GameResult:    "Win",
	// 		State:         true,
	// 	},
	// }

	xmlData, err := xml.MarshalIndent(response, "", "  ")
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(xml.Header + string(xmlData)))
}
