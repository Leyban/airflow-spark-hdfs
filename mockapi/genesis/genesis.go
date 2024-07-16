package genesis

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/julienschmidt/httprouter"
)

type HistoryResponse struct {
	Count         int64       `json:"count"`
	GenDetailList []GenDetail `json:"betHistories"`
	Status        int         `json:"status"`
}

type JSON json.RawMessage

type GenDetail struct {
	ID                     int       `db:"id" json:"-"`
	GameType               int       `db:"game_type" json:"GameType"`
	BetMap                 string    `db:"bet_map" json:"-"`
	BetMapJson             any       `db:"-" json:"BetMap"`
	JudgeResult            string    `db:"judge_result" json:"-"`
	JudgeResultJson        any       `db:"-" json:"JudgeResult"`
	RoundNo                string    `db:"round_no" json:"RoundNo"`
	Bet                    float64   `db:"bet" json:"Bet"`
	Payout                 float64   `db:"payout" json:"Payout"`
	BankerCards            string    `db:"banker_cards" json:"-"`
	BankerCardsJson        any       `db:"-" json:"BankerCards"`
	PlayerCards            string    `db:"player_cards" json:"-"`
	PlayerCardsJson        any       `db:"-" json:"PlayerCards"`
	AllDices               string    `db:"all_dices" json:"-"`
	AllDicesJson           any       `db:"-" json:"AllDices"`
	DragonCard             int       `db:"dragon_card" json:"DragonCard"`
	TigerCard              int       `db:"tiger_card" json:"TigerCard"`
	Number                 int       `db:"number" json:"Number"`
	CreateTime             time.Time `db:"create_time" json:"-"`
	CreateTimeUnix         int64     `db:"-" json:"CreateTime"`
	PayoutTime             time.Time `db:"payout_time" json:"-"`
	PayoutTimeUnix         int64     `db:"-" json:"PayoutTime"`
	BetHistoryID           string    `db:"bet_history_id" json:"BetHistoryId"`
	ValidBet               float64   `db:"valid_bet" json:"ValidBet"`
	Username               string    `db:"user_name" json:"Username"`
	UserID                 int       `db:"user_id" json:"UserId"`
	GameName               string    `db:"game_name" json:"GameName"`
	Platform               int       `db:"platform" json:"Platform"`
	PlayerResult           int       `db:"player_result" json:"PlayerResult"`
	BankerResult           int       `db:"banker_result" json:"BankerResult"`
	NiuNiuWithHoldingTotal float64   `db:"niu_niu_with_holding_total" json:"NiuNiuWithHoldingTotal"`
	CreateAt               time.Time `db:"create_at" json:"-"`
	UpdateAt               time.Time `db:"update_at" json:"-"`
}

type HistoryRequest struct {
	StartTimeStr string `json:"StartTimeStr"`
	EndTimeStr   string `json:"EndTimeStr"`
	ChannelId    int64  `json:"ChannelId"`
	SubChannelId int64  `json:"SubChannelId"`
	BetStatus    int    `json:"BetStatus"`
	PageNum      int64  `json:"PageNum"`
	PageSize     int64  `json:"PageSize"`
	Timestamp    int64  `json:"Timestamp"`
	Signature    string `json:"Signature"`
	Currency     string `json:"Currency"`
}

func runQuery(dateFrom, dateTo time.Time, page, pageSize int64) []GenDetail {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=dummyDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	var genDetails []GenDetail

	rawQuery := `
        SELECT *
        FROM genesis_wager
        LIMIT ?
        OFFSET ?
    `

	offset := (page - 1) * pageSize

	err = db.Select(&genDetails, db.Rebind(rawQuery), pageSize, offset)
	if err != nil {
		log.Fatal(err)
	}

	min := dateFrom.Unix()
	max := dateTo.Unix()
	delta := max - min

	for i := range genDetails {
		sec := rand.Int63n(delta) + min
		genDetails[i].CreateTimeUnix = sec
		genDetails[i].PayoutTimeUnix = sec

		err := json.Unmarshal([]byte(genDetails[i].BetMap), &genDetails[i].BetMapJson)
		err = json.Unmarshal([]byte(genDetails[i].BankerCards), &genDetails[i].BankerCardsJson)
		err = json.Unmarshal([]byte(genDetails[i].JudgeResult), &genDetails[i].JudgeResultJson)
		err = json.Unmarshal([]byte(genDetails[i].AllDices), &genDetails[i].AllDicesJson)
		err = json.Unmarshal([]byte(genDetails[i].PlayerCards), &genDetails[i].PlayerCardsJson)
		if err != nil {
			log.Fatal(err)
		}

		// decodedBetMap, err := base64.StdEncoding.DecodeString(string(genDetails[i].BetMap))
		// if err != nil {
		// 	log.Fatal(err)
		// }

		// var jsonBetMap interface{}

		// json.Unmarshal([]byte(betMap), jsonBetMap)

		// BetMap                 JSON      `db:"bet_map" json:"BetMap"`
		// JudgeResult            JSON      `db:"judge_result" json:"JudgeResult"`
		// RoundNo                string    `db:"round_no" json:"RoundNo"`
		// Bet                    float64   `db:"bet" json:"Bet"`
		// Payout                 float64   `db:"payout" json:"Payout"`
		// BankerCards            JSON      `db:"banker_cards" json:"BankerCards"`
		// PlayerCards            JSON      `db:"player_cards" json:"PlayerCards"`
		// AllDices               JSON      `db:"all_dices" json:"AllDices"`

	}

	return genDetails
}

func countBets() int64 {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=dummyDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	var count int64

	sql := `
        SELECT count(*)
        FROM genesis_wager
    `

	err = db.Get(&count, db.Rebind(sql))
	if err != nil {
		log.Fatal(err)
	}

	return count

}

func HandleGenesis(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	fmt.Println("Genesis")

	var (
		historyResponse HistoryResponse
		req             HistoryRequest
		err             error
	)

	err = json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		log.Fatal(err)
	}

	dateFrom, err := time.Parse("20060102150405", req.StartTimeStr)
	if err != nil {
		log.Fatal(err)
	}
	dateTo, err := time.Parse("20060102150405", req.EndTimeStr)
	if err != nil {
		log.Fatal(err)
	}

	historyResponse.GenDetailList = runQuery(dateFrom, dateTo, req.PageNum, req.PageSize)
	historyResponse.Count = countBets()
	historyResponse.Status = req.BetStatus

	jsonResult, err := json.Marshal(historyResponse)
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonResult)
}
