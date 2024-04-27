package bti

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"time"

	"github.com/jmoiron/sqlx"
)

type Wager struct {
	LastUpdateDate string `json:"LastUpdateDate"`
	Bets           []Bet
	CurrentPage    int64 `json:"currentPage"`
	TotalPages     int64 `json:"totalPages"`
}

type TokenRequest struct {
	AgentUsername string `json:"agentUserName"`
	AgentPassword string `json:"agentPassword"`
}

type TokenResponse struct {
	ErrorCode int    `json:"errorCode"`
	Token     string `json:"token"`
}

type History struct {
	From       string      `json:"from"`
	To         string      `json:"to"`
	Pagination *Pagination `json:"pagination"`
}

type Pagination struct {
	Page       int64 `json:"page"`
	RowPerPage int64 `json:"rowperpage"`
}

type Bet struct {
	Id                 int64       `db:"id" json:"-"`
	PL                 float64     `db:"pl" json:"PL"`
	NonCashOutAmount   float64     `db:"non_cash_out_amount" json:"NonCashOutAmount"`
	ComboBonusAmount   float64     `db:"combo_bonus_amount" json:"ComboBonusAmount"`
	BetSettledDate     *string     `db:"bet_settled_date" json:"BetSettledDate"`
	PurchaseID         string      `db:"purchase_id" json:"PurchaseID"`
	UpdateDate         *string     `db:"update_date" json:"UpdateDate"`
	Odds               int         `db:"odds" json:"Odds"`
	OddsInUserStyle    string      `db:"odds_in_user_style" json:"OddsInUserStyle"`
	OddsStyleOfUser    string      `db:"odds_style_of_user" json:"OddsStyleOfUser"`
	TotalStake         float64     `db:"total_stake" json:"TotalStake"`
	OddsDec            float64     `db:"odds_dec" json:"OddsDec"`
	ValidStake         float64     `db:"valid_stake" json:"ValidStake"`
	Platform           string      `db:"platform" json:"Platform"`
	Return             float64     `db:"return" json:"Return"`
	DomainID           int         `db:"domain_id" json:"DomainID"`
	BetStatus          string      `db:"bet_status" json:"BetStatus"`
	Brand              string      `db:"brand" json:"Brand"`
	UserName           string      `db:"username" json:"UserName"`
	BetTypeName        string      `db:"bet_type_name" json:"BetTypeName"`
	BetTypeId          int         `db:"bet_type_id" json:"BetTypeId"`
	CreationDate       string      `db:"creation_date" json:"CreationDate"`
	Status             string      `db:"status" json:"Status"`
	CustomerID         int32       `db:"customer_id" json:"CustomerID"`
	MerchantCustomerID string      `db:"merchant_customer_id" json:"MerchantCustomerID"`
	Currency           string      `db:"currency" json:"Currency"`
	PlayerLevelID      int         `db:"player_level_id" json:"PlayerLevelID"`
	PlayerLevelName    string      `db:"player_level_name" json:"PlayerLevelName"`
	Selections         []Selection `db:"selections" json:"Selections"`
	CreateAt           time.Time   `db:"create_at" json:"CreateAt"`
	UpdateAt           time.Time   `db:"update_at" json:"UpdateAt"`
}

type Selection struct {
	Id              int     `db:"id" json:"Id"`
	Isresettled     int     `db:"is_resettled" json:"Isresettled"`
	RelatedBetID    int64   `db:"related_bet_id" json:"RelatedBetID"`
	ActionType      string  `db:"action_type" json:"ActionType"`
	BonusID         int64   `db:"bonus_id" json:"BonusID"`
	CouponCode      string  `db:"coupon_code" json:"CouponCode"`
	ReferenceID     int64   `db:"reference_id" json:"ReferenceID"`
	BetID           string  `db:"bet_id" json:"BetID"`
	LineID          int64   `db:"line_id" json:"LineID"`
	Odds            int     `db:"odds" json:"Odds"`
	OddsInUserStyle string  `db:"odds_in_user_style" json:"OddsInUserStyle"`
	LeagueName      string  `db:"league_name" json:"LeagueName"`
	LeagueID        int64   `db:"league_id" json:"LeagueID"`
	HomeTeam        string  `db:"home_team" json:"HomeTeam"`
	AwayTeam        string  `db:"away_team" json:"AwayTeam"`
	BranchName      string  `db:"branch_name" json:"BranchName"`
	BranchID        int     `db:"branch_id" json:"BranchID"`
	LineTypeName    string  `db:"line_type_name" json:"LineTypeName"`
	Points          float64 `db:"points" json:"Points"`
	Score           string  `db:"score" json:"Score"`
	YourBet         string  `db:"your_bet" json:"YourBet"`
	EventDate       *string `db:"event_date" json:"EventDate"`
	EventTypeName   string  `db:"event_type_name" json:"EventTypeName"`
	BetType         string  `db:"bet_type" json:"BetType"`
	Isfreebet       int     `db:"isfreebet" json:"Isfreebet"`
	OddsDec         float64 `db:"odds_dec" json:"OddsDec"`
	LiveScore1      int     `db:"live_score1" json:"LiveScore1"`
	LiveScore2      int     `db:"live_score2" json:"LiveScore2"`
	Status          string  `db:"status" json:"Status"`
	GameID          int64   `db:"game_id" json:"GameID"`
	EventTypeID     int64   `db:"event_type_id" json:"EventTypeID"`
	LineTypeID      int64   `db:"line_type_id" json:"LineTypeID"`
	IsLive          int     `db:"is_live" json:"IsLive"`
	UpdateDate      *string `db:"update_date" json:"UpdateDate"`
	IsNewLine       int     `db:"is_new_line" json:"IsNewLine"`
}

const dummyToken = "ThisIsADummyTokenEveryBodySayYeah"

func randDate(dateFrom, dateTo time.Time) time.Time {

	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)

	diff := dateTo.Sub(dateFrom)

	newDate := dateFrom.Add(time.Duration(r.Float64()) * diff)

	return newDate
}

func HandleBtiToken(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Bti Token")

	var tokenReq TokenRequest

	err := json.NewDecoder(r.Body).Decode(&tokenReq)
	if err != nil {
		fmt.Println("Json Error")
		fmt.Println(err)
	}
	fmt.Println(tokenReq.AgentUsername, tokenReq.AgentPassword)

	tokRes := TokenResponse{
		ErrorCode: 0,
		Token:     dummyToken,
	}
	jsonResponse, err := json.Marshal(tokRes)
	if err != nil {
		fmt.Println(err)
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	w.Write(jsonResponse)
}

func randomizeBetCreationTime(bets []Bet, dateFrom, dateTo time.Time) []Bet {
	for i := range bets {
		bets[i].CreationDate = randDate(dateFrom, dateTo).Format(time.RFC3339Nano)
	}

	return bets
}

func runQuery(req History) (Wager, error) {
	var (
		bets   []Bet
		rawSql bytes.Buffer
		params []any
	)

	wager := Wager{
		LastUpdateDate: time.Now().Format(time.RFC3339Nano),
		CurrentPage:    req.Pagination.Page,
	}

	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=dummyDB sslmode=disable")
	if err != nil {
		fmt.Println(err)
	}
	defer db.Close()

	rawSql.WriteString(`
        SELECT *
        FROM bti_wager
    `)

	dateFrom, err := time.Parse("20060102150405", req.From)
	dateTo, err := time.Parse("20060102150405", req.From)
	if err != nil {
		fmt.Println(err)
	}

	params = append(params, dateFrom, dateTo)

	betCount := countBets(db, rawSql.String(), params)

	offset := req.Pagination.RowPerPage * (req.Pagination.Page)
	rawSql.WriteString(" LIMIT ? OFFSET ?")
	params = append(params, req.Pagination.RowPerPage, offset)

	err = db.Select(&bets, db.Rebind(rawSql.String()), params...)
	if err != nil {
		fmt.Println(err)
	}

	wager.Bets = bets
	wager.CurrentPage = int64(math.Ceil(float64(betCount / req.Pagination.RowPerPage)))

	return wager, nil
}

func countBets(db *sqlx.DB, rawSql string, parameters []any) int64 {
	var count int64

	err := db.Get(&count, db.Rebind("SELECT count(*) as count FROM ("+rawSql+") as dumdumtable"), parameters...)
	if err != nil {
		fmt.Println("Count Query Error")
		fmt.Println(err)
	}

	return count
}

func HandleBti(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Bti")

	fmt.Println(r.URL.Query())

	queries := r.URL.Query()

	if len(queries) > 0 {
		tok, ok := queries["token"]
		if !ok {
			w.WriteHeader(http.StatusUnauthorized)
		}
		if tok[0] != dummyToken {
			w.WriteHeader(http.StatusUnauthorized)
		}
	} else {
		w.WriteHeader(http.StatusTeapot)
	}

	var historyReq History

	err := json.NewDecoder(r.Body).Decode(&historyReq)
	if err != nil {
		fmt.Println("JSON Decoding Error")
		fmt.Println(err)
	}

	result, err := runQuery(historyReq)
	if err != nil {
		fmt.Println("Error at query")
		fmt.Println(err)
	}

	jsonResponse, err := json.Marshal(result)
	if err != nil {
		fmt.Println("Error at marshalling")
		fmt.Println(err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(jsonResponse)
}
