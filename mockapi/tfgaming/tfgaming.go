package tfgaming

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/julienschmidt/httprouter"
)

type HistoryResponse struct {
	Count         int64       `db:"count" json:"count"`
	BetDetailList []BetDetail `db:"results" json:"results"`
}

type Bet struct {
	ID                 string      `db:"id" json:"-"`
	BetHistoryID       string      `db:"bet_history_id" json:"bet_history_id"`
	Odds               *float64    `db:"odds" json:"odds"`
	MalayOdds          *float64    `db:"malay_odds" json:"malay_odds"`
	EuroOdds           *float64    `db:"euro_odds" json:"euro_odds"`
	MemberOdds         *float64    `db:"member_odds" json:"member_odds"`
	MemberOddsStyle    string      `db:"member_odds_style" json:"member_odds_style"`
	GameTypeID         int         `db:"game_type_id" json:"game_type_id"`
	GameTypeName       string      `db:"game_type_name" json:"game_type_name"`
	GameMarketName     string      `db:"game_market_name" json:"game_market_name"`
	MarketOption       string      `db:"market_option" json:"market_option"`
	MapMum             string      `db:"map_num" json:"map_num"`
	BetTypeName        string      `db:"bet_type_name" json:"bet_type_name"`
	CompetitionName    string      `db:"competition_name" json:"competition_name"`
	EventID            int         `db:"event_id" json:"event_id"`
	EventName          string      `db:"event_name" json:"event_name"`
	EventDatetime      string      `db:"event_datetime" json:"event_datetime"`
	DateCreated        string      `db:"date_created" json:"date_created"`
	SettlementDatetime *string     `db:"settlement_datetime" json:"settlement_datetime"`
	ModifiedDatetime   *string     `db:"modified_datetime" json:"modified_datetime"`
	BetSelection       string      `db:"bet_selection" json:"bet_selection"`
	Currency           string      `db:"currency" json:"currency"`
	Amount             float64     `db:"amount" json:"amount"`
	SettlementStatus   string      `db:"settlement_status" json:"settlement_status"`
	IsUnsettled        bool        `db:"is_unsettled" json:"is_unsettled"`
	ResultStatus       string      `db:"result_status" json:"result_status"`
	Result             string      `db:"result" json:"result"`
	Earnings           *float64    `db:"earnings" json:"earnings"`
	Handicap           interface{} `db:"handicap" json:"handicap"`
	MemberCode         string      `db:"member_code" json:"member_code"`
	RequestSource      string      `db:"request_source" json:"request_source"`
	IsCombo            bool        `db:"is_combo" json:"is_combo"`
	TicketType         string      `db:"ticket_type" json:"ticket_type"`

	ParentBetHistoryID string `db:"parent_bet_history_id" json:"-"`
	CreateAt           string `db:"create_at" json:"-"`
	UpdateAt           string `db:"update_at" json:"-"`
}

type BetDetail struct {
	Bet
	Tickets []Bet `db:"tickets" json:"tickets"`
}

func runQuery(currency string, dateFrom, dateTo time.Time, page, pageSize int64) []BetDetail {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=dummyDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	var betDetails []BetDetail

	rawQuery := `
        SELECT * 
        FROM tfgaming_wager
        WHERE parent_bet_history_id = ''
        AND currency = ?
        LIMIT ?
        OFFSET ? 
    `

	ticketQuery := `
        SELECT * 
        FROM tfgaming_wager
        WHERE parent_bet_history_id = ?
    `

	offset := (page - 1) * pageSize

	err = db.Select(&betDetails, db.Rebind(rawQuery), currency, pageSize, offset)
	if err != nil {
		log.Fatal(err)
	}

	min := dateFrom.Unix()
	max := dateTo.Unix()
	delta := max - min

	for i := range betDetails {
		sec := rand.Int63n(delta) + min
		betDetails[i].DateCreated = time.Unix(sec, 0).Format(time.RFC3339)
		betDetails[i].Tickets = []Bet{}

		if betDetails[i].IsCombo {
			err = db.Select(&betDetails[i].Tickets, db.Rebind(ticketQuery), betDetails[i].BetHistoryID)
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	return betDetails
}

func countBets(currency string) int64 {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=dummyDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	var count int64

	sql := `
        SELECT count(*)
        FROM tfgaming_wager
        WHERE parent_bet_history_id = ''
        AND currency = ?
    `

	err = db.Get(&count, db.Rebind(sql), currency)
	if err != nil {
		log.Fatal(err)
	}

	return count

}

func HandleTFGaming(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	fmt.Println("TFGAMING")

	var (
		dateFrom        time.Time
		historyResponse HistoryResponse
		dateTo          time.Time
		page            int64
		pageSize        int64
		err             error
	)

	q := r.URL.Query()

	currency := ps.ByName("currency")
	if len(currency) == 0 {
		fmt.Println("what money bruh")
		w.WriteHeader(http.StatusTeapot)
		return
	}
	currency = strings.ToUpper(currency)

	dateFromStr := q.Get("from_modified_datetime")
	if len(dateFromStr) == 0 {
		fmt.Println("from modified datetime missing")
		w.WriteHeader(http.StatusTeapot)
		return
	}
	dateFrom, err = time.Parse("20060102150405", dateFromStr)
	if err != nil {
		fmt.Println("date from parsing error")
		w.WriteHeader(http.StatusTeapot)
		return
	}

	dateToStr := q.Get("to_modified_datetime")
	if len(dateToStr) == 0 {
		fmt.Println("to modified datetime missing")
		w.WriteHeader(http.StatusTeapot)
		return
	}
	dateTo, err = time.Parse("20060102150405", dateToStr)
	if err != nil {
		fmt.Println("date from parsing error")
		w.WriteHeader(http.StatusTeapot)
		return
	}

	pageSizeStr := q.Get("page_size")
	if len(pageSizeStr) == 0 {
		fmt.Println("Missing page size")
		w.WriteHeader(http.StatusTeapot)
		return
	}
	pageSize, err = strconv.ParseInt(pageSizeStr, 10, 64)
	if err != nil {
		fmt.Println("Page size parsing error")
		w.WriteHeader(http.StatusTeapot)
		return
	}

	pageStr := q.Get("page")
	if len(pageStr) == 0 {
		fmt.Println("Missing page")
		w.WriteHeader(http.StatusTeapot)
		return
	}
	page, err = strconv.ParseInt(pageStr, 10, 64)
	if err != nil {
		fmt.Println("Page parsing error")
		w.WriteHeader(http.StatusTeapot)
		return
	}

	historyResponse.BetDetailList = runQuery(currency, dateFrom, dateTo, page, pageSize)
	historyResponse.Count = countBets(currency)

	jsonResult, err := json.Marshal(historyResponse)
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonResult)
}
