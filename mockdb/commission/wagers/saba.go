package wagers

import (
	. "commission/random"
	"fmt"
	"log"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type SabaData struct {
	Id              interface{} `db:"id"`
	TransId         interface{} `db:"trans_id"`
	VendorMemberId  interface{} `db:"vendor_member_id"`
	OperatorId      interface{} `db:"operator_id"`
	LeagueId        interface{} `db:"league_id"`
	MatchId         interface{} `db:"match_id"`
	HomeId          interface{} `db:"home_id"`
	AwayId          interface{} `db:"away_id"`
	TeamId          interface{} `db:"team_id"`
	MatchDatetime   interface{} `db:"match_datetime"`
	SportType       interface{} `db:"sport_type"`
	BetType         interface{} `db:"bet_type"`
	ParlayRefNo     interface{} `db:"parlay_ref_no"`
	Odds            interface{} `db:"odds"`
	Stake           interface{} `db:"stake"`
	TransactionTime interface{} `db:"transaction_time"`
	TicketStatus    interface{} `db:"ticket_status"`
	WinlostAmount   interface{} `db:"winlost_amount"`
	AfterAmount     interface{} `db:"after_amount"`
	Currency        interface{} `db:"currency"`
	WinlostDatetime interface{} `db:"winlost_datetime"`
	OddsType        interface{} `db:"odds_type"`
	IsLucky         interface{} `db:"is_lucky"`
	BetTeam         interface{} `db:"bet_team"`
	Exculding       interface{} `db:"exculding"`
	BetTag          interface{} `db:"bet_tag"`
	HomeHdp         interface{} `db:"home_hdp"`
	AwayHdp         interface{} `db:"away_hdp"`
	Hdp             interface{} `db:"hdp"`
	Betfrom         interface{} `db:"betfrom"`
	Islive          interface{} `db:"islive"`
	CreateAt        interface{} `db:"create_at"`
	UpdateAt        interface{} `db:"update_at"`
	VersionKey      interface{} `db:"version_key"`
}

func CopySaba(timeEnd time.Time) {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=collectorDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	insertSQL := `INSERT INTO saba_wager (
        trans_id,
        vendor_member_id,
        operator_id,
        league_id,
        match_id,
        home_id,
        away_id,
        team_id,
        match_datetime,
        sport_type,
        bet_type,
        parlay_ref_no,
        odds,
        stake,
        transaction_time,
        ticket_status,
        winlost_amount,
        after_amount,
        currency,
        winlost_datetime,
        odds_type,
        is_lucky,
        bet_team,
        exculding,
        bet_tag,
        home_hdp,
        away_hdp,
        hdp,
        betfrom,
        islive,
        create_at,
        update_at,
        version_key
    ) VALUES (
        :trans_id,
        :vendor_member_id,
        :operator_id,
        :league_id,
        :match_id,
        :home_id,
        :away_id,
        :team_id,
        :match_datetime,
        :sport_type,
        :bet_type,
        :parlay_ref_no,
        :odds,
        :stake,
        :transaction_time,
        :ticket_status,
        :winlost_amount,
        :after_amount,
        :currency,
        :winlost_datetime,
        :odds_type,
        :is_lucky,
        :bet_team,
        :exculding,
        :bet_tag,
        :home_hdp,
        :away_hdp,
        :hdp,
        :betfrom,
        :islive,
        :create_at,
        :update_at,
        :version_key
    )`

	selectSQL := `SELECT * FROM saba_wager LIMIT 1000`

	var original []SabaData

	err = db.Select(&original, selectSQL)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Found ", len(original), " Data")

	for i := 1; i <= 31; i++ {
		fmt.Println("Inserting Saba for day", i)
		for _, data := range original {
			data.TransId = RandInt64(9223372036854775806)
			data.TransactionTime = timeEnd.Add(time.Hour*time.Duration(RandInt(23))).AddDate(0, 0, 1-i).Format(time.RFC3339Nano)
			_, err := db.NamedExec(db.Rebind(insertSQL), data)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}
