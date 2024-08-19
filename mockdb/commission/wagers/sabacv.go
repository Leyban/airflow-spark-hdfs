package wagers

import (
	. "commission/random"
	"fmt"
	"log"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type SBCVBetBase struct {
	Id               interface{} `db:"id"`
	Trans_id         interface{} `db:"trans_id"`
	Vendor_member_id interface{} `db:"vendor_member_id"`
	Operator_id      interface{} `db:"operator_id"`
	League_id        interface{} `db:"league_id"`
	Match_id         interface{} `db:"match_id"`
	Home_id          interface{} `db:"home_id"`
	Away_id          interface{} `db:"away_id"`
	Team_id          interface{} `db:"team_id"`
	Match_datetime   interface{} `db:"match_datetime"`
	Sport_type       interface{} `db:"sport_type"`
	Bet_type         interface{} `db:"bet_type"`
	Parlay_ref_no    interface{} `db:"parlay_ref_no"`
	Odds             interface{} `db:"odds"`
	Stake            interface{} `db:"stake"`
	Transaction_time interface{} `db:"transaction_time"`
	Ticket_status    interface{} `db:"ticket_status"`
	Winlost_amount   interface{} `db:"winlost_amount"`
	After_amount     interface{} `db:"after_amount"`
	Currency         interface{} `db:"currency"`
	Winlost_datetime interface{} `db:"winlost_datetime"`
	Odds_type        interface{} `db:"odds_type"`
	Is_lucky         interface{} `db:"is_lucky"`
	Bet_team         interface{} `db:"bet_team"`
	Exculding        interface{} `db:"exculding"`
	Bet_tag          interface{} `db:"bet_tag"`
	Home_hdp         interface{} `db:"home_hdp"`
	Away_hdp         interface{} `db:"away_hdp"`
	Hdp              interface{} `db:"hdp"`
	Betfrom          interface{} `db:"betfrom"`
	Islive           interface{} `db:"islive"`
	Create_at        interface{} `db:"create_at"`
	Update_at        interface{} `db:"update_at"`
}

func CopySabacv(timeEnd time.Time) {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=collectorDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	insertSQL := `INSERT INTO sabacv_wager (
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
        update_at
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
        :update_at
    )`

	selectSQL := `SELECT * FROM sabacv_wager LIMIT 1000`

	var original []SBCVBetBase

	err = db.Select(&original, selectSQL)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Found ", len(original), " Data")

	for i := 1; i <= 31; i++ {
		fmt.Println("Inserting Sabacv for day", i)
		for _, data := range original {
			data.Trans_id = RandInt64(9223372036854775806)
			data.Transaction_time = timeEnd.Add(time.Hour*time.Duration(RandInt(23))).AddDate(0, 0, 1-i).Format(time.RFC3339Nano)
			_, err := db.NamedExec(db.Rebind(insertSQL), data)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}
