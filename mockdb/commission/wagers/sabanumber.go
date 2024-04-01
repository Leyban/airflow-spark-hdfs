package wagers

import (
	. "commission/random"
	"fmt"
	"log"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type SabaNumberData struct {
	Id              interface{} `db:"id"`
	TransId         interface{} `db:"trans_id"`
	VendorMemberId  interface{} `db:"vendor_member_id"`
	OperatorId      interface{} `db:"operator_id"`
	MatchId         interface{} `db:"match_id"`
	TransactionTime interface{} `db:"transaction_time"`
	Odds            interface{} `db:"odds"`
	Stake           interface{} `db:"stake"`
	TicketStatus    interface{} `db:"ticket_status"`
	Betfrom         interface{} `db:"betfrom"`
	Islive          interface{} `db:"islive"`
	LastBallNo      interface{} `db:"last_ball_no"`
	BetTeam         interface{} `db:"bet_team"`
	WinlostDatetime interface{} `db:"winlost_datetime"`
	BetType         interface{} `db:"bet_type"`
	Currency        interface{} `db:"currency"`
	OddsType        interface{} `db:"odds_type"`
	WinlostAmount   interface{} `db:"winlost_amount"`
	AfterAmount     interface{} `db:"after_amount"`
	SportType       interface{} `db:"sport_type"`
	BaStatus        interface{} `db:"ba_status"`
	VersionKey      interface{} `db:"version_key"`
	CustomInfo1     interface{} `db:"custom_info1"`
	CustomInfo2     interface{} `db:"custom_info2"`
	CustomInfo3     interface{} `db:"custom_info3"`
	CustomInfo4     interface{} `db:"custom_info4"`
	CustomInfo5     interface{} `db:"custom_info5"`
	CreateAt        interface{} `db:"create_at"`
	UpdateAt        interface{} `db:"update_at"`
}

func CopySabaNumber(timeEnd time.Time) {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=collectorDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	insertSQL := `INSERT INTO saba_number (
        trans_id,
        vendor_member_id,
        operator_id,
        match_id,
        transaction_time,
        odds,
        stake,
        ticket_status,
        betfrom,
        islive,
        last_ball_no,
        bet_team,
        winlost_datetime,
        bet_type,
        currency,
        odds_type,
        winlost_amount,
        after_amount,
        sport_type,
        ba_status,
        version_key,
        custom_info1,
        custom_info2,
        custom_info3,
        custom_info4,
        custom_info5,
        create_at,
        update_at
    ) VALUES (
        :trans_id,
        :vendor_member_id,
        :operator_id,
        :match_id,
        :transaction_time,
        :odds,
        :stake,
        :ticket_status,
        :betfrom,
        :islive,
        :last_ball_no,
        :bet_team,
        :winlost_datetime,
        :bet_type,
        :currency,
        :odds_type,
        :winlost_amount,
        :after_amount,
        :sport_type,
        :ba_status,
        :version_key,
        :custom_info1,
        :custom_info2,
        :custom_info3,
        :custom_info4,
        :custom_info5,
        :create_at,
        :update_at
    )`

	selectSQL := `SELECT * FROM saba_number LIMIT 1000`

	var original []SabaNumberData

	err = db.Select(&original, selectSQL)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Found ", len(original), " Data")

	for i := 1; i <= 31; i++ {
		fmt.Println("Inserting SabaNumber for day", i)
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
