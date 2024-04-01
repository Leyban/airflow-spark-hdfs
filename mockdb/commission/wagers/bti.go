package wagers

import (
	. "commission/random"
	"fmt"
	"log"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type BtiData struct {
	ID                 interface{} `db:"id"`
	PurchaseId         interface{} `db:"purchase_id"`
	NonCashOutAmount   interface{} `db:"non_cash_out_amount"`
	ComboBonusAmount   interface{} `db:"combo_bonus_amount"`
	BetSettledDate     interface{} `db:"bet_settled_date"`
	Odds               interface{} `db:"odds"`
	OddsInUserStyle    interface{} `db:"odds_in_user_style"`
	OddsStyleOfUser    interface{} `db:"odds_style_of_user"`
	Pl                 interface{} `db:"pl"`
	TotalStake         interface{} `db:"total_stake"`
	OddsDec            interface{} `db:"odds_dec"`
	ValidStake         interface{} `db:"valid_stake"`
	Platform           interface{} `db:"platform"`
	Return             interface{} `db:"return"`
	DomainId           interface{} `db:"domain_id"`
	BetStatus          interface{} `db:"bet_status"`
	Brand              interface{} `db:"brand"`
	Username           interface{} `db:"username"`
	BetTypeName        interface{} `db:"bet_type_name"`
	BetTypeId          interface{} `db:"bet_type_id"`
	CreationDate       interface{} `db:"creation_date"`
	Status             interface{} `db:"status"`
	CustomerId         interface{} `db:"customer_id"`
	MerchantCustomerId interface{} `db:"merchant_customer_id"`
	Currency           interface{} `db:"currency"`
	PlayerLevelId      interface{} `db:"player_level_id"`
	PlayerLevelName    interface{} `db:"player_level_name"`
	UpdateDate         interface{} `db:"update_date"`
	CreateAt           interface{} `db:"create_at"`
	UpdateAt           interface{} `db:"update_at"`
}

func CopyBtiData(timeEnd time.Time) {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=collectorDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	insertSQL := `INSERT INTO bti_wager (
        purchase_id,
        non_cash_out_amount,
        combo_bonus_amount,
        bet_settled_date,
        odds,
        odds_in_user_style,
        odds_style_of_user,
        pl,
        total_stake,
        odds_dec,
        valid_stake,
        platform,
        return,
        domain_id,
        bet_status,
        brand,
        username,
        bet_type_name,
        bet_type_id,
        creation_date,
        status,
        customer_id,
        merchant_customer_id,
        currency,
        player_level_id,
        player_level_name,
        update_date,
        create_at,
        update_at
    ) VALUES (
        :purchase_id,
        :non_cash_out_amount,
        :combo_bonus_amount,
        :bet_settled_date,
        :odds,
        :odds_in_user_style,
        :odds_style_of_user,
        :pl,
        :total_stake,
        :odds_dec,
        :valid_stake,
        :platform,
        :return,
        :domain_id,
        :bet_status,
        :brand,
        :username,
        :bet_type_name,
        :bet_type_id,
        :creation_date,
        :status,
        :customer_id,
        :merchant_customer_id,
        :currency,
        :player_level_id,
        :player_level_name,
        :update_date,
        :create_at,
        :update_at
    )`

	selectSQL := `SELECT * FROM bti_wager LIMIT 1000`

	var original []BtiData

	err = db.Select(&original, selectSQL)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Found ", len(original), " Data")

	for i := 1; i <= 31; i++ {
		fmt.Println("Inserting Bti for day", i)
		for _, data := range original {
			data.PurchaseId = RandInt64(9223372036854775806)
			data.CreationDate = timeEnd.Add(time.Hour*time.Duration(RandInt(23))).AddDate(0, 0, 1-i).Format(time.RFC3339Nano)
			_, err := db.NamedExec(db.Rebind(insertSQL), data)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}
