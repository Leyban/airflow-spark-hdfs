package wagers

import (
	. "commission/random"
	"fmt"
	"log"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type AGData struct {
	Id             interface{} `db:"id"`
	BillNo         interface{} `db:"bill_no"`
	PlayerName     interface{} `db:"player_name"`
	AgentCode      interface{} `db:"agent_code"`
	GameCode       interface{} `db:"game_code"`
	NetAmount      interface{} `db:"net_amount"`
	BetTime        interface{} `db:"bet_time"`
	GameType       interface{} `db:"game_type"`
	BetAmount      interface{} `db:"bet_amount"`
	ValidBetAmount interface{} `db:"valid_bet_amount"`
	Flag           interface{} `db:"flag"`
	PlayType       interface{} `db:"play_type"`
	Currency       interface{} `db:"currency"`
	TableCode      interface{} `db:"table_code"`
	LoginIp        interface{} `db:"login_ip"`
	RecalcuTime    interface{} `db:"recalcu_time"`
	PlatformType   interface{} `db:"platform_type"`
	Remark         interface{} `db:"remark"`
	Round          interface{} `db:"round"`
	Result         interface{} `db:"result"`
	BeforeCredit   interface{} `db:"before_credit"`
	DeviceType     interface{} `db:"device_type"`
	GameTypeCode   interface{} `db:"game_type_code"`
	CreateAt       interface{} `db:"create_at"`
	UpdateAt       interface{} `db:"update_at"`
}

func CopyAGData(timeEnd time.Time) {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=collectorDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	insertSQL := `INSERT INTO asiagaming_wager (
        bill_no,
        player_name,
        agent_code,
        game_code,
        net_amount,
        bet_time,
        game_type,
        bet_amount,
        valid_bet_amount,
        flag,
        play_type,
        currency,
        table_code,
        login_ip,
        recalcu_time,
        platform_type,
        remark,
        round,
        result,
        before_credit,
        device_type,
        game_type_code,
        create_at,
        update_at
    ) VALUES (
        :bill_no,
        :player_name,
        :agent_code,
        :game_code,
        :net_amount,
        :bet_time,
        :game_type,
        :bet_amount,
        :valid_bet_amount,
        :flag,
        :play_type,
        :currency,
        :table_code,
        :login_ip,
        :recalcu_time,
        :platform_type,
        :remark,
        :round,
        :result,
        :before_credit,
        :device_type,
        :game_type_code,
        :create_at,
        :update_at
    )`

	selectSQL := `SELECT * FROM asiagaming_wager LIMIT 1000`

	var original []AGData

	err = db.Select(&original, selectSQL)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Found ", len(original), " Data")

	for i := 1; i <= 31; i++ {
		fmt.Println("Inserting Asiagaming for day", i)
		for _, data := range original {
			data.BillNo = RandInt64(9223372036854775806)
			data.BetTime = timeEnd.Add(time.Hour*time.Duration(RandInt(23))).AddDate(0, 0, 1-i).Format(time.RFC3339Nano)
			_, err := db.NamedExec(db.Rebind(insertSQL), data)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}
