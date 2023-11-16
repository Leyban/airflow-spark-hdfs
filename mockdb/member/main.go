package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

var count int64

type Member struct {
	ID                int64      `db:"id" json:"id"`
	AffiliateID       *int64     `db:"affiliate_id" json:"affiliate_id"`
	Uuid              string     `db:"uuid" json:"uuid"`
	LoginName         string     `db:"login_name" json:"login_name"`
	Password          string     `db:"password" json:"password,omitempty"`
	Salt              string     `db:"salt" json:"salt,omitempty"`
	Avatar            *string    `db:"avatar" json:"avatar"`
	Status            int        `db:"status" json:"status"`
	Currency          string     `db:"currency" json:"currency"`
	Email             string     `db:"email" json:"email"`
	EmailVerifyStatus int        `db:"email_verify_status" json:"email_verify_status"`
	FullName          string     `db:"full_name" json:"full_name"`
	DateOfBirth       time.Time  `db:"date_of_birth" json:"date_of_birth"`
	Phone             string     `db:"phone" json:"phone"`
	PhoneVerifyStatus int        `db:"phone_verify_status" json:"phone_verify_status"`
	Language          string     `db:"language" json:"language"`
	Country           string     `db:"country" json:"country"`
	Address           *string    `db:"address" json:"address"`
	City              *string    `db:"city" json:"city"`
	AccountType       int        `db:"account_type" json:"account_type"`
	PostalCode        *int64     `db:"postal_code" json:"postal_code"`
	CreateAt          string     `db:"create_at" json:"create_at"`
	UpdateAt          string     `db:"update_at" json:"update_at"`
	LastLoginDate     *time.Time `db:"last_login_date" json:"last_login_date"`
	RegisteredChannel int        `db:"registered_channel" json:"registered_channel"`
	SecurityQuestion  *string    `db:"security_question" json:"security_question"`
	SecurityAnswer    *string    `db:"security_answer" json:"security_answer"`
	Rands             string     `db:"rands" json:"rands"`
	TwoFactorEnabled  bool       `db:"two_factor_enabled" json:"two_factor_enabled"`
	TwoFactorSecret   *string    `db:"two_factor_secret" json:"two_factor_secret"`
	Kyc               bool       `db:"kyc" json:"kyc"`

	//new
	ClientIP    string
	Remarks     string
	SelectField []string
	CreateBy    string
}

// Return a one of the values from the given slice n
func RandSelect[T any](n []T) T {
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)
	return n[r.Intn(len(n))]
}

// Create Alpanumeric with with n length
func RandAlphanumeric(n int) string {
	const letterBytes = "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[r.Intn(len(letterBytes))]
	}
	return string(b)
}

// Create String with alphabet characters with n length
func RandString(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[r.Intn(len(letterBytes))]
	}
	return string(b)
}

// Creates a int with [0,n)
func RandInt(n int) int {
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)
	return r.Intn(n)
}

// Creates a int64 with [0,n)
func RandInt64(n int) int64 {
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)
	return int64(r.Intn(n))
}

// Creates a Null or calls the function given a percent that it will be nil
func NullableValue[T any](f func(n T) T, n T, nilPercent int) any {
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)
	chance := r.Intn(100)

	if nilPercent > chance {
		return nil
	}

	return f(n)
}

func createDummyMember(login_name string) error {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=identityDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	query := `INSERT INTO member (
        affiliate_id,
        uuid,
        login_name,
        password,
        salt,
        avatar,
        status,
        currency,
        email,
        email_verify_status,
        full_name,
        date_of_birth,
        phone,
        phone_verify_status,
        language,
        country,
        address,
        city,
        account_type,
        postal_code,
        create_at,
        update_at,
        last_login_date,
        registered_channel,
        security_question,
        security_answer,
        rands,
        two_factor_enabled,
        two_factor_secret,
        kyc
    ) VALUES (
        :affiliate_id,
        :uuid,
        :login_name,
        :password,
        :salt,
        :avatar,
        :status,
        :currency,
        :email,
        :email_verify_status,
        :full_name,
        :date_of_birth,
        :phone,
        :phone_verify_status,
        :language,
        :country,
        :address,
        :city,
        :account_type,
        :postal_code,
        :create_at,
        :update_at,
        :last_login_date,
        :registered_channel,
        :security_question,
        :security_answer,
        :rands,
        :two_factor_enabled,
        :two_factor_secret,
        :kyc
    )
    ON CONFLICT DO NOTHING`

	now := time.Now().UTC()

	entity := &Member{
		// AffiliateID:       *RandInt64(12),
		Uuid:      uuid.New().String(),
		LoginName: login_name,
		Password:  RandString(24),
		Salt:      RandString(24),
		// Avatar:            *string,
		Status:            1,
		Currency:          RandSelect([]string{"VND", "RMB", "THB"}),
		Email:             RandString(24),
		EmailVerifyStatus: RandSelect([]int{0, 1}),
		FullName:          RandString(24),
		DateOfBirth:       now,
		Phone:             RandString(24),
		PhoneVerifyStatus: RandSelect([]int{0, 1}),
		Language:          RandSelect([]string{"vi-VN", "zh-CN", "th-TH", "en-US"}),
		Country:           RandSelect([]string{"VN", "CN", "TH", "US"}),
		// Address:           *string,
		// City:              *string,
		AccountType: RandSelect([]int{1, 2, 3}),
		// PostalCode:        NullableValue(RandNumber, 8, 15),
		CreateAt: now.Format(time.RFC3339Nano),
		UpdateAt: now.Format(time.RFC3339Nano),
		// LastLoginDate:     *time.Time,
		RegisteredChannel: 1,
		// SecurityQuestion:  *string,
		// SecurityAnswer:    *string,
		Rands:            RandString(24),
		TwoFactorEnabled: RandSelect([]bool{true, false}),
		// TwoFactorSecret:   *string,
		Kyc: RandSelect([]bool{true, false}),
	}

	_, err = db.NamedExec(db.Rebind(query), entity)

	if err != nil {
		fmt.Println("This guy's trouble: ", login_name)
		log.Fatal(err)
	}

	count++

	if count%50 == 0 {
		fmt.Println("Inserted: ", count, " Members ")
	}

	return nil
}

func get_sagaming_members() []string {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=collectorDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rawSql := `
        SELECT DISTINCT username
        FROM sagaming_wager
    `

	var result []string

	err = db.Select(&result, rawSql)
	if err != nil {
		log.Fatal(err)
	}

	return result
}

func get_simpleplay_members() []string {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=collectorDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rawSql := `
        SELECT DISTINCT username
        FROM simpleplay_wager
    `

	var result []string

	err = db.Select(&result, rawSql)
	if err != nil {
		log.Fatal(err)
	}

	return result
}

func get_genesis_members() []string {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=collectorDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rawSql := `
        SELECT DISTINCT user_name
        FROM genesis_wager
    `

	var result []string

	err = db.Select(&result, rawSql)
	if err != nil {
		log.Fatal(err)
	}

	return result
}

func get_weworld_members() []string {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=collectorDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rawSql := `
        SELECT DISTINCT player_id
        FROM weworld_wager
    `

	var result []string

	err = db.Select(&result, rawSql)
	if err != nil {
		log.Fatal(err)
	}

	return result
}

func get_ebet_members() []string {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=collectorDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rawSql := `
        SELECT DISTINCT user_name
        FROM ebet_wager
    `

	var result []string

	err = db.Select(&result, rawSql)
	if err != nil {
		log.Fatal(err)
	}

	return result
}

func get_saba_wager_members() []string {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=collectorDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rawSql := `
        SELECT DISTINCT vendor_member_id
        FROM saba_wager
    `

	var result []string

	err = db.Select(&result, rawSql)
	if err != nil {
		log.Fatal(err)
	}

	return result
}

func get_saba_number_members() []string {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=collectorDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rawSql := `
        SELECT DISTINCT vendor_member_id
        FROM saba_number
    `

	var result []string

	err = db.Select(&result, rawSql)
	if err != nil {
		log.Fatal(err)
	}

	return result
}

func get_saba_virtual_members() []string {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=collectorDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rawSql := `
        SELECT DISTINCT vendor_member_id
        FROM saba_virtual
    `

	var result []string

	err = db.Select(&result, rawSql)
	if err != nil {
		log.Fatal(err)
	}

	return result
}

func get_sabacv_members() []string {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=collectorDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rawSql := `
        SELECT DISTINCT vendor_member_id
        FROM sabacv_wager
    `

	var result []string

	err = db.Select(&result, rawSql)
	if err != nil {
		log.Fatal(err)
	}

	return result
}

func get_digitain_ids() []int64 {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=collectorDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rawSql := `
        SELECT DISTINCT partner_client_id
        FROM digitain_order_wager
    `

	var result []int64

	err = db.Select(&result, rawSql)
	if err != nil {
		log.Fatal(err)
	}

	return result
}

type DigitainUser struct {
	Id        int64  `db:"id"`
	LoginName string `db:"login_name"`
	Currency  string `db:"currency"`
	CreateAt  string `db:"created_at"`
}

func create_dummy_digitain_user(id int64) error {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=collectorDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	now := time.Now().UTC().Format(time.RFC3339Nano)

	rawSql := `
        INSERT INTO digitain_user(
            id,
            login_name,
            currency,
            created_at
        ) VALUES (
            :id,
            :login_name,
            :currency,
            :created_at
        )
        ON CONFLICT DO NOTHING
    `

	entity := DigitainUser{
		Id:        int64(RandInt(10000000)),
		LoginName: RandAlphanumeric(15),
		Currency:  RandSelect[string]([]string{"VND", "THB", "RMB"}),
		CreateAt:  now,
	}

	_, err = db.NamedExec(rawSql, entity)
	if err != nil {
		log.Fatal(err)
	}

	return nil
}

func updateMembers() {
	allMembers := []string{}

	simple_members := get_simpleplay_members()
	fmt.Println("simpleplay: ", len(simple_members))
	allMembers = append(allMembers, simple_members...)

	saga_members := get_sagaming_members()
	fmt.Println("sagagaming: ", len(saga_members))
	allMembers = append(allMembers, saga_members...)

	genesis_members := get_genesis_members()
	fmt.Println("genesis: ", len(genesis_members))
	allMembers = append(allMembers, genesis_members...)

	weworld_members := get_weworld_members()
	fmt.Println("weworld: ", len(weworld_members))
	allMembers = append(allMembers, weworld_members...)

	ebet_members := get_ebet_members()
	fmt.Println("ebet: ", len(ebet_members))
	allMembers = append(allMembers, ebet_members...)

	sabaWager_members := get_saba_wager_members()
	fmt.Println("saba wager: ", len(sabaWager_members))
	allMembers = append(allMembers, sabaWager_members...)

	sabaNumber_members := get_saba_number_members()
	fmt.Println("saba number: ", len(sabaNumber_members))
	allMembers = append(allMembers, sabaNumber_members...)

	sabaVirtual_members := get_saba_virtual_members()
	fmt.Println("saba virtual: ", len(sabaVirtual_members))
	allMembers = append(allMembers, sabaVirtual_members...)

	sabacv_members := get_sabacv_members()
	fmt.Println("sabacv: ", len(sabacv_members))
	allMembers = append(allMembers, sabacv_members...)

	fmt.Println("Total: ", len(allMembers))

	distinctMembers := make(map[string]bool, 0)
	for _, m := range allMembers {
		_, ok := distinctMembers[m]
		if !ok {
			distinctMembers[m] = true

			err := createDummyMember(m)
			if err != nil {
				log.Fatal(err)
			}

		}
	}

	digitain_ids := get_digitain_ids()
	fmt.Println("digitain: ", len(digitain_ids))
	for _, id := range digitain_ids {
		err := create_dummy_digitain_user(id)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func main() {
	updateMembers()
}
