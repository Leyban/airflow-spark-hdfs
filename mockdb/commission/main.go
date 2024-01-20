package main

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

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

var memCount int64
var memAccCount int64
var affCount int64
var depoCount int64
var withdrawCount int64
var adjCount int64

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
	CreateAt          string     `db:"created_at" json:"created_at"`
	UpdateAt          string     `db:"updated_at" json:"updated_at"`
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

func createDummyMember(login_name string, currency *string) (int64, error) {
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
        created_at,
        updated_at,
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
        :created_at,
        :updated_at,
        :last_login_date,
        :registered_channel,
        :security_question,
        :security_answer,
        :rands,
        :two_factor_enabled,
        :two_factor_secret,
        :kyc
    ) RETURNING id`

	now := time.Now().UTC()

	affId := RandInt64(1100)

	entity := &Member{
		AffiliateID:       &affId,
		Uuid:              uuid.New().String(),
		LoginName:         strings.ToLower(login_name),
		Password:          RandString(24),
		Salt:              RandString(24),
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
		AccountType:       RandSelect([]int{1, 2, 3}),
		RegisteredChannel: 1,
		Rands:             RandString(24),
		TwoFactorEnabled:  RandSelect([]bool{true, false}),
		Kyc:               RandSelect([]bool{true, false}),
		CreateAt:          now.Format(time.RFC3339Nano),
		UpdateAt:          now.Format(time.RFC3339Nano),
		// Avatar:            *string,
		// Address:           *string,
		// City:              *string,
		// PostalCode:        NullableValue(RandNumber, 8, 15),
		// LastLoginDate:     *time.Time,
		// SecurityQuestion:  *string,
		// SecurityAnswer:    *string,
		// TwoFactorSecret:   *string,
	}

	if currency != nil {
		entity.Currency = *currency
	}

	// _, err = db.NamedExec(db.Rebind(query), entity)
	// NamedExecReturningId

	query, args, err := sqlx.Named(query, entity)
	if err != nil {
		return 0, err
	}
	query = db.Rebind(query)
	var id int64
	err = db.Get(&id, query, args...)
	if err != nil {
		return id, err
	}

	// if err != nil {
	// 	fmt.Println("This guy's trouble: ", login_name)
	// 	log.Fatal(err)
	// }

	memCount++

	if memCount%50 == 0 {
		fmt.Println("Inserted: ", memCount, " Members ")
	}

	return id, nil
}

type ProductMember struct {
	LoginName string `db:"login_name"`
	Currency  string `db:"currency"`
}

func get_product_members(tableName, loginNameCol string, withCurrency bool) ([]ProductMember, error) {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=collectorDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rawSql := `SELECT DISTINCT ` + loginNameCol + ` AS login_name `
	if withCurrency {
		rawSql += `, currency `
	}
	rawSql += ` FROM ` + tableName

	var result []ProductMember

	err = db.Select(&result, rawSql)
	if err != nil {
		return nil, err
	}

	return result, err
}

type Wager struct {
	TableName    string
	LoginNameCol string
	WithCurrency bool
}

func generateWagerMembers() {
	currencyLess := []Wager{
		{"sagaming_wager", "username", false},
		{"simpleplay_wager", "username", false},
		{"genesis_wager", "user_name", false},
		{"weworld_wager", "player_id", false},
		{"ebet_wager", "user_name", false},
		{"saba_wager", "vendor_member_id", false},
		{"saba_number", "vendor_member_id", false},
		{"saba_virtual", "vendor_member_id", false},
		{"sabacv_wager", "vendor_member_id", false},
	}

	for _, w := range currencyLess {
		members, err := get_product_members(w.TableName, w.LoginNameCol, w.WithCurrency)
		if err != nil {
			log.Fatal(err)
		}
		print(w.TableName, len(members))
		for _, m := range members {
			createDummyMember(m.LoginName, nil)
		}
	}

	withCurrency := []Wager{
		{"allbet_wager", "login_name", true},
		{"asiagaming_wager", "player_name", true},
		{"pgsoft_wager", "player_name", true},
		{"bti_wager", "username", true},
		{"tfgaming_wager", "member_code", true},
		{"evolution_wager", "player_id", true},
	}

	for _, w := range withCurrency {
		members, err := get_product_members(w.TableName, w.LoginNameCol, w.WithCurrency)
		if err != nil {
			log.Fatal(err)
		}
		print(w.TableName, len(members))
		for _, m := range members {
			createDummyMember(m.LoginName, &m.Currency)
		}
	}

	// digitainTable := Wager{TableName: "digitain_order_wager", LoginNameCol: "partner_client_id"}
}

type AffiliateAccount struct {
	ID                  int64      `db:"id" json:"id"`
	Uuid                string     `db:"uuid" json:"uuid"`
	AffiliateID         int64      `db:"affiliate_id" json:"affiliate_id" xlsx:"Affiliate ID"`
	LoginName           string     `db:"login_name" json:"login_name" xlsx:"Login Name"`
	FullName            string     `db:"full_name" json:"full_name" xlsx:"Full name"`
	Country             string     `db:"country" json:"country,omitempty" xlsx:"Country"`
	Currency            string     `db:"currency" json:"currency" xlsx:"Currency"`
	Language            string     `db:"language" json:"language,omitempty" xlsx:"Language"`
	Status              int        `db:"status" json:"status" xlsx:"Status"`
	PayoutFrequency     *string    `db:"payout_frequency" json:"payout_frequency" xlsx:"Payout Frequency"`
	Balance             float64    `db:"balance" json:"balance" xlsx:"Balance"`
	IsInternalAffiliate *bool      `db:"is_internal_affiliate" json:"is_internal_affiliate" xlsx:"Show Marketing Account"`
	OutstandingBalance  float64    `db:"outstanding_balance" json:"outstanding_balance" xlsx:"Outstanding Balance"`
	Level               int        `db:"level" json:"level" xlsx:"Level"`
	RegisterIp          *string    `db:"register_ip" json:"register_ip" xlsx:"Register IP"`
	CommissionTier1     *float64   `db:"commission_tier1" json:"commission_tier1"`
	CommissionTier2     *float64   `db:"commission_tier2" json:"commission_tier2"`
	CommissionTier3     *float64   `db:"commission_tier3" json:"commission_tier3" xlsx:"Commission Rate"`
	MinActivePlayer     *int       `db:"min_active_player" json:"min_active_player"`
	CommissionType      *string    `db:"commission_type" json:"commission_type"`
	DefaultRedirectPage *string    `db:"default_redirect_page" json:"default_redirect_page"`
	LastLoginDate       *time.Time `db:"last_login_date" json:"last_login_date" xlsx:"Last Login Date"`
	CreateAt            string     `db:"created_at" json:"created_at" xlsx:"Joined at"`
	UpdateAt            string     `db:"updated_at" json:"updated_at"`
	PlayerName          *string    `db:"player_name" json:"player_name"`
	DescribeOurBrand    *string    `db:"describe_our_brand" json:"describe_our_brand"`
	Phone               string     `db:"phone" json:"phone"`
	Email               string     `db:"email" json:"email"`
}

func createDummyAffiliate(affiliateID int64) (int64, error) {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=affiliateDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rawSql := `INSERT INTO affiliate_account (
            uuid,
            affiliate_id,
            login_name,
            full_name,
            country,
            currency,
            language,
            status,
            payout_frequency,
            balance,
            is_internal_affiliate,
            outstanding_balance,
            level,
            register_ip,
            commission_tier1,
            commission_tier2,
            commission_tier3,
            min_active_player,
            commission_type,
            default_redirect_page,
            last_login_date,
            created_at,
            updated_at,
            player_name,
            describe_our_brand,
            phone,
            email
        ) VALUES (
            :uuid,
            :affiliate_id,
            :login_name,
            :full_name,
            :country,
            :currency,
            :language,
            :status,
            :payout_frequency,
            :balance,
            :is_internal_affiliate,
            :outstanding_balance,
            :level,
            :register_ip,
            :commission_tier1,
            :commission_tier2,
            :commission_tier3,
            :min_active_player,
            :commission_type,
            :default_redirect_page,
            :last_login_date,
            :created_at,
            :updated_at,
            :player_name,
            :describe_our_brand,
            :phone,
            :email
        )
        ON CONFLICT DO NOTHING
    `

	now := time.Now().UTC()

	comT1 := float64(RandInt(40))
	comT2 := comT1 + float64(RandInt(10))
	comT3 := comT2 + float64(RandInt(10))
	payoutFrequency := RandSelect([]string{"monthly", "bi_monthly", "weekly"})
	minActivePlayer := 5 + RandInt(5)
	isInternalAff := RandSelect([]bool{true, false})
	playerName := RandString(15)
	commissionType := RandSelect([]string{"Revenue Share", "CPC", "CPO", "CPL", "CPI", "CPS"})

	entity := &AffiliateAccount{
		AffiliateID:         affiliateID,
		Uuid:                uuid.New().String(),
		LoginName:           strings.ToLower(RandString(24)),
		FullName:            RandString(6) + " " + RandString(9),
		Currency:            RandSelect([]string{"VND", "RMB", "THB"}),
		Balance:             float64(RandInt(100000)),
		OutstandingBalance:  float64(RandInt(100000)),
		Status:              RandSelect([]int{1, 2, 3, 4, 5, 6}),
		CommissionTier1:     &comT1,
		CommissionTier2:     &comT2,
		CommissionTier3:     &comT3,
		PayoutFrequency:     &payoutFrequency,
		MinActivePlayer:     &minActivePlayer,
		Phone:               "09" + strconv.Itoa(RandInt(9)),
		Country:             RandSelect([]string{"VN", "CN", "TH", "US"}),
		Language:            RandSelect([]string{"vi-VN", "zh-CN", "th-TH", "en-US"}),
		Email:               RandString(16) + "@" + RandString(6) + ".com",
		Level:               RandSelect([]int{1, 2, 3, 4}),
		IsInternalAffiliate: &isInternalAff,
		PlayerName:          &playerName,
		CommissionType:      &commissionType,
		CreateAt:            now.Format(time.RFC3339Nano),
		UpdateAt:            now.Format(time.RFC3339Nano),
		// RegisterIp: ,
		// DescribeOurBrand: ,
		// LastLoginDate: ,
		// DefaultRedirectPage: ,
	}

	res, err := db.NamedExec(db.Rebind(rawSql), entity)
	if err != nil {
		fmt.Println("This guy's trouble: ", affiliateID, playerName)
		log.Fatal(err)
	}

	affCount++

	if affCount%50 == 0 {
		fmt.Println("Inserted: ", affCount, " Affiliates ")
	}

	return res.LastInsertId()
}

type Deposit struct {
	ID                int64       `db:"id" json:"id"`
	TransactionID     string      `db:"transaction_id" json:"transaction_id"`
	MemberID          int64       `db:"member_id" json:"member_id"`
	BankAccountID     int64       `db:"bank_account_id" json:"bank_account_id"`
	PaymentMethodCode string      `db:"payment_method_code" json:"payment_method_code"`
	RefCode           string      `db:"ref_code" json:"ref_code"`
	Status            int         `db:"status" json:"status"`
	LoginName         string      `db:"login_name" json:"login_name"`
	Currency          string      `db:"currency" json:"currency"`
	NetAmount         float64     `db:"net_amount" json:"net_amount"`
	GrossAmount       float64     `db:"gross_amount" json:"gross_amount"`
	ChargeAmount      float64     `db:"charge_amount" json:"charge_amount"`
	Detail            interface{} `db:"detail" json:"detail"`
	CreatedAt         string      `db:"created_at" json:"created_at" `
	UpdatedAt         *string     `db:"updated_at" json:"updated_at"`
	CreatedBy         string      `db:"created_by" json:"created_by"`
	UpdatedBy         *string     `db:"updated_by" json:"updated_by"`
}

type Withdrawal struct {
	Id                     int64       `db:"id" json:"id"`
	BankAccountId          *int64      `db:"bank_account_id" json:"bank_account_id"`
	MemberPaymentAccountId int64       `db:"member_payment_account_id" json:"member_payment_account_id"`
	TransactionId          string      `db:"transaction_id" json:"transaction_id"`
	PaymentMethodCode      string      `db:"payment_method_code" json:"payment_method_code"`
	Status                 int         `db:"status" json:"status"`
	MemberId               int64       `db:"member_id" json:"member_id"`
	LoginName              string      `db:"login_name" json:"login_name"`
	Currency               string      `db:"currency" json:"currency"`
	GatewayType            int         `db:"gateway_type" json:"gateway_type"`
	WithdrawalAmount       float64     `db:"withdrawal_amount" json:"withdrawal_amount"`
	GrossAmount            float64     `db:"gross_amount" json:"gross_amount"`
	ChargeAmount           float64     `db:"charge_amount" json:"charge_amount"`
	MemberBankID           *int64      `db:"member_bank_id" json:"member_bank_id"`
	MemberAccountNo        *string     `db:"member_account_no" json:"member_account_no"`
	Detail                 interface{} `db:"detail" json:"detail"`
	IsRefunded             bool        `db:"is_refunded" json:"is_refunded"`
	CreatedAt              string      `db:"created_at" json:"created_at"`
	UpdatedAt              string      `db:"updated_at" json:"updated_at"`

	PreviousStatus int `db:"previous_status" json:"previous_status"`
}

type Adjustment struct {
	ID                  int64          `db:"id" json:"id"`
	BatchAdjustmentID   *int64         `db:"batch_adjustment_id" json:"batch_adjustment_id,omitempty"`
	TransactionID       string         `db:"transaction_id" json:"transaction_id" xlsx:"Transaction ID"`
	TransactionType     int            `db:"transaction_type" json:"transaction_type"`
	LoginName           string         `db:"login_name" json:"login_name" xlsx:"Username"`
	Currency            string         `db:"currency" json:"currency" xlsx:"Currency"`
	ReasonID            int64          `db:"reason_id" json:"reason_id"`
	Amount              float64        `db:"amount" json:"amount" xlsx:"Amount"`
	BatchAdjustmentName *string        `db:"batch_adjustment_name" json:"batch_adjustment_name,omitempty" xlsx:"Batch adjustment name"`
	ReasonName          *string        `db:"reason_name" json:"reason_name,omitempty" xlsx:"Reason"`
	Status              int            `db:"status" json:"status" xlsx:"Status"`
	MemberID            int64          `db:"member_id" json:"member_id"`
	CreatedAt           string         `db:"created_at" json:"created_at"`
	CreatedBy           string         `db:"created_by" json:"created_by"`
	UpdatedBy           *string        `db:"updated_by" json:"updated_by,omitempty" xlsx:"Modified by"`
	UpdatedAt           string         `db:"updated_at" json:"updated_at" xlsx:"Last modified"`
	BatchTransactionID  *string        `db:"batch_transaction_id" json:"batch_transaction_id,omitempty"`
	MemberStatus        int            `db:"member_status" json:"member_status" xlsx:"Member Status"`
	AdjustmentLog       []*interface{} `json:"adjustment_log,omitempty"`
}

func createDummyDeposits(memberID int64) {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=paymentDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	entity := &Deposit{
		RefCode:           RandString(14),
		PaymentMethodCode: RandSelect([]string{"DLBT", "DLBT60"}),
		TransactionID:     RandString(16),
		Status:            RandSelect([]int{1, 2, 3, 4}),
		MemberID:          memberID,
		LoginName:         RandString(12),
		Currency:          RandSelect([]string{"VND", "RMB", "THB"}),
		NetAmount:         float64(RandInt(10000)),
		GrossAmount:       float64(RandInt(10000)),
		ChargeAmount:      float64(RandInt(10000)),
		Detail:            "{}",
		BankAccountID:     RandInt64(1000000),
		CreatedAt:         time.Now().UTC().AddDate(0, 0, -RandInt(30)).Format(time.RFC3339Nano),
		CreatedBy:         RandString(24),
	}

	rawSql := `INSERT INTO deposit (
        transaction_id,
        member_id,
        bank_account_id,
        payment_method_code,
        ref_code, status,
        login_name,
        currency,
        net_amount,
        gross_amount,
        charge_amount,
        detail,
        created_at,
        created_by
    ) 
    VALUES (
        :transaction_id,
        :member_id,
        :bank_account_id,
        :payment_method_code,
        :ref_code,
        :status,
        :login_name,
        :currency,
        :net_amount,
        :gross_amount,
        :charge_amount,
        :detail,
        :created_at,
        :created_by
    )
    `
	_, err = db.NamedExec(db.Rebind(rawSql), entity)
	if err != nil {
		log.Fatal(err)
	}

	depoCount++

	if depoCount%50 == 0 {
		fmt.Println("Inserted: ", depoCount, " Deposits ")
	}
}

func createDummyWithdrawal(memberID int64) {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=paymentDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	entity := Withdrawal{
		MemberId:               memberID,
		MemberPaymentAccountId: RandInt64(10000),
		TransactionId:          RandString(16),
		PaymentMethodCode:      RandSelect([]string{"WLBT"}),
		Status:                 RandSelect([]int{1, 2, 3, 4, 5, 6, 7}),
		LoginName:              RandString(12),
		Currency:               RandSelect([]string{"VND", "RMB", "THB"}),
		WithdrawalAmount:       float64(RandInt64(100000)),
		GrossAmount:            float64(RandInt64(100000)),
		ChargeAmount:           float64(RandInt64(100000)),
		Detail:                 "{}",
		GatewayType:            RandSelect([]int{1, 2, 3, 4}),
		IsRefunded:             false,
		UpdatedAt:              time.Now().UTC().AddDate(0, 0, -RandInt(30)).Format(time.RFC3339Nano),
		CreatedAt:              time.Now().UTC().AddDate(0, 0, -RandInt(30)).Format(time.RFC3339Nano),
		// MemberBankID:           &cmd.MemberBankID,
		// MemberAccountNo:        &cmd.MemberAccountNo,
	}

	rawSql := `
    INSERT INTO withdrawal (
        member_payment_account_id,
        transaction_id,
        payment_method_code,
        status,
        member_id,
        login_name,
        currency,
        withdrawal_amount,
        gross_amount,
        charge_amount,
        member_bank_id,
        member_account_no,
        detail,
        is_refunded,
        gateway_type,
        created_at,
        updated_at
    ) 
    VALUES (
        :member_payment_account_id,
        :transaction_id,
        :payment_method_code,
        :status,
        :member_id,
        :login_name,
        :currency,
        :withdrawal_amount,
        :gross_amount,
        :charge_amount,
        :member_bank_id,
        :member_account_no,
        :detail,
        :is_refunded,
        :gateway_type,
        :created_at,
        :updated_at
    )
    `

	_, err = db.NamedExec(db.Rebind(rawSql), entity)
	if err != nil {
		log.Fatal(err)
	}

	withdrawCount++

	if depoCount%50 == 0 {
		fmt.Println("Inserted: ", withdrawCount, " Withdrawal ")
	}
}

func createDummyAdjustment(memberID int64) {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=paymentDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	entity := &Adjustment{
		TransactionID:   RandString(16),
		TransactionType: 2,
		ReasonID:        RandInt64(10000),
		MemberID:        memberID,
		LoginName:       RandString(16),
		Status:          RandSelect([]int{1, 2, 3, 4}),
		Amount:          float64(RandInt64(10000)),
		Currency:        RandSelect([]string{"VND", "RMB", "THB"}),
		CreatedAt:       time.Now().UTC().AddDate(0, 0, -RandInt(30)).Format(time.RFC3339Nano),
		UpdatedAt:       time.Now().UTC().AddDate(0, 0, -RandInt(30)).Format(time.RFC3339Nano),
		CreatedBy:       RandString(16),
		// BatchAdjustmentID: cmd.BatchAdjustmentID,
	}

	rawSql := `
    INSERT INTO "adjustment" (
        batch_adjustment_id,
        transaction_id,
        transaction_type,
        reason_id,
        login_name,
        member_id,
        status,
        amount,
        currency,
        created_at,
        updated_at,
        created_by
    ) VALUES (
        :batch_adjustment_id,
        :transaction_id,
        :transaction_type,
        :reason_id,
        :login_name,
        :member_id,
        :status,
        :amount,
        :currency,
        :created_at,
        :updated_at,
        :created_by
    ) RETURNING id
    `

	_, err = db.NamedExec(db.Rebind(rawSql), entity)
	if err != nil {
		log.Fatal(err)
	}

	adjCount++

	if adjCount%50 == 0 {
		fmt.Println("Inserted: ", adjCount, " Adjustments ")
	}
}

func getMembers() []Member {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=identityDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rawSql := `
        SELECT *
        FROM member
    `

	var results []Member

	err = db.Select(&results, rawSql)
	if err != nil {
		log.Fatal(err)
	}

	return results
}

type MemberAccount struct {
	Id                 int64   `json:"id" db:"id"`
	MemberID           int64   `json:"member_id" db:"member_id"`
	LoginName          string  `json:"login_name" db:"login_name"`
	FullName           string  `json:"full_name" db:"full_name"`
	Currency           string  `json:"currency" db:"currency"`
	Status             int     `json:"status" db:"status"`
	Balance            float64 `json:"balance" db:"balance"`
	OutstandingBalance float64 `json:"outstanding_balance" db:"outstanding_balance"`
	AvailableBalance   float64 `json:"available_balance" db:"available_balance"`
	Uuid               string  `json:"uuid" db:"uuid"`
	AffiliateID        *int64  `json:"affiliate_id" db:"affiliate_id"`
	Email              *string `json:"email" db:"email"`
	Phone              string  `json:"phone" db:"phone"`
	PaymentStatus      int     `json:"payment_status" db:"payment_status"`
	RiskStatus         int     `json:"risk_status" db:"risk_status"`
	CreatedAt          string  `json:"created_at" db:"created_at"`
	UpdatedAt          string  `json:"updated_at" db:"updated_at"`

	//new
	ClientIP    string
	Remarks     string
	SelectField []string
	CreatedBy   string
}

func createMemberAccount(member Member) {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=paymentDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	entity := &MemberAccount{
		Uuid:               member.Uuid,
		AffiliateID:        member.AffiliateID,
		MemberID:           member.ID,
		LoginName:          member.LoginName,
		FullName:           member.FullName,
		Currency:           member.Currency,
		Status:             member.Status,
		Balance:            0,
		OutstandingBalance: 0,
		Email:              &member.Email,
		Phone:              member.Phone,
		CreatedAt:          time.Now().UTC().Format(time.RFC3339Nano),
		UpdatedAt:          time.Now().UTC().Format(time.RFC3339Nano),
	}

	rawSql := `
        INSERT INTO "member_account" (
            uuid,
            member_id,
            login_name,
            full_name,
            currency,
            status,
            balance,
            outstanding_balance,
            affiliate_id,
            email,
            phone,
            created_at,
            updated_at
        )
        VALUES(
            :uuid,
            :member_id,
            :login_name,
            :full_name,
            :currency,
            :status,
            :balance,
            :outstanding_balance,
            :affiliate_id,
            :email,
            :phone,
            :created_at,
            :updated_at
        )
    `

	_, err = db.NamedExec(db.Rebind(rawSql), entity)
	if err != nil {
		log.Fatal(err)
	}

	memAccCount++

	if memAccCount%50 == 0 {
		fmt.Println("Copied: ", memAccCount, " Members ")
	}
}

func copyMembersToPaymentDB() {
	members := getMembers()

	for _, m := range members {
		createMemberAccount(m)
	}

}

func generateDeposits(memberID int64) {
	howMany := RandInt(12)

	for i := 1; i <= howMany-2; i++ {
		createDummyDeposits(memberID)
	}
}
func generateWithdrawals(memberID int64) {
	howMany := RandInt(12)

	for i := 1; i <= howMany-2; i++ {
		createDummyWithdrawal(memberID)
	}
}
func generateAdjustments(memberID int64) {
	howMany := RandInt(5)

	for i := 1; i <= howMany-2; i++ {
		createDummyAdjustment(memberID)
	}
}

func generateTransactionMembers() {

	affiliateCount := 1000
	memberCount := 10_000

	for i := 1; i <= affiliateCount; i++ {
		createDummyAffiliate(int64(i))
	}

	for i := 1; i <= memberCount; i++ {
		loginName := RandString(16)
		memId, err := createDummyMember(loginName, nil)
		if err != nil {
			log.Fatal(err)
		}

		generateDeposits(memId)
		generateWithdrawals(memId)
		generateAdjustments(memId)
	}
}

func main() {
	generateWagerMembers()
	generateTransactionMembers()
	copyMembersToPaymentDB()
}
