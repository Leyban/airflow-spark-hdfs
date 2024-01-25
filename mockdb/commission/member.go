package main

import (
	. "commission/random"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

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

func CreateDummyMember(login_name string, currency *string) (int64, error) {
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

func getProductMembers(tableName, loginNameCol string, withCurrency bool) ([]ProductMember, error) {
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

func GetMemberIds() []int64 {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=identityDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rawSql := `
        SELECT id
        FROM member
    `

	var results []int64

	err = db.Select(&results, rawSql)
	if err != nil {
		log.Fatal(err)
	}

	return results
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
