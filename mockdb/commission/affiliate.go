package main

import (
	. "commission/random"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

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
