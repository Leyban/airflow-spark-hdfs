package main

import (
	. "commission/random"
	"fmt"
	"log"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

var affAdjCount int64

type AffAdjustment struct {
	ID              int64   `db:"id" json:"id,omitempty"`
	AffiliateID     int64   `db:"affiliate_id" json:"affiliate_id,omitempty"`
	TransactionID   string  `db:"transaction_id" json:"transaction_id,omitempty"`
	TransactionType int     `db:"transaction_type" json:"transaction_type,omitempty"`
	Status          int     `db:"status" json:"status,omitempty"`
	LoginName       string  `db:"login_name" json:"login_name,omitempty"`
	Currency        string  `db:"currency" json:"currency,omitempty"`
	Reason          *string `db:"reason" json:"reason,omitempty"`
	Amount          float64 `db:"amount" json:"amount,omitempty"`
	Type            int     `db:"type" json:"type,omitempty"`
	UpdatedAt       string  `db:"updated_at" json:"updated_at,omitempty"`
	CreatedAt       string  `db:"created_at" json:"created_at"`
	CreatedBy       *string `db:"created_by" json:"created_by,omitempty"`
	UpdatedBy       *string `db:"updated_by" json:"updated_by,omitempty"`
}

func createDummyAffAdjustment(affiliateID int64, timeEnd time.Time) error {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=affiliateDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rawSql := `INSERT INTO adjustment (
            affiliate_id,
            transaction_id,
            transaction_type,
            status,
            login_name,
            currency,
            reason,
            amount,
            type,
            updated_at,
            created_at,
            created_by,
            updated_by
        ) VALUES (
            :affiliate_id,
            :transaction_id,
            :transaction_type,
            :status,
            :login_name,
            :currency,
            :reason,
            :amount,
            :type,
            :updated_at,
            :created_at,
            :created_by,
            :updated_by
        ) ON CONFLICT DO NOTHING
    `

	now := time.Now().UTC().Format(time.RFC3339Nano)

	currency := RandSelect([]string{"VND", "THB", "RMB"})
	transactionType := RandSelect([]int{4, 3})
	reason := RandString(12)

	entity := &AffAdjustment{
		AffiliateID:     affiliateID,
		TransactionID:   RandString(12),
		TransactionType: transactionType,
		Status:          2,
		LoginName:       RandString(12),
		Currency:        currency,
		Reason:          &reason,
		Amount:          float64(RandInt(1000)),
		Type:            1,
		UpdatedAt:       now,
		CreatedAt:       timeEnd.AddDate(0, 0, -RandInt(30)).Add(time.Hour * time.Duration(RandInt(23))).Format(time.RFC3339Nano),
		CreatedBy:       &now,
		UpdatedBy:       &now,
	}

	_, err = db.NamedExec(db.Rebind(rawSql), entity)
	if err != nil {
		return err
	}

	affAdjCount++
	if affAdjCount%100 == 0 {
		fmt.Println("Inserted ", affAdjCount, " Affiliate Adjustments")
	}

	return nil
}
