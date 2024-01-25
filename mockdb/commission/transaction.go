package main

import (
	. "commission/random"
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"log"
	"time"
)

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

func createDummyDeposits(memberID int64, timeEnd time.Time) {
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
		CreatedAt:         timeEnd.AddDate(0, 0, -RandInt(30)).Add(time.Hour * time.Duration(RandInt(23))).Format(time.RFC3339Nano),
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

func createDummyWithdrawal(memberID int64, timeEnd time.Time) {
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
		UpdatedAt:              timeEnd.AddDate(0, 0, -RandInt(30)).Add(time.Hour * time.Duration(RandInt(23))).Format(time.RFC3339Nano),
		CreatedAt:              timeEnd.AddDate(0, 0, -RandInt(30)).Add(time.Hour * time.Duration(RandInt(23))).Format(time.RFC3339Nano),
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

func createDummyAdjustment(memberID int64, timeEnd time.Time) {
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
		UpdatedAt:       timeEnd.AddDate(0, 0, -RandInt(30)).Add(time.Hour * time.Duration(RandInt(23))).Format(time.RFC3339Nano),
		CreatedAt:       timeEnd.AddDate(0, 0, -RandInt(30)).Add(time.Hour * time.Duration(RandInt(23))).Format(time.RFC3339Nano),
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
