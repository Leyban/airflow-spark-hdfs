package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type Status int64

const (
	StatusUnClaim Status = iota + 1
	StatusClaimed
)

type DepositStatusType int

const (
	DepositProcessing DepositStatusType = iota + 1
	DepositSuccessful
	DepositFailed
	DepositExpired
)

type Deposit struct {
	Id              int64             `db:"id" json:"id"`
	TransactionId   string            `db:"transaction_id" json:"transaction_id"`
	MemberId        int64             `db:"member_id" json:"member_id"`
	BankAccountId   int64             `db:"bank_account_id" json:"bank_account_id"`
	PaymentTypeCode string            `db:"payment_type_code" json:"payment_type_code"`
	RefCode         string            `db:"ref_code" json:"ref_code"`
	Status          DepositStatusType `db:"status" json:"status"`
	LoginName       string            `db:"login_name" json:"login_name"`
	Currency        string            `db:"currency" json:"currency"`
	NetAmount       float64           `db:"net_amount" json:"net_amount"`
	GrossAmount     float64           `db:"gross_amount" json:"gross_amount"`
	ChargeAmount    float64           `db:"charge_amount" json:"charge_amount"`
	Detail          interface{}       `db:"detail" json:"detail"`
	CreateAt        string            `db:"create_at" json:"create_at" `
	UpdateAt        string            `db:"update_at" json:"update_at"`
	CreateBy        string            `db:"create_by" json:"create_by"`
	UpdateBy        *string           `db:"update_by" json:"update_by"`
}

type OnlineBankData struct {
	Id              int64     `db:"id" json:"id"`
	BankId          int64     `db:"bank_id" json:"bank_id"`
	BankAccountId   int64     `db:"bank_account_id" json:"bank_account_id"`
	DepositId       int       `db:"deposit_id" json:"deposit_id"`
	BankAccountCode string    `db:"bank_account_code" json:"bank_account_code"`
	BankCode        *string   `db:"bank_code" json:"bank_code"`
	BankReference   string    `db:"bank_reference" json:"bank_reference"`
	BankDescription string    `db:"bank_description" json:"bank_description"`
	NetAmount       float64   `db:"net_amount" json:"net_amount"`
	TransactionDate time.Time `db:"transaction_date" json:"transaction_date"`
	CreateAt        string    `db:"create_at" json:"create_at"`
	UpdateAt        string    `db:"update_at" json:"update_at"`
	Status          Status    `db:"status" json:"status"`
}

type DummyOnlineData struct {
	Currency             string `db:"currency"`
	Remark               string `db:"remark"`
	Amount               int64  `db:"amount"`
	Balance              int64  `db:"balance"`
	TransactionID        string `db:"trx_id"`
	ProcessDate          string `db:"process_date"`
	Dorc                 string `db:"dorc"`
	RefType              string `db:"ref_type"`
	RefID                int64  `db:"ref_id"`
	TellerID             string `db:"teller_id"`
	CorresponsiveAccount int64  `db:"corresponsive_account"`
	CorresponsiveName    string `db:"corresponsive_name"`
	Channel              string `db:"channel"`
	ServiceBranchID      int64  `db:"service_branch_id"`
	ServiceBranchName    string `db:"service_branch_name"`
	PMTType              string `db:"pmt_type"`
	SendingBankID        int64  `db:"sending_bank_id"`
	SendingBranchID      int64  `db:"sending_branch_id"`
	SendingBranchName    string `db:"sending_branch_name"`
	ReceivingBankID      int64  `db:"receiving_bank_id"`
	ReceivingBranchID    int64  `db:"receiving_branch_id"`
	ReceivingBranchName  string `db:"receiving_branch_name"`
}

type DepositDetail struct {
	MemberBankID           int64  `json:"member_bank_id"`
	MemberPaymentAccountID int64  `json:"member_payment_account_id"`
	MemberBankCode         string `json:"member_bank_code"`
	MemberAccountNo        int64  `json:"member_account_no"`
	MemberAccountName      string `json:"member_account_name"`
	MemberBankRef          string `json:"member_bank_ref"`
	BankAccountCode        string `json:"bank_account_code"`
	BankAccountBankName    string `json:"bank_account_bank_name"`
	MemberFullName         string `json:"member_full_name"`
	BankRef                string `json:"bank_ref"`
}

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

func RandNumber(n int) int {
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)
	return r.Intn(n)
}

func createDummyOnlineBankDataTable() error {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=dummyDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	query := `CREATE TABLE IF NOT EXISTS online_bank_data (
        id SERIAL PRIMARY KEY,
        currency VARCHAR(3),
        remark VARCHAR(255),
        amount DECIMAL(18, 2),
        balance DECIMAL(18, 2),
        trx_id VARCHAR(20),
        process_date TIMESTAMP,
        dorc CHAR(1),
        ref_type VARCHAR(50),
        ref_id VARCHAR(10),
        teller_id VARCHAR(10),
        corresponsive_account VARCHAR(20),
        corresponsive_name VARCHAR(255),
        channel VARCHAR(100),
        service_branch_id VARCHAR(10),
        service_branch_name VARCHAR(255),
        pmt_type VARCHAR(50),
        sending_bank_id VARCHAR(20),
        sending_branch_id VARCHAR(20),
        sending_branch_name VARCHAR(255),
        receiving_bank_id VARCHAR(20),
        receiving_branch_id VARCHAR(20),
        receiving_branch_name VARCHAR(255)
    )`

	_, err = db.Exec(query)
	if err != nil {
		log.Fatal(err)
	}

	return nil
}

func createDummyOnlineBankData(hoursAgo int) error {
	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=dummyDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	query := `INSERT INTO online_bank_data (
        currency,
        remark,
        amount,
        balance,
        trx_id,
        process_date,
        dorc,
        ref_type,
        ref_id,
        teller_id,
        corresponsive_account,
        corresponsive_name,
        channel,
        service_branch_id,
        service_branch_name,
        pmt_type,
        sending_bank_id,
        sending_branch_id,
        sending_branch_name,
        receiving_bank_id,
        receiving_branch_id,
        receiving_branch_name
    ) VALUES (
        :currency,
        :remark,
        :amount,
        :balance,
        :trx_id,
        :process_date,
        :dorc,
        :ref_type,
        :ref_id,
        :teller_id,
        :corresponsive_account,
        :corresponsive_name,
        :channel,
        :service_branch_id,
        :service_branch_name,
        :pmt_type,
        :sending_bank_id,
        :sending_branch_id,
        :sending_branch_name,
        :receiving_bank_id,
        :receiving_branch_id,
        :receiving_branch_name
    )`

	now := time.Now().UTC()
	randTime := now.Add(-time.Hour * time.Duration(RandNumber(hoursAgo-1)))
	randTime = randTime.Add(-time.Minute * time.Duration(RandNumber(59)))

	dumdum := &DummyOnlineData{
		Currency:             "VND",
		Remark:               RandAlphanumeric(12),
		Amount:               int64(RandNumber(10000)),
		Balance:              int64(RandNumber(10000)),
		TransactionID:        RandAlphanumeric(12),
		ProcessDate:          randTime.Format(time.RFC3339Nano),
		Dorc:                 RandAlphanumeric(1),
		RefType:              RandAlphanumeric(12),
		RefID:                int64(RandNumber(9000000)),
		TellerID:             RandAlphanumeric(10),
		CorresponsiveAccount: int64(RandNumber(90000)),
		CorresponsiveName:    RandAlphanumeric(12),
		Channel:              RandAlphanumeric(12),
		ServiceBranchID:      int64(RandNumber(90000)),
		ServiceBranchName:    RandAlphanumeric(12),
		PMTType:              RandAlphanumeric(12),
		SendingBankID:        int64(RandNumber(90000)),
		SendingBranchID:      int64(RandNumber(90000)),
		SendingBranchName:    RandAlphanumeric(12),
		ReceivingBankID:      int64(RandNumber(90000)),
		ReceivingBranchID:    int64(RandNumber(90000)),
		ReceivingBranchName:  RandAlphanumeric(12),
	}

	_, err = db.NamedExec(db.Rebind(query), dumdum)

	if err != nil {
		log.Fatal(err)
	}

	return nil
}

func insertDeposit(obd OnlineBankData, hoursAgo int) error {
	paymentDB, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=paymentDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer paymentDB.Close()

	now := time.Now().UTC()
	randTime := now.Add(-time.Hour * time.Duration(RandNumber(hoursAgo-1)))
	randTime = randTime.Add(-time.Minute * time.Duration(RandNumber(59)))

	rawSQL := `
        INSERT INTO deposit (
            transaction_id,
            member_id,
            bank_account_id,
            payment_type_code,
            ref_code,
            status,
            login_name,
            currency,
            net_amount,
            gross_amount,
            charge_amount,
            detail,
            create_by,
            create_at,
            update_at
        ) VALUES (
            :transaction_id,
            :member_id,
            :bank_account_id,
            :payment_type_code,
            :ref_code,
            :status,
            :login_name,
            :currency,
            :net_amount,
            :gross_amount,
            :charge_amount,
            :detail,
            :create_by,
            :create_at,
            :update_at
        )
        `

	depositDetail, _ := json.Marshal(DepositDetail{
		MemberBankID:           1,
		MemberPaymentAccountID: 2,
		MemberBankCode:         "",
		MemberAccountNo:        8520123456879,
		MemberAccountName:      "member tester",
		MemberBankRef:          "",
		BankAccountCode:        "VCB-98981",
		BankAccountBankName:    "Vietcombank",
		MemberFullName:         "member tester",
		BankRef:                "TEST543210",
	})

	entity := &Deposit{
		TransactionId:   RandAlphanumeric(12),
		MemberId:        4,
		BankAccountId:   1,
		PaymentTypeCode: "DLBT",
		RefCode:         obd.BankReference,
		Status:          1,
		LoginName:       "member2023",
		Currency:        "VND",
		NetAmount:       obd.NetAmount,
		GrossAmount:     obd.NetAmount,
		ChargeAmount:    0,
		Detail:          depositDetail,
		CreateBy:        "superuser",
		CreateAt:        randTime.Format(time.RFC3339Nano),
		UpdateAt:        randTime.Format(time.RFC3339Nano),
	}

	_, err = paymentDB.NamedExec(rawSQL, entity)
	if err != nil {
		return err
	}

	return nil
}

func createDeposits(percent, hoursAgo int) error {
	dummydb, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=dummyDB sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer dummydb.Close()

	now := time.Now().UTC()

	timeSpan := now.Add(-time.Hour * time.Duration(hoursAgo))

	var OBD []OnlineBankData
	rawSQL := `
        SELECT  trx_id AS bank_reference,
                amount AS net_amount
        FROM online_bank_data 
        WHERE process_date >= ?
    `

	err = dummydb.Select(&OBD, dummydb.Rebind(rawSQL), timeSpan.Format(time.RFC3339Nano))
	if err != nil {
		return err
	}

	fmt.Println("Creating Deposits for ", len(OBD), " online bank data")

	for _, obd := range OBD {
		if RandNumber(100) <= percent {
			insertDeposit(obd, hoursAgo)
		}
	}

	return nil
}

func main() {
	hoursAgo := 3
	dummySize := 500

	err := createDummyOnlineBankDataTable()
	if err != nil {
		log.Fatal(err.Error())
	}

	fmt.Println("Creating ", dummySize, "Dummy Online Bank Data")
	for i := 0; i < dummySize; i++ {
		err = createDummyOnlineBankData(hoursAgo)
		if err != nil {
			log.Fatal(err.Error())
		}
	}

	err = createDeposits(85, hoursAgo)
	if err != nil {
		log.Fatal(err.Error())
	}
}
