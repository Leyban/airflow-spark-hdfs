package vtb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"mock/onlinebankdata/vtb/datagen"
	"net/http"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/julienschmidt/httprouter"
)

type transactionQuery struct {
	Username      string `json:"username"`
	Password      string `json:"password"`
	AccountNumber string `json:"accountNumber"`
	Begin         string `json:"begin"`
	End           string `json:"end"`
	Page          int64  `json:"page"`
}

type DummyOnlineData struct {
	Currency             string  `db:"currency" json:"currency"`
	Remark               string  `db:"remark" json:"remark"`
	Amount               float64 `db:"amount" json:"amount"`
	Balance              float64 `db:"balance" json:"balance"`
	TransactionID        string  `db:"trx_id" json:"trxId"`
	ProcessDate          string  `db:"process_date" json:"processDate"`
	Dorc                 string  `db:"dorc" json:"dorc"`
	RefType              string  `db:"ref_type" json:"refType"`
	RefID                int64   `db:"ref_id" json:"refId"`
	TellerID             string  `db:"teller_id" json:"tellerId"`
	CorresponsiveAccount int64   `db:"corresponsive_account" json:"corresponsiveAccount"`
	CorresponsiveName    string  `db:"corresponsive_name" json:"corresponsiveName"`
	Channel              string  `db:"channel" json:"channel"`
	ServiceBranchID      int64   `db:"service_branch_id" json:"serviceBranchId"`
	ServiceBranchName    string  `db:"service_branch_name" json:"serviceBranchName"`
	PMTType              string  `db:"pmt_type" json:"pmtType"`
	SendingBankID        int64   `db:"sending_bank_id" json:"sendingBankId"`
	SendingBranchID      int64   `db:"sending_branch_id" json:"sendingBranchId"`
	SendingBranchName    string  `db:"sending_branch_name" json:"sendingBranchName"`
	ReceivingBankID      int64   `db:"receiving_bank_id" json:"receivingBankId"`
	ReceivingBranchID    int64   `db:"receiving_branch_id" json:"receivingBranchId"`
	ReceivingBranchName  string  `db:"receiving_branch_name" json:"receivingBranchName"`
}

type Data struct {
	Error          bool              `json:"error"`
	AccountNo      string            `json:"accountNo"`
	CurrentPage    int64             `json:"currentPage"`
	NextPage       int64             `json:"nextPage"`
	PageSize       int64             `json:"pageSize"`
	TotalRecords   int64             `json:"totalRecords"`
	WarningMessage bool              `json:"warningMsg"`
	Transactions   []DummyOnlineData `json:"transactions"`
}

type Response struct {
	Success    bool   `json:"success"`
	Message    string `json:"message"`
	Data       Data   `json:"data"`
	FromSource int64  `json:"from_source"`
	UseProxy   bool   `json:"use_proxy"`
}

func formatTime(t string) (string, error) {
	tParsed, err := time.Parse("02/01/2006", t)
	if err != nil {
		return "", err
	}

	tstring := tParsed.Format("1/2/2006")

	return tstring, nil
}

func HandleTMO(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	var query transactionQuery

	err := json.NewDecoder(r.Body).Decode(&query)
	if err != nil {
		fmt.Println(err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	query.End, err = formatTime(query.End)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	query.Begin, err = formatTime(query.Begin)
	if err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	db, err := sqlx.Connect("postgres", "user=postgres password=secret dbname=dummyDB sslmode=disable")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	defer db.Close()

	result := Response{
		Success: true,
		Message: "Ok Na",
		Data: Data{
			Error:        false,
			AccountNo:    query.AccountNumber,
			CurrentPage:  query.Page,
			PageSize:     100,
			Transactions: make([]DummyOnlineData, 0),
		},
	}

	var sql bytes.Buffer

	sql.WriteString(`
        SELECT  
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
        FROM online_bank_data
        WHERE CAST ( process_date as DATE) >= ?
        AND CAST ( process_date as DATE) <= ?
    `)

	// fmt.Println(`SELECT count(*) FROM (`+sql.String()+`) as t1`, query.Begin, query.End)
	var count int64
	err = db.Get(&count, db.Rebind(`SELECT count(*) FROM (`+sql.String()+`) as t1`), query.Begin, query.End)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if count == 0 {
		err := datagen.GenerateData(300, query.Begin, query.End)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		err = db.Get(&count, db.Rebind(`SELECT count(*) FROM (`+sql.String()+`) as t1`), query.Begin, query.End)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	result.Data.TotalRecords = count

	sql.WriteString(` LIMIT 100 OFFSET ?`)

	err = db.Select(&result.Data.Transactions, db.Rebind(sql.String()), query.Begin, query.End, query.Page)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	for i, t := range result.Data.Transactions {
		uglyTime, _ := time.Parse(time.RFC3339Nano, t.ProcessDate)
		result.Data.Transactions[i].ProcessDate = uglyTime.Format("01-02-2006 15:04:05")
	}

	jsonResult, err := json.Marshal(result)
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	w.Write(jsonResult)
}
