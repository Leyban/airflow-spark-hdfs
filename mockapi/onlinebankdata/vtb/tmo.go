package vtb

import (
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/jmoiron/sqlx"
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
	Currency             string  `db:"currency"`
	Remark               string  `db:"remark"`
	Amount               float64 `db:"amount"`
	Balance              float64 `db:"balance"`
	TransactionID        string  `db:"trx_id"`
	ProcessDate          string  `db:"process_date"`
	Dorc                 string  `db:"dorc"`
	RefType              string  `db:"ref_type"`
	RefID                int64   `db:"ref_id"`
	TellerID             string  `db:"teller_id"`
	CorresponsiveAccount int64   `db:"corresponsive_account"`
	CorresponsiveName    string  `db:"corresponsive_name"`
	Channel              string  `db:"channel"`
	ServiceBranchID      int64   `db:"service_branch_id"`
	ServiceBranchName    string  `db:"service_branch_name"`
	PMTType              string  `db:"pmt_type"`
	SendingBankID        int64   `db:"sending_bank_id"`
	SendingBranchID      int64   `db:"sending_branch_id"`
	SendingBranchName    string  `db:"sending_branch_name"`
	ReceivingBankID      int64   `db:"receiving_bank_id"`
	ReceivingBranchID    int64   `db:"receiving_branch_id"`
	ReceivingBranchName  string  `db:"receiving_branch_name"`
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
	Success bool   `json:"success"`
	Message string `json:"message"`
	Data    Data   `json:"data"`
}

func HandleTMO(w http.ResponseWriter, r *http.Request) {
	var query transactionQuery

	err := json.NewDecoder(r.Body).Decode(&query)
	if err != nil {
		if err != nil {
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}
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
        WHERE process_date >= ?
        AND process_date <= ?
    `)

	var count int64
	err = db.Get(&count, db.Rebind(`SELECT count(*) FROM (`+sql.String()+`) as t1`), query.Begin, query.End)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	result.Data.TotalRecords = count

	sql.WriteString(` LIMIT 100 OFFSET ?`)

	err = db.Select(&result.Data.Transactions, db.Rebind(sql.String()), query.Begin, query.End, query.Page)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	jsonResult, err := json.Marshal(result)
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	w.Write(jsonResult)
}
