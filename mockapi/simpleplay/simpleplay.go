package simpleplay

import (
	"encoding/xml"
	"net/http"
)

type HistoryResponse struct {
	ErrorMsgID    string        `xml:"ErrorMsgId"`
	ErrorMsg      string        `xml:"ErrorMsg"`
	BetDetailList BetDetailList `xml:"BetDetailList"`
}

type BetDetailList struct {
	BetDetail []BetDetail `xml:"BetDetail"`
}

type BetDetail struct {
	BetTime       string      `xml:"BetTime"`
	PayoutTime    string      `xml:"PayoutTime"`
	Username      string      `xml:"Username"`
	HostID        int16       `xml:"HostID"`
	Detail        string      `xml:"Detail"`
	GameID        string      `xml:"GameID"`
	Round         int         `xml:"Round"`
	Set           int         `xml:"Set"`
	BetID         int64       `xml:"BetID"`
	BetAmount     float64     `xml:"BetAmount"`
	Rolling       float64     `xml:"Rolling"`
	ResultAmount  float64     `xml:"ResultAmount"`
	Balance       float64     `xml:"Balance"`
	GameType      string      `xml:"GameType"`
	BetType       int         `xml:"BetType"`
	BetSource     int         `xml:"BetSource"`
	TransactionID int64       `xml:"TransactionID"`
	GameResult    interface{} `xml:"GameResult"`
	State         bool        `xml:"State"`
}

func HandleSimplePlay(w http.ResponseWriter, r *http.Request) {
	betDetails := []BetDetail{
		{
			BetTime:       "2023-08-01 12:00:00",
			PayoutTime:    "2023-08-01 13:00:00",
			Username:      "user1",
			HostID:        1,
			Detail:        "Bet on slot machine",
			GameID:        "slot123",
			Round:         1,
			Set:           1,
			BetID:         123456789,
			BetAmount:     100.00,
			Rolling:       50.00,
			ResultAmount:  150.00,
			Balance:       500.00,
			GameType:      "slot",
			BetType:       1,
			BetSource:     1,
			TransactionID: 987654321,
			GameResult:    "Win",
			State:         true,
		},
		{
			BetTime:       "2023-08-01 12:00:00",
			PayoutTime:    "2023-08-01 13:00:00",
			Username:      "user2",
			HostID:        2,
			Detail:        "Bet on slot machine",
			GameID:        "fishing123",
			Round:         1,
			Set:           1,
			BetID:         123456789,
			BetAmount:     100.00,
			Rolling:       50.00,
			ResultAmount:  150.00,
			Balance:       500.00,
			GameType:      "fishing",
			BetType:       1,
			BetSource:     1,
			TransactionID: 987654321,
			GameResult:    "Win",
			State:         true,
		},
	}

	betDetailList := BetDetailList{
		BetDetail: betDetails,
	}

	historyResponse := HistoryResponse{
		ErrorMsgID:    "0",
		ErrorMsg:      "Success",
		BetDetailList: betDetailList,
	}

	xmlData, err := xml.MarshalIndent(historyResponse, "", "  ")
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(xml.Header + string(xmlData)))
}
