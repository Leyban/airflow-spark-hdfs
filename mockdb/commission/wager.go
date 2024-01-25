package main

import (
	"log"
	"time"

	"commission/wagers"
)

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
		{"allbet_wager", "login_name", false},
		{"asiagaming_wager", "player_name", false},
		{"pgsoft_wager", "player_name", false},
		{"bti_wager", "username", false},
		{"tfgaming_wager", "member_code", false},
		{"evolution_wager", "player_id", false},
	}

	for _, w := range currencyLess {
		members, err := getProductMembers(w.TableName, w.LoginNameCol, w.WithCurrency)
		if err != nil {
			log.Fatal(err)
		}
		print(w.TableName, len(members))
		for _, m := range members {
			CreateDummyMember(m.LoginName, nil)
		}
	}

	// withCurrency := []Wager{
	// 	{"allbet_wager", "login_name", true},
	// 	{"asiagaming_wager", "player_name", true},
	// 	{"pgsoft_wager", "player_name", true},
	// 	{"bti_wager", "username", true},
	// 	{"tfgaming_wager", "member_code", true},
	// 	{"evolution_wager", "player_id", true},
	// }

	// for _, w := range withCurrency {
	// 	members, err := getProductMembers(w.TableName, w.LoginNameCol, w.WithCurrency)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// 	print(w.TableName, len(members))
	// 	for _, m := range members {
	// 		CreateDummyMember(m.LoginName, &m.Currency)
	// 	}
	// }

	// digitainTable := Wager{TableName: "digitain_order_wager", LoginNameCol: "partner_client_id"}

}

func copyWagersToMonth(timeEnd time.Time) {
	// wagers.CopyAllBet(timeEnd)
	// wagers.CopyAGData(timeEnd)
	// wagers.CopyBtiData(timeEnd)
	// wagers.CopyEbet(timeEnd)
	// wagers.CopyEvolution(timeEnd)
	// wagers.CopyGenesis(timeEnd)
	// wagers.CopySaba(timeEnd)
	// wagers.CopySabaNumber(timeEnd)
	// wagers.CopySabaVirtual(timeEnd)
	// wagers.CopySabacv(timeEnd)
	// wagers.CopySagaming(timeEnd)
	// wagers.CopySimpleplay(timeEnd)
	// wagers.CopyTfGaming(timeEnd)
	wagers.CopyWeworld(timeEnd)
}
