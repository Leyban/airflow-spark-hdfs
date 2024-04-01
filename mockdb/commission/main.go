package main

import (
	. "commission/random"
	"fmt"
	"log"
	"time"
)

var memCount int64
var memAccCount int64
var affCount int64
var depoCount int64
var withdrawCount int64
var adjCount int64

func copyMembersToPaymentDB() {
	members := getMembers()

	for _, m := range members {
		createMemberAccount(m)
	}
}

func generateDeposits(memberID int64, timeEnd time.Time) {
	howMany := RandInt(12)

	for i := 1; i <= howMany-2; i++ {
		createDummyDeposits(memberID, timeEnd)
	}
}

func generateWithdrawals(memberID int64, timeEnd time.Time) {
	howMany := RandInt(12)

	for i := 1; i <= howMany-2; i++ {
		createDummyWithdrawal(memberID, timeEnd)
	}
}

func generateAdjustments(memberID int64, timeEnd time.Time) {
	howMany := RandInt(12)

	for i := 1; i <= howMany-2; i++ {
		createDummyAdjustment(memberID, timeEnd)
	}
}

func generateTransactionMembers(timeEnd time.Time) {

	affiliateCount := 100
	memberCount := 1000

	for i := 1; i <= affiliateCount; i++ {
		createDummyAffiliate(int64(i))
	}

	for i := 1; i <= memberCount; i++ {
		loginName := RandString(16)
		memId, err := CreateDummyMember(loginName, nil)
		if err != nil {
			log.Fatal(err)
		}

		generateDeposits(memId, timeEnd)
		generateWithdrawals(memId, timeEnd)
		generateAdjustments(memId, timeEnd)
	}
}

func generateAffAdjustments(timeEnd time.Time) {
	affiliateCount := 100
	adjustmentCount := 100

	for affId := affiliateCount; affId >= 771; affId-- {
		for i := 0; i <= adjustmentCount; i++ {
			err := createDummyAffAdjustment(int64(affId), timeEnd)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

func generateTransactionsExistingMembers(timeEnd time.Time) {

	memIds := GetMemberIds()

	fmt.Println("Member Count: ", len(memIds))

	for _, id := range memIds {
		generateDeposits(id, timeEnd)
		generateWithdrawals(id, timeEnd)
		generateAdjustments(id, timeEnd)
	}

}

func main() {
	timeEnd := time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC)

	// generateWagerMembers()
	// generateTransactionMembers(timeEnd)

	// generateTransactionsExistingMembers(timeEnd)

	// copyMembersToPaymentDB()

	copyWagersToMonth(timeEnd)

	generateAffAdjustments(timeEnd)
}
