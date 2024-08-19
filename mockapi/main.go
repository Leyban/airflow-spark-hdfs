package main

import (
	"fmt"
	"mock/bti"
	"mock/genesis"
	"mock/onlinebankdata/vtb"
	"mock/pgsoft"
	"mock/simpleplay"
	"mock/tfgaming"
	"net/http"

	"github.com/julienschmidt/httprouter"
)

func main() {
	// vendors
	r := httprouter.New()
	r.GET("/genesis", genesis.HandleGenesis)
	r.GET("/tfgaming/:currency/api/v2/bet-transaction/", tfgaming.HandleTFGaming)
	r.POST("/bti", bti.HandleBti)
	r.POST("/bti/token/authorize_v2", bti.HandleBtiToken)
	r.POST("/simpleplay", simpleplay.HandleSimplePlay)
	r.POST("/pg_soft/v2/Bet/GetHistory", pgsoft.HandlePGSoft)

	// bank
	r.GET("/VTB", vtb.HandleTMO)

	port := 8800
	fmt.Printf("Server is listening on port %d...\n", port)

	err := http.ListenAndServe(fmt.Sprintf(":%d", port), r)
	if err != nil {
		fmt.Println("Error:", err)
	}
}
