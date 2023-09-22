package main

import (
	"fmt"
	"mock/onlinebankdata/vtb"
	"mock/pgsoft"
	"mock/simpleplay"
	"net/http"
)

func main() {
	http.HandleFunc("/simpleplay", simpleplay.HandleSimplePlay)
	http.HandleFunc("/pg_soft/v2/Bet/GetHistory", pgsoft.HandlePGSoft)
	http.HandleFunc("/VTB", vtb.HandleTMO)

	port := 8800
	fmt.Printf("Server is listening on port %d...\n", port)

	err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
	if err != nil {
		fmt.Println("Error:", err)
	}
}
