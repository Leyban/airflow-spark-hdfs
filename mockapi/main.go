package main

import (
	"fmt"
	"mock/pgsoft"
	"mock/simpleplay"
	"net/http"
)

func main() {
	http.HandleFunc("/simpleplay", simpleplay.HandleSimplePlay)
	http.HandleFunc("/pg_soft/v2/Bet/GetHistory", pgsoft.HandlePGSoft)
	port := 8800
	fmt.Printf("Server is listening on port %d...\n", port)
	err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
	if err != nil {
		fmt.Println("Error:", err)
	}
}
