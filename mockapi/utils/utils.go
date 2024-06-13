package utils

import (
	"fmt"
	"net/http"
)

func PrintHeaders(r *http.Request) {
	for name, values := range r.Header {
		// Loop over all values for the name.
		for _, value := range values {
			fmt.Println(name, value)
		}
	}
}
