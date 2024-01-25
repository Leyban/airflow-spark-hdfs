package random

import (
	"math/rand"
	"time"
)

// Return a one of the values from the given slice n
func RandSelect[T any](n []T) T {
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)
	return n[r.Intn(len(n))]
}

// Create Alpanumeric with with n length
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

// Create String with alphabet characters with n length
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

// Creates a int with [0,n)
func RandInt(n int) int {
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)
	return r.Intn(n)
}

// Creates a int64 with [0,n)
func RandInt64(n int64) int64 {
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)
	return int64(r.Int63n(n))
}

// Creates a Null or calls the function given a percent that it will be nil
func NullableValue[T any](f func(n T) T, n T, nilPercent int) any {
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)
	chance := r.Intn(100)

	if nilPercent > chance {
		return nil
	}

	return f(n)
}
