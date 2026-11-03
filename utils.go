package tradingview

import (
	"encoding/json"
	"math/rand/v2"
)

// GetRandomString returns length random ASCII letters. It is safe for
// concurrent use.
func GetRandomString(length int) string {
	const characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	if length <= 0 {
		return ""
	}
	b := make([]byte, length)
	for i := range b {
		b[i] = characters[rand.IntN(len(characters))]
	}
	return string(b)
}

// GetStringRepresentation returns data as JSON, or "" if it cannot be encoded.
func GetStringRepresentation(data interface{}) string {
	str, _ := json.Marshal(data)
	return string(str)
}
