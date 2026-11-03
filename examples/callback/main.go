package main

import (
	"fmt"
	"time"

	tradingview "github.com/condrove10/tradingview-scraper"
)

func main() {
	socket, err := tradingview.Connect(
		func(q *tradingview.QuoteData) {
			if q.Price != nil {
				fmt.Println(q.Source, q.Symbol, *q.Price)
			}
		},
		func(err error, context string) {
			fmt.Println("error:", context, err)
		},
	)
	if err != nil {
		panic(err)
	}
	defer socket.Close()

	_ = socket.AddSymbol("OANDA:EURUSD")
	_ = socket.AddSymbol("BITSTAMP:BTCUSD")
	time.Sleep(30 * time.Second)
	_ = socket.RemoveSymbol("OANDA:EURUSD")
}
