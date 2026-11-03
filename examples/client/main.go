package main

import (
	"fmt"
	"os"
	"os/signal"

	tradingview "github.com/condrove10/tradingview-scraper"
)

func main() {
	client := tradingview.NewClient(
		tradingview.WithErrorHandler(func(err error, context string) {
			fmt.Println("error:", context, err)
		}),
	)
	defer client.Close()

	// Two listeners on the same market share one server subscription.
	for _, name := range []string{"logger", "strategy"} {
		name := name
		_, err := client.SubscribeQuotes("BINANCE:BTCUSDT", func(q *tradingview.QuoteData) {
			if q.Price != nil {
				fmt.Println(name, q.Source, q.Symbol, "price", *q.Price)
			}
		})
		if err != nil {
			panic(err)
		}
	}

	// One-minute candles with the last 100 bars of history.
	_, err := client.SubscribeKlines("BINANCE:ETHUSDT", "1", 100, func(u tradingview.KlineUpdate) {
		if u.Snapshot {
			fmt.Println("history:", len(u.Bars), "bars")
			return
		}
		for _, bar := range u.Bars {
			fmt.Printf("%s %s close=%.2f closed=%v\n", u.Symbol, bar.Time.Format("15:04"), bar.Close, bar.Closed)
		}
	})
	if err != nil {
		panic(err)
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	<-stop
}
