# tradingview-scraper

Real-time quotes and candles (klines) from the TradingView WebSocket, for Go.
Any number of listeners can follow the same market over one connection.

This is a fork of [marcos-gonalons/tradingview-scraper](https://github.com/marcos-gonalons/tradingview-scraper)
(Apache-2.0). It is not affiliated with TradingView.

## Installation

```bash
go get github.com/condrove10/tradingview-scraper@latest
```

Requires Go 1.23 or newer.

## Quotes and candles

```go
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
```

Call `Unsubscribe` on the returned `*Subscription` to stop a listener.

### How listeners work

- **Lazy.** `NewClient` does not connect. The first subscription opens the
  connection; `Connect(ctx)` does it explicitly and waits until it is ready.
- **Reference counted.** A market is subscribed on the server when its first
  listener arrives and unsubscribed when its last listener leaves, however
  many listeners come and go in between.
- **Late joiners catch up.** A new quote listener first receives the latest
  value of every field; a new kline listener first receives the current
  history as a snapshot.
- **Independent delivery.** Each listener has its own goroutine and queue.
  Its callbacks run in order and never concurrently with each other, and a
  slow listener cannot hold up the connection or other listeners. When a
  listener's queue is full (1024 events by default, see `WithListenerBuffer`)
  its oldest events are dropped and `ErrListenerOverflow` is reported.
- **Reconnects.** If the connection drops, times out, or the server reports a
  `critical_error` or `protocol_error`, the client reconnects (5s, doubling up
  to 60s, 20 attempts by default; see `WithReconnect`) and subscribes every
  market that still has listeners. Kline listeners then receive a fresh
  snapshot. After the last attempt the client reports `ErrMaxReconnects`; the
  next subscription or `Connect` call tries again.
- Callbacks may call any client method, including `Unsubscribe` and `Close`.
  No callback starts after `Unsubscribe` or `Close` returns; one that is
  already running is not waited for.

### Quotes

`SubscribeQuotes` delivers `*QuoteData` with `Price`, `Volume`, `Bid` and `Ask`.
Each update contains only the fields that changed; the others are `nil`.
`Source` and `Symbol` are the exchange and ticker (`BINANCE`, `BTCUSDT`); for a
symbol without an exchange prefix, such as `AAPL`, `Source` is empty.
`client.Quote(symbol)` returns the latest merged values.

### Candles (klines)

`SubscribeKlines(symbol, interval, count, fn)`:

- `interval` is a TradingView resolution. These were checked against the live
  server: `1`, `3`, `5`, `15`, `30`, `60`, `120`, `240` (minutes), `1D`, `1W`, `1M`.
- `count` is how many bars of history to load when the market is first
  opened. Later listeners on the same symbol and interval share that history.
- The first update has `Snapshot` set and holds the full history. Later updates
  hold the bars that changed. When a new bar starts, the previous bar is sent
  again with `Closed` set. In a snapshot, the last bar is also `Closed` if its
  close time has already passed (for example, the market is shut).
- `client.Klines(symbol, interval)` returns the bars currently held.

Without a login, TradingView allows one candle series per chart session, so the
client opens one chart session per market. On the anonymous tier the history
can take tens of seconds to arrive when TradingView's servers are busy.

### Errors

Errors go to the `WithErrorHandler` callback with a short context string.
A symbol the server rejects is reported as a `*SymbolError` (its listeners
stay registered), server messages as a `*ServerError`.

## Callback API (`Connect`)

The original API is still available. Its callback receives quotes for every
added symbol and is never called concurrently with itself.

```go
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
```

If the first connection attempt fails, `Connect` returns the error and nothing
keeps running in the background. After that, the socket reconnects on its own.

## Options

| Option | Default |
| --- | --- |
| `WithErrorHandler(fn)` | none |
| `WithListenerBuffer(n)` | 1024 events per listener |
| `WithReconnect(delay, maxDelay, maxAttempts)` | 5s, 60s, 20 (0 = forever) |
| `WithReadTimeout(d)` | 20s without data, heartbeat or pong |
| `WithPingInterval(d)` | 10s |
| `WithAuthToken(token)` | `unauthorized_user_token` |
| `WithHost(host)` | `data.tradingview.com` |
| `WithReconnectHost(host)` | `history-data.tradingview.com`, used after 3 failed reconnects |
| `WithBuildTime(s)`, `WithFromParam(s)`, `WithConnectionType(s)` | `date`, `from` and `type` URL parameters |

## Development

```bash
go test -race ./...
```

The tests run against an in-process fake server that enforces the rules seen
on the live one. To run a short check against the real TradingView server:

```bash
TRADINGVIEW_LIVE=1 go test -run TestLive -v .
```

## License

Apache-2.0, see [LICENSE](LICENSE).
