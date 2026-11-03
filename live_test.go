package tradingview

import (
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

// TestLive runs against the real TradingView server. It is skipped unless
// TRADINGVIEW_LIVE=1; TRADINGVIEW_LIVE_SECONDS sets how long it listens
// (default 20, use 75+ to see a 1-minute candle close).
//
//	TRADINGVIEW_LIVE=1 go test -run TestLive -v .
func TestLive(t *testing.T) {
	if os.Getenv("TRADINGVIEW_LIVE") != "1" {
		t.Skip("set TRADINGVIEW_LIVE=1 to run against the live server")
	}
	seconds := 20
	if v, err := strconv.Atoi(os.Getenv("TRADINGVIEW_LIVE_SECONDS")); err == nil && v > 0 {
		seconds = v
	}
	var mu sync.Mutex
	var errs []string
	c := NewClient(WithErrorHandler(func(err error, context string) {
		mu.Lock()
		errs = append(errs, context+": "+err.Error())
		mu.Unlock()
	}))
	defer c.Close()

	// Callbacks may still run after the test returns (Close does not wait for
	// a running callback), so they stop logging once stopped is set.
	var quotesA, quotesB, snapshots, updates, closed int
	var lastBar Kline
	var stopped bool
	var countMu sync.Mutex
	count := func(p *int) { countMu.Lock(); *p++; countMu.Unlock() }
	for _, sym := range []string{"BINANCE:BTCUSDT", "AAPL"} {
		if _, err := c.SubscribeQuotes(sym, func(*QuoteData) { count(&quotesA) }); err != nil {
			t.Fatal(err)
		}
	}
	// Second listener on the same market shares the subscription.
	if _, err := c.SubscribeQuotes("BINANCE:BTCUSDT", func(*QuoteData) { count(&quotesB) }); err != nil {
		t.Fatal(err)
	}
	for _, sym := range []string{"BINANCE:BTCUSDT", "BINANCE:ETHUSDT"} {
		_, err := c.SubscribeKlines(sym, "1", 5, func(u KlineUpdate) {
			countMu.Lock()
			defer countMu.Unlock()
			if stopped {
				return
			}
			if u.Snapshot {
				snapshots++
				t.Logf("snapshot %s %s: %d bars", u.Symbol, u.Interval, len(u.Bars))
				return
			}
			updates++
			for _, b := range u.Bars {
				if b.Closed {
					closed++
					t.Logf("closed bar %s: %+v", u.Symbol, b)
				}
			}
			lastBar = u.Bars[len(u.Bars)-1]
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(time.Duration(seconds) * time.Second)

	countMu.Lock()
	stopped = true
	defer countMu.Unlock()
	mu.Lock()
	defer mu.Unlock()
	t.Logf("quotes listener A=%d B=%d, kline snapshots=%d updates=%d closed=%d last=%+v ping=%+v session=%s",
		quotesA, quotesB, snapshots, updates, closed, lastBar, c.PingInfo(), c.SessionID())
	for _, e := range errs {
		t.Logf("error: %s", e)
	}
	if quotesA == 0 || quotesB == 0 || len(errs) != 0 {
		t.Fatalf("live run incomplete")
	}
	if snapshots != 2 {
		// Anonymous candle history sometimes takes 20-45s or longer to arrive
		// (seen with a bare WebSocket client too), so this only warns.
		t.Logf("warning: %d of 2 candle histories arrived; TradingView may be slow", snapshots)
	}
}
