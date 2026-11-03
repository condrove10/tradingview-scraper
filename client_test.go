package tradingview

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestURLHasQueryAndNoHostOverride(t *testing.T) {
	f := newFakeTV(t)
	c := NewClient(f.testOptions(WithBuildTime("2025_01_01-00_00"), WithFromParam("chart/"), WithConnectionType("main"))...)
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	r := f.requests[0]
	f.mu.Unlock()
	want := url.Values{"from": {"chart/"}, "date": {"2025_01_01-00_00"}, "type": {"main"}}
	if got := r.URL.Query(); got.Encode() != want.Encode() {
		t.Errorf("query = %q, want %q", got.Encode(), want.Encode())
	}
	if r.Host != f.addr() {
		t.Errorf("Host header = %q, want the dialed host %q", r.Host, f.addr())
	}
	if got := r.Header.Get("Origin"); got != "https://www.tradingview.com" {
		t.Errorf("Origin = %q", got)
	}
}

func TestLazySharedQuoteListeners(t *testing.T) {
	f := newFakeTV(t)
	c := NewClient(f.testOptions()...)
	defer c.Close()

	time.Sleep(50 * time.Millisecond)
	if n := f.requestCount(); n != 0 {
		t.Fatalf("client dialed %d times before any listener", n)
	}

	const sym = "BINANCE:BTCUSDT"
	a, b, late := newRecorder[*QuoteData](), newRecorder[*QuoteData](), newRecorder[*QuoteData]()
	subA, err := c.SubscribeQuotes(sym, a.add)
	if err != nil {
		t.Fatal(err)
	}
	add := f.waitFrame(t, "quote_add_symbols", isMethod(0, "quote_add_symbols"))
	qs := paramString(add, 0)
	subB, err := c.SubscribeQuotes(sym, b.add)
	if err != nil {
		t.Fatal(err)
	}

	conn := f.conn(t, 0)
	conn.send(qsd(qs, sym, map[string]any{"lp": 1.5, "volume": 10}))
	for _, r := range []*recorder[*QuoteData]{a, b} {
		q := r.next(t, "quote")
		if price(q) != 1.5 || q.Source != "BINANCE" || q.Symbol != "BTCUSDT" || q.Bid != nil {
			t.Fatalf("quote = %+v", q)
		}
	}
	conn.send(qsd(qs, sym, map[string]any{"bid": 1.4}))
	a.next(t, "bid update")
	b.next(t, "bid update")

	// A late listener starts from the merged state.
	subLate, err := c.SubscribeQuotes(sym, late.add)
	if err != nil {
		t.Fatal(err)
	}
	snap := late.next(t, "snapshot")
	if price(snap) != 1.5 || snap.Bid == nil || *snap.Bid != 1.4 || snap.Volume == nil || *snap.Volume != 10 {
		t.Fatalf("late snapshot = %+v", snap)
	}

	// Leaving listeners only unsubscribe on the server when the last leaves.
	if err := subA.Unsubscribe(); err != nil {
		t.Fatal(err)
	}
	if err := subB.Unsubscribe(); err != nil {
		t.Fatal(err)
	}
	for _, fr := range f.drainFrames(100 * time.Millisecond) {
		if fr.method == "quote_remove_symbols" || fr.method == "quote_add_symbols" {
			t.Fatalf("unexpected %s while a listener remains", fr.method)
		}
	}
	if err := subLate.Unsubscribe(); err != nil {
		t.Fatal(err)
	}
	rm := f.waitFrame(t, "quote_remove_symbols", isMethod(0, "quote_remove_symbols"))
	if paramString(rm, 1) != sym {
		t.Fatalf("removed %v", rm.params)
	}
	if err := subLate.Unsubscribe(); err != nil { // idempotent
		t.Fatal(err)
	}
	conn.send(qsd(qs, sym, map[string]any{"lp": 2.0}))
	a.none(t, 100*time.Millisecond, "quote after unsubscribe")
	f.checkNoViolations(t)
}

func TestHeartbeatEchoesOnlyTheHeartbeatFrame(t *testing.T) {
	f := newFakeTV(t)
	c := NewClient(f.testOptions()...)
	defer c.Close()
	r := newRecorder[*QuoteData]()
	if _, err := c.SubscribeQuotes("EURONEXT:GLE", r.add); err != nil {
		t.Fatal(err)
	}
	qs := paramString(f.waitFrame(t, "quote_add_symbols", isMethod(0, "quote_add_symbols")), 0)
	conn := f.conn(t, 0)

	// The live server packs data and a heartbeat into one message.
	conn.send(qsd(qs, "EURONEXT:GLE", map[string]any{"lp": 61.07}), conn.heartbeat(2))
	reply := f.waitFrame(t, "heartbeat reply", func(fr fakeFrame) bool { return fr.method != "" })
	if reply.payload != "~h~2" {
		t.Fatalf("client answered the heartbeat with %q, want only ~h~2", reply.payload)
	}
	if q := r.next(t, "quote"); price(q) != 61.07 {
		t.Fatalf("quote = %+v", q)
	}
	f.checkNoViolations(t)
}

func TestReconnectRestoresAllMarkets(t *testing.T) {
	f := newFakeTV(t)
	errs, onErr := errorRecorder()
	c := NewClient(f.testOptions(onErr)...)
	defer c.Close()

	quotes, klines := newRecorder[*QuoteData](), newRecorder[KlineUpdate]()
	if _, err := c.SubscribeQuotes("AAPL", quotes.add); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SubscribeKlines("BINANCE:ETHUSDT", "5", 2, klines.add); err != nil {
		t.Fatal(err)
	}
	qs0 := paramString(f.waitFrame(t, "quote_add_symbols", isMethod(0, "quote_add_symbols")), 0)
	cs0 := paramString(f.waitFrame(t, "create_series", isMethod(0, "create_series")), 0)
	conn0 := f.conn(t, 0)
	conn0.send(seriesMsg("timescale_update", cs0, 1e12, bar(300, 1, 2, 0.5, 1.5, 7), bar(600, 1.5, 3, 1, 2, 8)))
	if u := klines.next(t, "history"); !u.Snapshot || len(u.Bars) != 2 {
		t.Fatalf("history = %+v", u)
	}

	_ = conn0.ws.Close() // the server goes away
	if e := errs.next(t, "read error"); e.context != ReadMessageErrorContext {
		t.Fatalf("error context = %q (%v)", e.context, e.err)
	}

	add := f.waitFrame(t, "quote_add_symbols after reconnect", isMethod(1, "quote_add_symbols"))
	if len(add.params) != 2 || paramString(add, 1) != "AAPL" {
		t.Fatalf("resubscribed with params %v, want [session AAPL]", add.params)
	}
	qs1 := paramString(add, 0)
	if qs1 == qs0 {
		t.Fatalf("quote session id reused after reconnect: %s", qs1)
	}
	cs1 := paramString(f.waitFrame(t, "create_series after reconnect", isMethod(1, "create_series")), 0)

	conn1 := f.conn(t, 1)
	conn1.send(qsd(qs1, "AAPL", map[string]any{"lp": 330.0}))
	if q := quotes.next(t, "quote after reconnect"); price(q) != 330 || q.Symbol != "AAPL" || q.Source != "" {
		t.Fatalf("quote = %+v", q)
	}
	conn1.send(seriesMsg("timescale_update", cs1, 1e12, bar(600, 1.5, 3, 1, 2.5, 9)))
	if u := klines.next(t, "history after reconnect"); !u.Snapshot || len(u.Bars) != 1 || u.Bars[0].Close != 2.5 {
		t.Fatalf("history after reconnect = %+v", u)
	}

	time.Sleep(100 * time.Millisecond)
	if n := f.connCount(); n != 2 {
		t.Fatalf("%d connections, want exactly one reconnect", n)
	}
	f.checkNoViolations(t)
}

func TestServerCriticalErrorReconnects(t *testing.T) {
	f := newFakeTV(t)
	errs, onErr := errorRecorder()
	c := NewClient(f.testOptions(onErr)...)
	defer c.Close()
	r := newRecorder[*QuoteData]()
	if _, err := c.SubscribeQuotes("BINANCE:BTCUSDT", r.add); err != nil {
		t.Fatal(err)
	}
	qs0 := paramString(f.waitFrame(t, "add", isMethod(0, "quote_add_symbols")), 0)
	f.conn(t, 0).sendMsg("critical_error", qs0, "invalid_method", "qsd")

	e := errs.next(t, "server error")
	var se *ServerError
	if !errors.As(e.err, &se) || se.Method != "critical_error" || e.context != ServerErrorMessageContext {
		t.Fatalf("error = %v (%s)", e.err, e.context)
	}
	qs1 := paramString(f.waitFrame(t, "add after reconnect", isMethod(1, "quote_add_symbols")), 0)
	f.conn(t, 1).send(qsd(qs1, "BINANCE:BTCUSDT", map[string]any{"lp": 7.0}))
	if q := r.next(t, "quote"); price(q) != 7 {
		t.Fatalf("quote = %+v", q)
	}
	f.checkNoViolations(t)
}

func TestKlineLifecycle(t *testing.T) {
	f := newFakeTV(t)
	c := NewClient(f.testOptions()...)
	defer c.Close()

	btc, eth := newRecorder[KlineUpdate](), newRecorder[KlineUpdate]()
	subBTC, err := c.SubscribeKlines("BINANCE:BTCUSDT", "1", 3, btc.add)
	if err != nil {
		t.Fatal(err)
	}
	subETH, err := c.SubscribeKlines("BINANCE:ETHUSDT", "1", 3, eth.add)
	if err != nil {
		t.Fatal(err)
	}
	series := map[string]string{} // symbol -> chart session
	for i := 0; i < 2; i++ {
		fr := f.waitFrame(t, "resolve_symbol", isMethod(0, "resolve_symbol"))
		for _, sym := range []string{"BINANCE:BTCUSDT", "BINANCE:ETHUSDT"} {
			if strings.Contains(paramString(fr, 2), sym) {
				series[sym] = paramString(fr, 0)
			}
		}
	}
	cs := series["BINANCE:BTCUSDT"]
	if cs == "" || cs == series["BINANCE:ETHUSDT"] {
		t.Fatalf("each market needs its own chart session, got %v", series)
	}
	conn := f.conn(t, 0)

	// History: 4 bars arrive, count is 3; the open bar's close time is ahead.
	future := time.Now().Add(time.Hour).Unix()
	conn.send(seriesMsg("timescale_update", cs, future,
		bar(60, 1, 1, 1, 1, 1), bar(120, 2, 2, 2, 2, 2), bar(180, 3, 3, 3, 3, 3), bar(240, 4, 4, 4, 4, 4)))
	u := btc.next(t, "history")
	if !u.Snapshot || u.Symbol != "BINANCE:BTCUSDT" || u.Interval != "1" || len(u.Bars) != 3 {
		t.Fatalf("history = %+v", u)
	}
	if u.Bars[0].Time.Unix() != 120 || !u.Bars[0].Closed || !u.Bars[1].Closed || u.Bars[2].Closed {
		t.Fatalf("history bars = %+v", u.Bars)
	}

	// The open bar changes.
	conn.send(seriesMsg("du", cs, future, bar(240, 4, 5, 4, 4.5, 6)))
	u = btc.next(t, "update")
	if u.Snapshot || len(u.Bars) != 1 || u.Bars[0].Close != 4.5 || u.Bars[0].Closed {
		t.Fatalf("update = %+v", u)
	}

	// A new bar closes the previous one.
	conn.send(seriesMsg("du", cs, future, bar(300, 4.5, 4.5, 4.5, 4.5, 1)))
	u = btc.next(t, "new bar")
	if len(u.Bars) != 2 || u.Bars[0].Time.Unix() != 240 || !u.Bars[0].Closed || u.Bars[1].Time.Unix() != 300 || u.Bars[1].Closed {
		t.Fatalf("new bar update = %+v", u.Bars)
	}
	bars, ok := c.Klines("BINANCE:BTCUSDT", "1")
	if !ok || len(bars) != 3 || bars[0].Time.Unix() != 180 || bars[2].Time.Unix() != 300 {
		t.Fatalf("Klines = %+v", bars)
	}

	// A second listener on the same market shares the series.
	late := newRecorder[KlineUpdate]()
	subLate, err := c.SubscribeKlines("BINANCE:BTCUSDT", "1", 50, late.add)
	if err != nil {
		t.Fatal(err)
	}
	if u := late.next(t, "late snapshot"); !u.Snapshot || len(u.Bars) != 3 {
		t.Fatalf("late snapshot = %+v", u)
	}
	eth.none(t, 50*time.Millisecond, "ETH update from BTC data")

	if err := subBTC.Unsubscribe(); err != nil {
		t.Fatal(err)
	}
	if err := subLate.Unsubscribe(); err != nil {
		t.Fatal(err)
	}
	del := f.waitFrame(t, "chart_delete_session", isMethod(0, "chart_delete_session"))
	if paramString(del, 0) != cs {
		t.Fatalf("deleted %v, want %s", del.params, cs)
	}
	if err := subETH.Unsubscribe(); err != nil {
		t.Fatal(err)
	}
	for _, fr := range f.drainFrames(100 * time.Millisecond) {
		if fr.method == "create_series" {
			t.Fatalf("series created again: %v", fr.params)
		}
	}
	f.checkNoViolations(t)
}

func TestSymbolErrorsAreReported(t *testing.T) {
	f := newFakeTV(t)
	errs, onErr := errorRecorder()
	c := NewClient(f.testOptions(onErr)...)
	defer c.Close()
	noop := func(*QuoteData) {}
	if _, err := c.SubscribeQuotes("NOTAREAL:XXXX", noop); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SubscribeKlines("NOTAREAL:XXXX", "5", 2, func(KlineUpdate) {}); err != nil {
		t.Fatal(err)
	}
	qs := paramString(f.waitFrame(t, "add", isMethod(0, "quote_add_symbols")), 0)
	cs := paramString(f.waitFrame(t, "series", isMethod(0, "create_series")), 0)
	conn := f.conn(t, 0)
	conn.sendMsg("qsd", qs, map[string]any{"n": "NOTAREAL:XXXX", "s": "error", "errmsg": "no_such_symbol", "v": map[string]any{}})
	conn.sendMsg("symbol_error", cs, "sds_sym_1", "invalid symbol")

	for _, want := range []string{"tradingview: symbol NOTAREAL:XXXX: no_such_symbol", "tradingview: symbol NOTAREAL:XXXX (5): invalid symbol"} {
		e := errs.next(t, want)
		var se *SymbolError
		if !errors.As(e.err, &se) || e.err.Error() != want || e.context != SymbolErrorContext {
			t.Fatalf("error = %v (%s), want %s", e.err, e.context, want)
		}
	}
	if f.connCount() != 1 {
		t.Fatalf("symbol errors must not reconnect")
	}
}

func TestSlowListenerDoesNotBlockOthers(t *testing.T) {
	f := newFakeTV(t)
	errs, onErr := errorRecorder()
	c := NewClient(f.testOptions(onErr, WithListenerBuffer(4))...)
	defer c.Close()

	release := make(chan struct{})
	var slowMu sync.Mutex
	var slowSeen []float64
	slow := func(q *QuoteData) {
		<-release
		slowMu.Lock()
		slowSeen = append(slowSeen, price(q))
		slowMu.Unlock()
	}
	fast := newRecorder[*QuoteData]()
	if _, err := c.SubscribeQuotes("X:Y", slow); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SubscribeQuotes("X:Y", fast.add); err != nil {
		t.Fatal(err)
	}
	qs := paramString(f.waitFrame(t, "add", isMethod(0, "quote_add_symbols")), 0)
	conn := f.conn(t, 0)
	const n = 20
	for i := 1; i <= n; i++ {
		conn.send(qsd(qs, "X:Y", map[string]any{"lp": float64(i)}))
		// fast must keep up even though slow is stuck
		if q := fast.next(t, "fast quote"); price(q) != float64(i) {
			t.Fatalf("fast listener got %v, want %d", price(q), i)
		}
	}
	conn.send(conn.heartbeat(9))
	if fr := f.waitFrame(t, "heartbeat", isMethod(0, "heartbeat")); fr.payload != "~h~9" {
		t.Fatalf("heartbeat reply %q", fr.payload)
	}
	e := errs.next(t, "overflow")
	if !errors.Is(e.err, ErrListenerOverflow) || e.context != ListenerOverflowErrorContext {
		t.Fatalf("error = %v (%s)", e.err, e.context)
	}
	close(release)
	deadline := time.Now().Add(2 * time.Second)
	for {
		slowMu.Lock()
		got := append([]float64(nil), slowSeen...)
		slowMu.Unlock()
		if len(got) > 0 && got[len(got)-1] == n {
			if len(got) > 5 {
				t.Fatalf("slow listener got %d events, buffer is 4 (+1 in flight): %v", len(got), got)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("slow listener never got the latest quote: %v", got)
		}
		time.Sleep(5 * time.Millisecond)
	}
	f.checkNoViolations(t)
}

func TestCloseIsFinal(t *testing.T) {
	f := newFakeTV(t)
	c := NewClient(f.testOptions()...)
	r := newRecorder[*QuoteData]()
	sub, err := c.SubscribeQuotes("A:B", r.add)
	if err != nil {
		t.Fatal(err)
	}
	f.waitFrame(t, "add", isMethod(0, "quote_add_symbols"))
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SubscribeQuotes("A:B", r.add); !errors.Is(err, ErrClosed) {
		t.Fatalf("subscribe after close: %v", err)
	}
	if err := c.Connect(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("connect after close: %v", err)
	}
	if err := sub.Unsubscribe(); err != nil {
		t.Fatalf("unsubscribe after close: %v", err)
	}
	time.Sleep(150 * time.Millisecond) // several reconnect delays
	if n := f.requestCount(); n != 1 {
		t.Fatalf("%d dials, want no reconnect after Close", n)
	}
}

func TestCloseAndUnsubscribeFromCallback(t *testing.T) {
	f := newFakeTV(t)
	c := NewClient(f.testOptions()...)
	done := make(chan struct{})
	var sub *Subscription
	var once sync.Once
	ready := make(chan struct{})
	var err error
	sub, err = c.SubscribeQuotes("A:B", func(*QuoteData) {
		<-ready
		once.Do(func() {
			_ = sub.Unsubscribe()
			_ = c.Close()
			close(done)
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	close(ready)
	qs := paramString(f.waitFrame(t, "add", isMethod(0, "quote_add_symbols")), 0)
	f.conn(t, 0).send(qsd(qs, "A:B", map[string]any{"lp": 1}))
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Unsubscribe/Close from a callback deadlocked")
	}
}

func TestRedirect(t *testing.T) {
	target := newFakeTV(t)
	origin := newFakeTV(t)
	origin.hello = func(int) string { return fmt.Sprintf(`{"redirect":%q}`, target.addr()) }
	c := NewClient(origin.testOptions()...)
	defer c.Close()
	if _, err := c.SubscribeQuotes("A:B", func(*QuoteData) {}); err != nil {
		t.Fatal(err)
	}
	target.waitFrame(t, "add on redirect target", isMethod(0, "quote_add_symbols"))
	if n := origin.connCount(); n != 1 {
		t.Fatalf("origin got %d connections", n)
	}
	target.checkNoViolations(t)
	origin.checkNoViolations(t)
}

func TestReconnectGivesUpAndRecovers(t *testing.T) {
	f := newFakeTV(t)
	f.mu.Lock()
	f.reject = true
	f.mu.Unlock()
	errs, onErr := errorRecorder()
	c := NewClient(f.testOptions(onErr, WithReconnect(time.Millisecond, 2*time.Millisecond, 3))...)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Connect(ctx); !errors.Is(err, ErrMaxReconnects) {
		t.Fatalf("Connect = %v, want ErrMaxReconnects", err)
	}
	if n := f.requestCount(); n != 4 {
		t.Fatalf("%d dials, want 1 + 3 reconnects", n)
	}
	for {
		if e := errs.next(t, "max reconnect"); e.context == MaxReconnectErrorContext {
			break
		}
	}
	f.mu.Lock()
	f.reject = false
	f.mu.Unlock()
	if err := c.Connect(ctx); err != nil {
		t.Fatalf("Connect after recovery: %v", err)
	}
}

// TestConcurrentListeners hammers the client from many goroutines while the
// server streams quotes and heartbeats. Run with -race.
func TestConcurrentListeners(t *testing.T) {
	f := newFakeTV(t)
	c := NewClient(f.testOptions()...)
	defer c.Close()
	symbols := []string{"S:A", "S:B", "S:C", "S:D"}

	// Hold one listener so the quote session stays known to the test.
	if _, err := c.SubscribeQuotes("S:HOLD", func(*QuoteData) {}); err != nil {
		t.Fatal(err)
	}
	qs := paramString(f.waitFrame(t, "add", isMethod(0, "quote_add_symbols")), 0)
	conn := f.conn(t, 0)

	stop := make(chan struct{})
	var feed sync.WaitGroup
	feed.Add(1)
	go func() {
		defer feed.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			conn.send(qsd(qs, symbols[i%len(symbols)], map[string]any{"lp": float64(i)}), conn.heartbeat(i))
			time.Sleep(200 * time.Microsecond)
		}
	}()

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed, 1))
			for i := 0; i < 40; i++ {
				sub, err := c.SubscribeQuotes(symbols[rng.IntN(len(symbols))], func(*QuoteData) {})
				if err != nil {
					t.Error(err)
					return
				}
				_, _ = c.Quote("S:A")
				_ = c.PingInfo()
				if rng.IntN(2) == 0 {
					time.Sleep(time.Duration(rng.IntN(300)) * time.Microsecond)
				}
				if err := sub.Unsubscribe(); err != nil {
					t.Error(err)
				}
			}
		}(uint64(g))
	}
	wg.Wait()
	close(stop)
	feed.Wait()

	f.drainFrames(200 * time.Millisecond)
	if got := conn.symbols(); len(got) != 1 || !got["S:HOLD"] {
		t.Fatalf("server still has %v subscribed, want only S:HOLD", got)
	}
	if n := f.connCount(); n != 1 {
		t.Fatalf("%d connections, want 1", n)
	}
	f.checkNoViolations(t)
}

func TestNoGoroutineLeakAfterClose(t *testing.T) {
	f := newFakeTV(t)
	before := runtime.NumGoroutine()
	for i := 0; i < 5; i++ {
		c := NewClient(f.testOptions()...)
		if _, err := c.SubscribeQuotes("A:B", func(*QuoteData) {}); err != nil {
			t.Fatal(err)
		}
		if _, err := c.SubscribeKlines("A:B", "1", 2, func(KlineUpdate) {}); err != nil {
			t.Fatal(err)
		}
		f.waitFrame(t, "series", isMethod(i, "create_series"))
		_ = c.Close()
	}
	f.close()
	deadline := time.Now().Add(3 * time.Second)
	for runtime.NumGoroutine() > before+2 {
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<16)
			t.Fatalf("goroutines: before %d, after %d\n%s", before, runtime.NumGoroutine(), buf[:runtime.Stack(buf, true)])
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestStaleConnectionEventsAreIgnored injects a read error from an earlier
// connection; it must not tear down the current one.
func TestStaleConnectionEventsAreIgnored(t *testing.T) {
	f := newFakeTV(t)
	errs, onErr := errorRecorder()
	c := NewClient(f.testOptions(onErr)...)
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	_ = c.call(func() error {
		c.handleEvent(connEvent{gen: c.gen - 1, kind: evReadErr, err: errors.New("stale: use of closed connection")})
		return nil
	})
	errs.none(t, 100*time.Millisecond, "error from a stale connection")
	if err := c.call(func() error {
		if c.state != stateReady {
			return fmt.Errorf("state = %d, want ready", c.state)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if n := f.connCount(); n != 1 {
		t.Fatalf("%d connections, want 1", n)
	}
}

// TestNoCallbackStartsAfterClose: a callback already running may finish after
// Close returns, but no queued callback starts.
func TestNoCallbackStartsAfterClose(t *testing.T) {
	f := newFakeTV(t)
	c := NewClient(f.testOptions()...)
	entered := make(chan struct{}, 10)
	release := make(chan struct{})
	var calls atomic.Int32
	if _, err := c.SubscribeQuotes("A:B", func(*QuoteData) {
		calls.Add(1)
		entered <- struct{}{}
		<-release
	}); err != nil {
		t.Fatal(err)
	}
	qs := paramString(f.waitFrame(t, "add", isMethod(0, "quote_add_symbols")), 0)
	conn := f.conn(t, 0)
	conn.send(qsd(qs, "A:B", map[string]any{"lp": 1}), qsd(qs, "A:B", map[string]any{"lp": 2}))
	<-entered
	time.Sleep(20 * time.Millisecond) // let the second quote reach the queue
	_ = c.Close()
	close(release)
	time.Sleep(50 * time.Millisecond)
	if n := calls.Load(); n != 1 {
		t.Fatalf("%d callbacks, want 1: a queued callback started after Close", n)
	}
}
