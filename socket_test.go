package tradingview

import (
	"errors"
	"sort"
	"sync/atomic"
	"testing"
	"time"
)

func TestLegacyConnectAddRemove(t *testing.T) {
	f := newFakeTV(t)
	quotes := newRecorder[*QuoteData]()
	s, err := Connect(quotes.add, func(error, string) {}, f.testOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.AddSymbol("AAPL"); err != nil {
		t.Fatal(err)
	}
	if err := s.AddSymbol("AAPL"); err != nil { // no effect
		t.Fatal(err)
	}
	qs := paramString(f.waitFrame(t, "add", isMethod(0, "quote_add_symbols")), 0)
	if got := s.(*Socket).GetSessionID(); got != qs {
		t.Fatalf("GetSessionID = %q, want %q", got, qs)
	}
	f.conn(t, 0).send(qsd(qs, "AAPL", map[string]any{"lp": 338.32, "volume": 64757691}))
	q := quotes.next(t, "quote")
	if q.Symbol != "AAPL" || q.Source != "" || price(q) != 338.32 || q.Volume == nil {
		t.Fatalf("quote = %+v", q)
	}
	if err := s.RemoveSymbol("AAPL"); err != nil {
		t.Fatal(err)
	}
	f.waitFrame(t, "remove", isMethod(0, "quote_remove_symbols"))
	f.checkNoViolations(t)
}

func TestLegacyAddSymbolBeforeInit(t *testing.T) {
	f := newFakeTV(t)
	s := NewSocket(f.testOptions()...)
	s.OnReceiveMarketDataCallback = func(*QuoteData) {}
	for _, sym := range []string{"OANDA:EURUSD", "BITSTAMP:BTCUSD", "OANDA:EURUSD"} {
		if err := s.AddSymbol(sym); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	add := f.waitFrame(t, "add", isMethod(0, "quote_add_symbols"))
	var got []string
	for i := 1; i < len(add.params); i++ {
		got = append(got, paramString(add, i))
	}
	sort.Strings(got)
	if len(got) != 2 || got[0] != "BITSTAMP:BTCUSD" || got[1] != "OANDA:EURUSD" {
		t.Fatalf("subscribed %v", got)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Init(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Init after Close = %v", err)
	}
	if err := s.AddSymbol("X:Y"); !errors.Is(err, ErrClosed) {
		t.Fatalf("AddSymbol after Close = %v", err)
	}
	f.checkNoViolations(t)
}

func TestLegacyConnectFailureStopsRetrying(t *testing.T) {
	f := newFakeTV(t)
	f.mu.Lock()
	f.reject = true
	f.mu.Unlock()
	s, err := Connect(func(*QuoteData) {}, func(error, string) {}, f.testOptions()...)
	if err == nil || s != nil {
		t.Fatalf("Connect = %v, %v; want an error", s, err)
	}
	time.Sleep(150 * time.Millisecond) // many reconnect delays
	if n := f.requestCount(); n != 1 {
		t.Fatalf("%d dials after a failed Connect, want 1", n)
	}
}

func TestLegacyCallbackIsNeverConcurrent(t *testing.T) {
	f := newFakeTV(t)
	var inside, overlaps, calls atomic.Int32
	s, err := Connect(func(*QuoteData) {
		if inside.Add(1) > 1 {
			overlaps.Add(1)
		}
		time.Sleep(50 * time.Microsecond)
		inside.Add(-1)
		calls.Add(1)
	}, nil, f.testOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	symbols := []string{"A:1", "A:2", "A:3", "A:4"}
	for _, sym := range symbols {
		if err := s.AddSymbol(sym); err != nil {
			t.Fatal(err)
		}
	}
	var qs string
	for seen := 0; seen < len(symbols); {
		fr := f.waitFrame(t, "add", isMethod(0, "quote_add_symbols"))
		qs = paramString(fr, 0)
		seen += len(fr.params) - 1
	}
	conn := f.conn(t, 0)
	for i := 0; i < 50; i++ {
		var packet []string
		for _, sym := range symbols {
			packet = append(packet, qsd(qs, sym, map[string]any{"lp": float64(i)}))
		}
		conn.send(packet...)
	}
	deadline := time.Now().Add(5 * time.Second)
	for calls.Load() < 200 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if calls.Load() != 200 {
		t.Fatalf("%d callbacks, want 200", calls.Load())
	}
	if n := overlaps.Load(); n != 0 {
		t.Fatalf("callback ran concurrently %d times", n)
	}
}
