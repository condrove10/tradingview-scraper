package tradingview

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"
)

// quoteFields are the fields requested for every quote session.
var quoteFields = []any{"lp", "volume", "bid", "ask"}

// Chart sessions hold a single series each: the anonymous token is limited to
// one series per chart session (the server answers "exceed limit of series in
// the session" for a second one).
const (
	symbolRef = "sds_sym_1"
	seriesID  = "sds_1"
	seriesTag = "s1"
)

type quoteValues struct {
	Price  *float64 `json:"lp"`
	Volume *float64 `json:"volume"`
	Bid    *float64 `json:"bid"`
	Ask    *float64 `json:"ask"`
}

func (v quoteValues) empty() bool {
	return v.Price == nil && v.Volume == nil && v.Bid == nil && v.Ask == nil
}

func (v *quoteValues) merge(u quoteValues) {
	if u.Price != nil {
		v.Price = u.Price
	}
	if u.Volume != nil {
		v.Volume = u.Volume
	}
	if u.Bid != nil {
		v.Bid = u.Bid
	}
	if u.Ask != nil {
		v.Ask = u.Ask
	}
}

func copyFloat(p *float64) *float64 {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

// toQuoteData returns a new QuoteData that shares no memory with v.
func (v quoteValues) toQuoteData(full string, at time.Time) *QuoteData {
	source, symbol := splitSymbol(full)
	return &QuoteData{
		Date:   at,
		Symbol: symbol,
		Source: source,
		Price:  copyFloat(v.Price),
		Volume: copyFloat(v.Volume),
		Bid:    copyFloat(v.Bid),
		Ask:    copyFloat(v.Ask),
	}
}

type quoteMarket struct {
	symbol    string
	listeners map[uint64]*mailbox[*QuoteData]
	state     quoteValues
	updated   time.Time
	added     bool // subscribed on the current connection
}

type klineKey struct {
	symbol   string
	interval string
}

type klineMarket struct {
	key          klineKey
	count        int
	listeners    map[uint64]*mailbox[KlineUpdate]
	bars         []Kline
	barCloseTime time.Time
	loaded       bool   // history received on the current connection
	chart        string // chart session id on the current connection, or ""
}

type subscriptionKind int

const (
	quoteSubscription subscriptionKind = iota
	klineSubscription
)

// Subscription is one listener on one market. Unsubscribe it when done; the
// market stays subscribed on the server while other listeners remain.
type Subscription struct {
	client *Client
	kind   subscriptionKind
	key    klineKey
	id     uint64
	once   sync.Once
}

// Unsubscribe stops this listener: no callback starts after it returns, but
// one already running is not waited for. It is idempotent, and safe to call
// from the listener's own callback or after the client is closed.
func (s *Subscription) Unsubscribe() error {
	var err error
	s.once.Do(func() {
		err = s.client.call(func() error {
			s.client.removeListener(s)
			return nil
		})
		if errors.Is(err, ErrClosed) {
			err = nil
		}
	})
	return err
}

// SubscribeQuotes adds a quote listener for symbol (for example
// "BINANCE:BTCUSDT"). fn receives the fields that changed in each update;
// unchanged fields are nil. A listener that joins a market other listeners
// already follow first receives the latest known values of every field.
func (c *Client) SubscribeQuotes(symbol string, fn OnReceiveDataCallback) (*Subscription, error) {
	symbol = strings.TrimSpace(symbol)
	if symbol == "" {
		return nil, errors.New("tradingview: empty symbol")
	}
	if fn == nil {
		return nil, errors.New("tradingview: nil quote callback")
	}
	sub := &Subscription{client: c, kind: quoteSubscription, key: klineKey{symbol: symbol}}
	err := c.call(func() error {
		m, ok := c.quotes[symbol]
		if !ok {
			m = &quoteMarket{symbol: symbol, listeners: make(map[uint64]*mailbox[*QuoteData])}
			c.quotes[symbol] = m
		}
		c.nextID++
		sub.id = c.nextID
		box := newMailbox(c.cfg.listenerBuffer, func(q *QuoteData) { fn(q) })
		m.listeners[sub.id] = box
		if !m.updated.IsZero() {
			box.push(m.state.toQuoteData(m.symbol, m.updated))
		}
		if c.state == stateReady && !m.added {
			if c.ensureQuoteSession() == nil {
				_ = c.addQuoteSymbols(m)
			}
		}
		c.ensureConnecting()
		return nil
	})
	if err != nil {
		return nil, err
	}
	return sub, nil
}

// SubscribeKlines adds a candle listener for symbol at interval, where
// interval is a TradingView resolution such as "1", "5", "60", "1D" or "1W".
// count is the number of historical bars requested when the market is first
// opened (default 300); listeners that join later share that history.
//
// fn first receives a KlineUpdate with Snapshot set and the full history,
// then updates with the bars that changed. A bar is marked Closed once a newer
// bar starts; in a snapshot the last bar is also Closed if its close time has
// already passed (for example, the market is shut). After a reconnect the
// history is sent again as a new snapshot.
func (c *Client) SubscribeKlines(symbol, interval string, count int, fn OnKlineCallback) (*Subscription, error) {
	symbol = strings.TrimSpace(symbol)
	interval = strings.TrimSpace(interval)
	if symbol == "" || interval == "" {
		return nil, errors.New("tradingview: empty symbol or interval")
	}
	if fn == nil {
		return nil, errors.New("tradingview: nil kline callback")
	}
	if count <= 0 {
		count = 300
	}
	key := klineKey{symbol: symbol, interval: interval}
	sub := &Subscription{client: c, kind: klineSubscription, key: key}
	err := c.call(func() error {
		m, ok := c.klines[key]
		if !ok {
			m = &klineMarket{key: key, count: count, listeners: make(map[uint64]*mailbox[KlineUpdate])}
			c.klines[key] = m
		}
		c.nextID++
		sub.id = c.nextID
		box := newMailbox(c.cfg.listenerBuffer, func(u KlineUpdate) { fn(u) })
		m.listeners[sub.id] = box
		if m.loaded {
			box.push(KlineUpdate{Symbol: symbol, Interval: interval, Snapshot: true, Bars: slices.Clone(m.bars)})
		}
		if c.state == stateReady && m.chart == "" {
			_ = c.openChart(m)
		}
		c.ensureConnecting()
		return nil
	})
	if err != nil {
		return nil, err
	}
	return sub, nil
}

// Quote returns the latest known values of a subscribed symbol.
func (c *Client) Quote(symbol string) (QuoteData, bool) {
	var q QuoteData
	var ok bool
	_ = c.call(func() error {
		if m := c.quotes[symbol]; m != nil && !m.updated.IsZero() {
			q, ok = *m.state.toQuoteData(m.symbol, m.updated), true
		}
		return nil
	})
	return q, ok
}

// Klines returns the bars currently held for a subscribed market.
func (c *Client) Klines(symbol, interval string) ([]Kline, bool) {
	var bars []Kline
	var ok bool
	_ = c.call(func() error {
		if m := c.klines[klineKey{symbol, interval}]; m != nil && m.loaded {
			bars, ok = slices.Clone(m.bars), true
		}
		return nil
	})
	return bars, ok
}

func (c *Client) removeListener(s *Subscription) {
	switch s.kind {
	case quoteSubscription:
		m := c.quotes[s.key.symbol]
		if m == nil || m.listeners[s.id] == nil {
			return
		}
		m.listeners[s.id].close()
		delete(m.listeners, s.id)
		if len(m.listeners) > 0 {
			return
		}
		delete(c.quotes, s.key.symbol)
		if m.added && c.quoteSession != "" {
			_ = c.send("quote_remove_symbols", c.quoteSession, m.symbol)
		}
	case klineSubscription:
		m := c.klines[s.key]
		if m == nil || m.listeners[s.id] == nil {
			return
		}
		m.listeners[s.id].close()
		delete(m.listeners, s.id)
		if len(m.listeners) > 0 {
			return
		}
		delete(c.klines, s.key)
		if m.chart != "" {
			delete(c.charts, m.chart)
			_ = c.send("chart_delete_session", m.chart)
		}
	}
}

func (c *Client) ensureQuoteSession() error {
	if c.quoteSession != "" {
		return nil
	}
	qs := "qs_" + GetRandomString(12)
	if err := c.send("quote_create_session", qs); err != nil {
		return err
	}
	c.quoteSession = qs
	return c.send("quote_set_fields", append([]any{qs}, quoteFields...)...)
}

// addQuoteSymbols subscribes markets on the quote session. Symbols are sent
// as plain strings: a {"flags":[...]} argument makes the server answer
// invalid_parameters and drop the session.
func (c *Client) addQuoteSymbols(markets ...*quoteMarket) error {
	params := make([]any, 0, len(markets)+1)
	params = append(params, c.quoteSession)
	for _, m := range markets {
		params = append(params, m.symbol)
	}
	if err := c.send("quote_add_symbols", params...); err != nil {
		return err
	}
	for _, m := range markets {
		m.added = true
	}
	return nil
}

func (c *Client) openChart(m *klineMarket) error {
	cs := "cs_" + GetRandomString(12)
	if err := c.send("chart_create_session", cs, ""); err != nil {
		return err
	}
	m.chart = cs
	c.charts[cs] = m
	ref, err := json.Marshal(struct {
		Symbol     string `json:"symbol"`
		Adjustment string `json:"adjustment"`
	}{m.key.symbol, "splits"})
	if err != nil {
		return err
	}
	if err := c.send("resolve_symbol", cs, symbolRef, "="+string(ref)); err != nil {
		return err
	}
	return c.send("create_series", cs, seriesID, seriesTag, symbolRef, m.key.interval, m.count, "")
}

func deliver[T any](c *Client, box *mailbox[T], v T, market string) {
	if box.push(v) {
		c.reportError(fmt.Errorf("%w: %s", ErrListenerOverflow, market), ListenerOverflowErrorContext)
	}
}

func (c *Client) onQuoteData(params []json.RawMessage) {
	if len(params) < 2 {
		return
	}
	var session string
	if json.Unmarshal(params[0], &session) != nil || session != c.quoteSession {
		return
	}
	var q struct {
		Name   string      `json:"n"`
		Status string      `json:"s"`
		ErrMsg string      `json:"errmsg"`
		Values quoteValues `json:"v"`
	}
	if err := json.Unmarshal(params[1], &q); err != nil {
		c.reportError(err, DecodeQuoteMessageErrorContext)
		return
	}
	if q.Status != "ok" {
		reason := q.ErrMsg
		if reason == "" {
			reason = q.Status
		}
		c.reportError(&SymbolError{Symbol: q.Name, Reason: reason}, SymbolErrorContext)
		return
	}
	c.markHealthy()
	m := c.quotes[q.Name]
	if m == nil || q.Values.empty() {
		return
	}
	now := time.Now().UTC()
	m.state.merge(q.Values)
	m.updated = now
	for _, box := range m.listeners {
		deliver(c, box, q.Values.toQuoteData(q.Name, now), q.Name)
	}
}

type seriesPayload struct {
	Bars []struct {
		Values []float64 `json:"v"`
	} `json:"s"`
	Lbs *struct {
		BarCloseTime float64 `json:"bar_close_time"`
	} `json:"lbs"`
}

func (c *Client) onKlineData(params []json.RawMessage, history bool) {
	if len(params) < 2 {
		return
	}
	var cs string
	if json.Unmarshal(params[0], &cs) != nil {
		return
	}
	m := c.charts[cs]
	if m == nil {
		return
	}
	var series map[string]seriesPayload
	if err := json.Unmarshal(params[1], &series); err != nil {
		c.reportError(err, DecodeMessageErrorContext)
		return
	}
	s, ok := series[seriesID]
	if !ok {
		return
	}
	c.markHealthy()
	if s.Lbs != nil && s.Lbs.BarCloseTime > 0 {
		m.barCloseTime = time.Unix(int64(s.Lbs.BarCloseTime), 0).UTC()
	}
	bars := make([]Kline, 0, len(s.Bars))
	for _, b := range s.Bars {
		v := b.Values
		if len(v) < 5 {
			continue
		}
		k := Kline{Time: time.Unix(int64(v[0]), 0).UTC(), Open: v[1], High: v[2], Low: v[3], Close: v[4]}
		if len(v) > 5 {
			k.Volume = v[5]
		}
		bars = append(bars, k)
	}
	slices.SortStableFunc(bars, func(a, b Kline) int { return a.Time.Compare(b.Time) })

	now := time.Now()
	update := KlineUpdate{Symbol: m.key.symbol, Interval: m.key.interval}
	switch {
	case history && !m.loaded:
		m.replace(bars, now)
		m.loaded = true
		update.Snapshot = true
		update.Bars = m.bars
	case !m.loaded:
		return // live updates before the history are superseded by it
	default:
		update.Bars = m.upsert(bars)
		if len(update.Bars) == 0 {
			return
		}
	}
	for _, box := range m.listeners {
		u := update
		u.Bars = slices.Clone(update.Bars)
		deliver(c, box, u, m.key.symbol+" "+m.key.interval)
	}
}

// replace installs a full history, keeping the newest count bars.
func (m *klineMarket) replace(bars []Kline, now time.Time) {
	if len(bars) > m.count {
		bars = bars[len(bars)-m.count:]
	}
	m.bars = slices.Clone(bars)
	for i := range m.bars {
		m.bars[i].Closed = i < len(m.bars)-1
	}
	if n := len(m.bars); n > 0 && !m.barCloseTime.IsZero() && !now.Before(m.barCloseTime) {
		m.bars[n-1].Closed = true
	}
}

// upsert applies live bars and returns the bars that changed. When a newer
// bar starts, the previous last bar is returned again with Closed set.
func (m *klineMarket) upsert(bars []Kline) []Kline {
	var changed []Kline
	for _, b := range bars {
		n := len(m.bars)
		switch {
		case n == 0 || b.Time.After(m.bars[n-1].Time):
			if n > 0 && !m.bars[n-1].Closed {
				m.bars[n-1].Closed = true
				changed = append(changed, m.bars[n-1])
			}
			m.bars = append(m.bars, b)
			if len(m.bars) > m.count {
				m.bars = slices.Clone(m.bars[len(m.bars)-m.count:])
			}
			changed = append(changed, b)
		case b.Time.Equal(m.bars[n-1].Time):
			b.Closed = m.bars[n-1].Closed
			m.bars[n-1] = b
			changed = append(changed, b)
		default:
			i, found := slices.BinarySearchFunc(m.bars, b.Time, func(k Kline, t time.Time) int { return k.Time.Compare(t) })
			if found {
				b.Closed = true
				m.bars[i] = b
				changed = append(changed, b)
			}
		}
	}
	return changed
}

func (c *Client) onSeriesError(msg wireMessage) {
	if len(msg.Params) == 0 {
		return
	}
	var cs string
	if json.Unmarshal(msg.Params[0], &cs) != nil {
		return
	}
	m := c.charts[cs]
	if m == nil {
		return
	}
	// symbol_error: [cs, symbolRef, reason]; series_error: [cs, seriesID, tag, reason, ...]
	reasonIndex := 2
	if msg.Method == "series_error" {
		reasonIndex = 3
	}
	reason := msg.Method
	if len(msg.Params) > reasonIndex {
		var r string
		if json.Unmarshal(msg.Params[reasonIndex], &r) == nil && r != "" {
			reason = r
		}
	}
	c.reportError(&SymbolError{Symbol: m.key.symbol, Interval: m.key.interval, Reason: reason}, SymbolErrorContext)
}
