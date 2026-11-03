package tradingview

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// fakeTV is an in-process stand-in for the TradingView socket. It speaks the
// framing used by the live server and enforces the rules observed there:
//   - a request for an unknown session gets protocol_error and the connection
//     is closed;
//   - a {"flags":...} argument to quote_add_symbols gets critical_error
//     invalid_parameters and the quote session is dropped;
//   - a second series in one chart session gets critical_error;
//   - a client frame that is not a heartbeat or a known method gets
//     critical_error invalid_method.
//
// Anything else a correct client must never do is recorded as a violation.
type fakeTV struct {
	t      *testing.T
	srv    *httptest.Server
	frames chan fakeFrame

	mu         sync.Mutex
	conns      []*fakeConn
	requests   []*http.Request
	violations []string
	hello      func(idx int) string
	reject     bool
}

type fakeConn struct {
	tv  *fakeTV
	idx int
	ws  *websocket.Conn
	wmu sync.Mutex

	// guarded by tv.mu
	quotes     map[string]map[string]bool // quote session -> symbols
	charts     map[string]int             // chart session -> series count
	heartbeats map[string]bool            // heartbeats sent and not yet answered
}

type fakeFrame struct {
	conn    int
	payload string
	method  string
	params  []any
}

func newFakeTV(t *testing.T) *fakeTV {
	t.Helper()
	f := &fakeTV{t: t, frames: make(chan fakeFrame, 100000)}
	f.hello = func(idx int) string {
		return fmt.Sprintf(`{"session_id":"fake-%d","timestamp":1,"protocol":"json"}`, idx)
	}
	f.srv = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.close)
	return f
}

func (f *fakeTV) addr() string { return strings.TrimPrefix(f.srv.URL, "http://") }

func (f *fakeTV) close() {
	f.mu.Lock()
	conns := append([]*fakeConn(nil), f.conns...)
	f.mu.Unlock()
	for _, c := range conns {
		_ = c.ws.Close()
	}
	f.srv.Close()
}

func (f *fakeTV) handle(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.requests = append(f.requests, r)
	reject := f.reject
	f.mu.Unlock()
	if reject {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	ws, err := up.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	f.mu.Lock()
	c := &fakeConn{
		tv: f, idx: len(f.conns), ws: ws,
		quotes:     map[string]map[string]bool{},
		charts:     map[string]int{},
		heartbeats: map[string]bool{},
	}
	f.conns = append(f.conns, c)
	hello := f.hello(c.idx)
	f.mu.Unlock()

	c.send(hello)
	for {
		_, data, err := ws.ReadMessage()
		if err != nil {
			return
		}
		frames, err := decodeFrames(string(data))
		if err != nil {
			f.violation("conn %d: undecodable packet %q: %v", c.idx, data, err)
		}
		for _, p := range frames {
			if !c.process(p) {
				return
			}
		}
	}
}

func (f *fakeTV) violation(format string, args ...any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.violations = append(f.violations, fmt.Sprintf(format, args...))
}

func (f *fakeTV) checkNoViolations(t *testing.T) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, v := range f.violations {
		t.Errorf("protocol violation: %s", v)
	}
}

func (f *fakeTV) connCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.conns)
}

func (f *fakeTV) requestCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.requests)
}

// conn waits for the idx-th connection.
func (f *fakeTV) conn(t *testing.T, idx int) *fakeConn {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		f.mu.Lock()
		if len(f.conns) > idx {
			c := f.conns[idx]
			f.mu.Unlock()
			return c
		}
		f.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("connection %d never arrived (have %d)", idx, f.connCount())
	return nil
}

// waitFrame returns the next client frame matching pred.
func (f *fakeTV) waitFrame(t *testing.T, what string, pred func(fakeFrame) bool) fakeFrame {
	t.Helper()
	timeout := time.After(5 * time.Second)
	for {
		select {
		case fr := <-f.frames:
			if pred(fr) {
				return fr
			}
		case <-timeout:
			t.Fatalf("timed out waiting for client frame: %s", what)
			return fakeFrame{}
		}
	}
}

// drainFrames returns every frame received within d.
func (f *fakeTV) drainFrames(d time.Duration) []fakeFrame {
	var out []fakeFrame
	timeout := time.After(d)
	for {
		select {
		case fr := <-f.frames:
			out = append(out, fr)
		case <-timeout:
			return out
		}
	}
}

func isMethod(conn int, method string) func(fakeFrame) bool {
	return func(fr fakeFrame) bool { return fr.conn == conn && fr.method == method }
}

func paramString(fr fakeFrame, i int) string {
	if i < len(fr.params) {
		if s, ok := fr.params[i].(string); ok {
			return s
		}
	}
	return ""
}

// process applies one client frame. It returns false when the server closes
// the connection in response.
func (c *fakeConn) process(payload string) bool {
	f := c.tv
	fr := fakeFrame{conn: c.idx, payload: payload}
	defer func() {
		select {
		case f.frames <- fr:
		default:
			f.violation("frame buffer full")
		}
	}()

	if strings.HasPrefix(payload, heartbeatPrefix) {
		f.mu.Lock()
		ok := c.heartbeats[payload]
		delete(c.heartbeats, payload)
		f.mu.Unlock()
		if !ok {
			f.violation("conn %d: unexpected heartbeat reply %q", c.idx, payload)
		}
		fr.method = "heartbeat"
		return true
	}
	var msg struct {
		M string `json:"m"`
		P []any  `json:"p"`
	}
	if err := json.Unmarshal([]byte(payload), &msg); err != nil || msg.M == "" {
		f.violation("conn %d: non-message frame %q", c.idx, truncate(payload, 80))
		c.sendMsg("critical_error", "", "invalid_method", "")
		return true
	}
	fr.method, fr.params = msg.M, msg.P
	session := paramString(fr, 0)

	f.mu.Lock()
	defer f.mu.Unlock()
	unknown := func() bool {
		f.violations = append(f.violations, fmt.Sprintf("conn %d: %s on unknown session %q", c.idx, msg.M, session))
		go func() {
			c.sendMsg("protocol_error", "unknown_session_id "+session)
			_ = c.ws.Close()
		}()
		return false
	}
	switch msg.M {
	case "set_auth_token":
	case "quote_create_session":
		c.quotes[session] = map[string]bool{}
	case "quote_set_fields":
		if c.quotes[session] == nil {
			return unknown()
		}
	case "quote_add_symbols":
		syms := c.quotes[session]
		if syms == nil {
			return unknown()
		}
		for _, p := range msg.P[1:] {
			sym, ok := p.(string)
			if !ok {
				f.violations = append(f.violations, fmt.Sprintf("conn %d: non-string quote_add_symbols argument %v", c.idx, p))
				delete(c.quotes, session)
				go c.sendMsg("critical_error", session, "invalid_parameters", "quote_add_symbols")
				return true
			}
			if syms[sym] {
				f.violations = append(f.violations, fmt.Sprintf("conn %d: %s added twice", c.idx, sym))
			}
			syms[sym] = true
		}
	case "quote_remove_symbols":
		syms := c.quotes[session]
		if syms == nil {
			return unknown()
		}
		for _, p := range msg.P[1:] {
			sym, _ := p.(string)
			if !syms[sym] {
				f.violations = append(f.violations, fmt.Sprintf("conn %d: %s removed but not added", c.idx, sym))
			}
			delete(syms, sym)
		}
	case "chart_create_session":
		c.charts[session] = 0
	case "resolve_symbol":
		if _, ok := c.charts[session]; !ok {
			return unknown()
		}
	case "create_series":
		n, ok := c.charts[session]
		if !ok {
			return unknown()
		}
		c.charts[session] = n + 1
		if n+1 > 1 {
			f.violations = append(f.violations, fmt.Sprintf("conn %d: second series in %s", c.idx, session))
			go c.sendMsg("critical_error", session, "exceed limit of series in the session")
		}
	case "chart_delete_session":
		if _, ok := c.charts[session]; !ok {
			return unknown()
		}
		delete(c.charts, session)
	default:
		f.violations = append(f.violations, fmt.Sprintf("conn %d: unexpected method %s", c.idx, msg.M))
		go c.sendMsg("critical_error", session, "invalid_method", msg.M)
	}
	return true
}

// symbols returns the symbols currently added on this connection.
func (c *fakeConn) symbols() map[string]bool {
	c.tv.mu.Lock()
	defer c.tv.mu.Unlock()
	out := map[string]bool{}
	for _, syms := range c.quotes {
		for s := range syms {
			out[s] = true
		}
	}
	return out
}

func (c *fakeConn) send(payloads ...string) {
	var packet []byte
	for _, p := range payloads {
		packet = append(packet, encodeFrame(p)...)
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	_ = c.ws.WriteMessage(websocket.TextMessage, packet)
}

func msgJSON(method string, params ...any) string {
	b, err := json.Marshal(map[string]any{"m": method, "p": params})
	if err != nil {
		panic(err)
	}
	return string(b)
}

func (c *fakeConn) sendMsg(method string, params ...any) { c.send(msgJSON(method, params...)) }

func (c *fakeConn) heartbeat(n int) string {
	hb := fmt.Sprintf("~h~%d", n)
	c.tv.mu.Lock()
	c.heartbeats[hb] = true
	c.tv.mu.Unlock()
	return hb
}

func qsd(session, symbol string, values map[string]any) string {
	return msgJSON("qsd", session, map[string]any{"n": symbol, "s": "ok", "v": values})
}

func bar(t int64, o, h, l, cl, v float64) map[string]any {
	return map[string]any{"i": 0, "v": []float64{float64(t), o, h, l, cl, v}}
}

func seriesMsg(method, chart string, closeTime int64, bars ...map[string]any) string {
	return msgJSON(method, chart, map[string]any{
		"sds_1": map[string]any{"s": bars, "t": "s1", "lbs": map[string]any{"bar_close_time": closeTime}},
	})
}

// testOptions points a client at the fake server with short timings.
func (f *fakeTV) testOptions(extra ...Option) []Option {
	return append([]Option{func(s *Socket) {
		s.cfg.scheme = "ws"
		s.cfg.host = f.addr()
		s.cfg.reconnectHost = ""
		s.cfg.readTimeout = 3 * time.Second
		s.cfg.pingInterval = 100 * time.Millisecond
		s.cfg.handshakeTimeout = 2 * time.Second
		s.cfg.reconnectDelay = 10 * time.Millisecond
		s.cfg.maxReconnectDelay = 40 * time.Millisecond
	}}, extra...)
}

// recorder collects callback values.
type recorder[T any] struct {
	mu    sync.Mutex
	items []T
	ch    chan T
}

func newRecorder[T any]() *recorder[T] { return &recorder[T]{ch: make(chan T, 10000)} }

func (r *recorder[T]) add(v T) {
	r.mu.Lock()
	r.items = append(r.items, v)
	r.mu.Unlock()
	r.ch <- v
}

func (r *recorder[T]) next(t *testing.T, what string) T {
	t.Helper()
	select {
	case v := <-r.ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

func (r *recorder[T]) none(t *testing.T, d time.Duration, what string) {
	t.Helper()
	select {
	case v := <-r.ch:
		t.Fatalf("unexpected %s: %+v", what, v)
	case <-time.After(d):
	}
}

type errRecord struct {
	err     error
	context string
}

func errorRecorder() (*recorder[errRecord], Option) {
	r := newRecorder[errRecord]()
	return r, WithErrorHandler(func(err error, context string) { r.add(errRecord{err, context}) })
}

func price(q *QuoteData) float64 {
	if q == nil || q.Price == nil {
		return -1
	}
	return *q.Price
}
