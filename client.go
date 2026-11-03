package tradingview

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var (
	// ErrClosed is returned by calls made after Close.
	ErrClosed = errors.New("tradingview: client closed")
	// ErrMaxReconnects is reported when the reconnect attempts are used up.
	// The next subscription or Connect call starts a fresh round of attempts.
	ErrMaxReconnects = errors.New("tradingview: maximum reconnect attempts reached")
	// ErrMaxRedirects is reported when the server keeps redirecting.
	ErrMaxRedirects = errors.New("tradingview: maximum redirects reached")
	// ErrListenerOverflow is reported when a listener is too slow and its
	// oldest queued events were dropped.
	ErrListenerOverflow = errors.New("tradingview: listener too slow, oldest events dropped")
)

type connState int

const (
	stateIdle       connState = iota // nothing subscribed yet: no connection (lazy)
	stateDialing                     // WebSocket handshake in progress
	stateAwaitHello                  // connected, waiting for the session frame
	stateReady                       // sessions can be created
	stateWaiting                     // a reconnect is scheduled
	stateStalled                     // gave up; a new subscription or Connect retries
)

type eventKind int

const (
	evDialed eventKind = iota
	evData
	evPong
	evReadErr
)

// connEvent carries everything that happens on a connection to the run
// goroutine. gen identifies the connection; events from an older connection
// are discarded.
type connEvent struct {
	gen  uint64
	kind eventKind
	conn *websocket.Conn
	data []byte
	err  error
}

type errorEvent struct {
	err     error
	context string
}

type waiter struct {
	ch       chan error
	failFast bool
}

// Client is a TradingView market data connection shared by any number of
// listeners.
//
// Listeners are lazy and reference counted: the client connects when the
// first listener subscribes, subscribes a market on the server when its first
// listener arrives, and unsubscribes it when its last listener leaves. After a
// reconnect every market that still has listeners is subscribed again.
//
// One goroutine owns all connection and market state and is the only writer
// to the WebSocket; public methods hand work to it. Every listener has its
// own goroutine and bounded queue, so callbacks for one listener run in order
// and never concurrently, and a slow listener cannot stall the connection or
// other listeners. Callbacks may call any Client method, including Close.
type Client struct {
	cfg config

	cmds      chan func()
	events    chan connEvent
	quit      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
	errs      *mailbox[errorEvent]

	// Owned by the run goroutine.
	state          connState
	gen            uint64
	conn           *websocket.Conn
	cancelDial     context.CancelFunc
	host           string
	attempts       int
	redirects      int
	reconnectTimer *time.Timer
	reconnectToken uint64
	pingTicker     *time.Ticker
	serverSession  string
	quoteSession   string
	quotes         map[string]*quoteMarket
	klines         map[klineKey]*klineMarket
	charts         map[string]*klineMarket
	waiters        []waiter
	nextID         uint64
	ping           PingInfo
}

// NewClient returns a client. It does not connect until the first listener
// subscribes or Connect is called. Call Close to release it.
func NewClient(options ...Option) *Client {
	return newClient(buildConfig(options))
}

func newClient(cfg config) *Client {
	c := &Client{
		cfg:    cfg,
		cmds:   make(chan func()),
		events: make(chan connEvent),
		quit:   make(chan struct{}),
		done:   make(chan struct{}),
		host:   cfg.host,
		quotes: make(map[string]*quoteMarket),
		klines: make(map[klineKey]*klineMarket),
		charts: make(map[string]*klineMarket),
	}
	if cfg.onError != nil {
		onError := cfg.onError
		c.errs = newMailbox(cfg.listenerBuffer, func(e errorEvent) { onError(e.err, e.context) })
	}
	go c.run()
	return c
}

// Connect connects now instead of waiting for the first listener, and waits
// until the connection is ready, ctx ends, or the client gives up.
func (c *Client) Connect(ctx context.Context) error {
	return c.connect(ctx, false)
}

// connect waits for a ready connection. With failFast it returns the error of
// the first failed attempt instead of waiting for reconnects.
func (c *Client) connect(ctx context.Context, failFast bool) error {
	ch := make(chan error, 1)
	err := c.call(func() error {
		if c.state == stateReady {
			ch <- nil
			return nil
		}
		c.waiters = append(c.waiters, waiter{ch: ch, failFast: failFast})
		c.ensureConnecting()
		return nil
	})
	if err != nil {
		return err
	}
	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return ErrClosed
	}
}

// Close disconnects, stops all listeners and cancels pending reconnects.
// Queued events that were not delivered yet are discarded: no callback starts
// after Close returns, but one that is already running is not waited for (so
// that Close can be called from a callback). Close is idempotent.
func (c *Client) Close() error {
	c.closeOnce.Do(func() { close(c.quit) })
	<-c.done
	return nil
}

// SessionID returns the current quote session id, or "" when there is none.
func (c *Client) SessionID() string {
	var id string
	_ = c.call(func() error { id = c.quoteSession; return nil })
	return id
}

// PingInfo returns round-trip statistics of the WebSocket pings.
func (c *Client) PingInfo() PingInfo {
	var info PingInfo
	_ = c.call(func() error { info = c.ping; return nil })
	return info
}

// do runs f on the run goroutine without waiting for it.
func (c *Client) do(f func()) error {
	select {
	case c.cmds <- f:
		return nil
	case <-c.done:
		return ErrClosed
	}
}

// call runs f on the run goroutine and waits for its result.
func (c *Client) call(f func() error) error {
	res := make(chan error, 1)
	if err := c.do(func() { res <- f() }); err != nil {
		return err
	}
	select {
	case err := <-res:
		return err
	case <-c.done:
		return ErrClosed
	}
}

// post hands a connection event to the run goroutine. It returns false once
// the client is closed.
func (c *Client) post(ev connEvent) bool {
	select {
	case c.events <- ev:
		return true
	case <-c.done:
		return false
	}
}

func (c *Client) run() {
	defer close(c.done)
	defer c.shutdown()
	for {
		var pingC <-chan time.Time
		if c.pingTicker != nil {
			pingC = c.pingTicker.C
		}
		select {
		case <-c.quit:
			return
		case f := <-c.cmds:
			f()
		case ev := <-c.events:
			c.handleEvent(ev)
		case <-pingC:
			c.sendPing()
		}
	}
}

func (c *Client) shutdown() {
	c.stopReconnectTimer()
	c.teardownConn(true)
	for _, m := range c.quotes {
		for _, box := range m.listeners {
			box.close()
		}
	}
	for _, m := range c.klines {
		for _, box := range m.listeners {
			box.close()
		}
	}
	clear(c.quotes)
	clear(c.klines)
	for _, w := range c.waiters {
		w.ch <- ErrClosed
	}
	c.waiters = nil
	if c.errs != nil {
		c.errs.close()
	}
}

func (c *Client) ensureConnecting() {
	switch c.state {
	case stateIdle:
		c.dial()
	case stateStalled:
		c.attempts = 0
		c.redirects = 0
		c.dial()
	}
}

func (c *Client) selectHost() string {
	if c.cfg.reconnectHost != "" && c.attempts > 3 {
		return c.cfg.reconnectHost
	}
	return c.host
}

func (c *Client) wsURL(host string) string {
	u := url.URL{Scheme: c.cfg.scheme, Host: host, Path: "/socket.io/websocket"}
	if parsed, err := url.Parse(host); err == nil && parsed.Host != "" {
		u.Host = parsed.Host // a redirect may carry a full URL
	}
	q := url.Values{}
	if c.cfg.fromParam != "" {
		q.Set("from", c.cfg.fromParam)
	}
	if c.cfg.buildTime != "" {
		q.Set("date", c.cfg.buildTime)
	}
	if c.cfg.connectionType != "" {
		q.Set("type", c.cfg.connectionType)
	}
	u.RawQuery = q.Encode()
	return u.String()
}

func headers() http.Header {
	h := http.Header{}
	h.Set("Origin", "https://www.tradingview.com")
	h.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36")
	h.Set("Accept-Language", "en-US,en;q=0.9")
	h.Set("Cache-Control", "no-cache")
	h.Set("Pragma", "no-cache")
	return h
}

func (c *Client) dial() {
	c.gen++
	gen := c.gen
	c.state = stateDialing
	target := c.wsURL(c.selectHost())
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.handshakeTimeout)
	c.cancelDial = cancel
	dialer := &websocket.Dialer{
		Proxy:            http.ProxyFromEnvironment,
		HandshakeTimeout: c.cfg.handshakeTimeout,
	}
	go func() {
		defer cancel()
		conn, _, err := dialer.DialContext(ctx, target, headers())
		if !c.post(connEvent{gen: gen, kind: evDialed, conn: conn, err: err}) && conn != nil {
			_ = conn.Close()
		}
	}()
}

func (c *Client) readLoop(gen uint64, conn *websocket.Conn) {
	conn.SetPongHandler(func(data string) error {
		_ = conn.SetReadDeadline(time.Now().Add(c.cfg.readTimeout))
		c.post(connEvent{gen: gen, kind: evPong, data: []byte(data)})
		return nil
	})
	for {
		if err := conn.SetReadDeadline(time.Now().Add(c.cfg.readTimeout)); err != nil {
			c.post(connEvent{gen: gen, kind: evReadErr, err: err})
			return
		}
		_, msg, err := conn.ReadMessage()
		if err != nil {
			c.post(connEvent{gen: gen, kind: evReadErr, err: err})
			return
		}
		if !c.post(connEvent{gen: gen, kind: evData, data: msg}) {
			_ = conn.Close()
			return
		}
	}
}

func (c *Client) handleEvent(ev connEvent) {
	if ev.gen != c.gen {
		if ev.conn != nil {
			_ = ev.conn.Close()
		}
		return
	}
	switch ev.kind {
	case evDialed:
		c.cancelDial = nil
		if ev.err != nil {
			c.connectionFailed(ev.err, InitErrorContext)
			return
		}
		c.conn = ev.conn
		c.state = stateAwaitHello
		go c.readLoop(ev.gen, ev.conn)
	case evData:
		c.handlePacket(ev.data)
	case evPong:
		c.recordPong(ev.data)
	case evReadErr:
		context := ReadMessageErrorContext
		var netErr net.Error
		if errors.As(ev.err, &netErr) && netErr.Timeout() {
			context = ReadTimeoutErrorContext
		}
		c.connectionFailed(ev.err, context)
	}
}

// connectionFailed drops the current connection and schedules a reconnect.
func (c *Client) connectionFailed(err error, context string) {
	c.reportError(err, context)
	c.teardownConn(false)
	c.notifyWaiters(err, true)
	c.scheduleReconnect()
}

// teardownConn closes the current connection (if any) and forgets every
// server-side session, which do not survive a reconnect.
func (c *Client) teardownConn(graceful bool) {
	c.gen++
	if c.cancelDial != nil {
		c.cancelDial()
		c.cancelDial = nil
	}
	if c.pingTicker != nil {
		c.pingTicker.Stop()
		c.pingTicker = nil
	}
	if c.conn != nil {
		if graceful {
			_ = c.conn.WriteControl(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
				time.Now().Add(time.Second))
		}
		_ = c.conn.Close()
		c.conn = nil
	}
	c.serverSession = ""
	c.quoteSession = ""
	for _, m := range c.quotes {
		m.added = false
	}
	for _, m := range c.klines {
		m.chart = ""
		m.loaded = false
	}
	clear(c.charts)
}

func (c *Client) scheduleReconnect() {
	if c.reconnectTimer != nil {
		return // one pending reconnect at a time
	}
	if limit := c.cfg.maxReconnectAttempts; limit > 0 && c.attempts >= limit {
		c.state = stateStalled
		c.reportError(ErrMaxReconnects, MaxReconnectErrorContext)
		c.notifyWaiters(ErrMaxReconnects, false)
		return
	}
	delay := c.cfg.reconnectDelay
	for i := 0; i < c.attempts && delay < c.cfg.maxReconnectDelay; i++ {
		delay *= 2
	}
	if delay > c.cfg.maxReconnectDelay {
		delay = c.cfg.maxReconnectDelay
	}
	c.attempts++
	c.state = stateWaiting
	c.reconnectToken++
	token := c.reconnectToken
	c.reconnectTimer = time.AfterFunc(delay, func() {
		_ = c.do(func() {
			if token != c.reconnectToken {
				return
			}
			c.reconnectTimer = nil
			if c.state == stateWaiting {
				c.dial()
			}
		})
	})
}

func (c *Client) stopReconnectTimer() {
	if c.reconnectTimer != nil {
		c.reconnectTimer.Stop()
		c.reconnectTimer = nil
	}
	c.reconnectToken++
}

// markHealthy resets the retry budget once a connection delivers data, so a
// server that accepts the handshake but then fails every request still runs
// out of attempts instead of looping forever.
func (c *Client) markHealthy() {
	c.attempts = 0
	c.redirects = 0
}

func (c *Client) notifyWaiters(err error, failFastOnly bool) {
	kept := c.waiters[:0]
	for _, w := range c.waiters {
		if err == nil || !failFastOnly || w.failFast {
			w.ch <- err
		} else {
			kept = append(kept, w)
		}
	}
	c.waiters = kept
}

func (c *Client) reportError(err error, context string) {
	if c.errs != nil && err != nil {
		c.errs.push(errorEvent{err: err, context: context})
	}
}

func (c *Client) handlePacket(data []byte) {
	frames, err := decodeFrames(string(data))
	if err != nil {
		c.reportError(err, ParsePacketErrorContext)
	}
	gen := c.gen
	for _, frame := range frames {
		if c.gen != gen {
			return // the connection was dropped while handling this packet
		}
		c.handleFrame(frame)
	}
}

func (c *Client) handleFrame(frame string) {
	if c.state == stateAwaitHello {
		c.handleHello(frame)
		return
	}
	if strings.HasPrefix(frame, heartbeatPrefix) {
		// Echo only this frame. Echoing the whole packet sends data frames
		// back to the server, which answers critical_error and drops the
		// quote session (seen on the live server).
		if c.writeFrame(frame) == nil {
			c.markHealthy()
		}
		return
	}
	frame = strings.TrimPrefix(frame, jsonPrefix)

	var msg wireMessage
	if err := json.Unmarshal([]byte(frame), &msg); err != nil {
		c.reportError(fmt.Errorf("tradingview: %w: %s", err, truncate(frame, 256)), DecodeMessageErrorContext)
		return
	}
	switch msg.Method {
	case "qsd":
		c.onQuoteData(msg.Params)
	case "timescale_update":
		c.onKlineData(msg.Params, true)
	case "du":
		c.onKlineData(msg.Params, false)
	case "symbol_error", "series_error":
		c.onSeriesError(msg)
	case "critical_error", "protocol_error":
		// The server drops the affected session (critical_error) or the
		// whole connection (protocol_error); reconnecting rebuilds both.
		c.connectionFailed(&ServerError{Method: msg.Method, Detail: truncate(frame, 512)}, ServerErrorMessageContext)
	case "error":
		c.reportError(&ServerError{Method: msg.Method, Detail: truncate(frame, 512)}, ServerErrorMessageContext)
	}
}

func (c *Client) handleHello(frame string) {
	var hello helloMessage
	if err := json.Unmarshal([]byte(frame), &hello); err != nil {
		c.connectionFailed(fmt.Errorf("tradingview: %w: %s", err, truncate(frame, 256)), DecodeFirstMessageErrorContext)
		return
	}
	if hello.Redirect != "" {
		c.redirect(hello.Redirect)
		return
	}
	if hello.SessionID == "" {
		c.connectionFailed(fmt.Errorf("tradingview: no session_id in %s", truncate(frame, 256)), FirstMessageWithoutSessionIdErrorContext)
		return
	}
	c.serverSession = hello.SessionID
	c.state = stateReady
	c.pingTicker = time.NewTicker(c.cfg.pingInterval)
	if c.send("set_auth_token", c.cfg.authToken) != nil {
		return
	}
	if c.syncMarkets() != nil {
		return
	}
	c.notifyWaiters(nil, false)
}

func (c *Client) redirect(host string) {
	c.redirects++
	c.teardownConn(false)
	if limit := c.cfg.maxRedirects; limit > 0 && c.redirects > limit {
		c.state = stateStalled
		c.reportError(ErrMaxRedirects, RedirectErrorContext)
		c.notifyWaiters(ErrMaxRedirects, false)
		return
	}
	c.host = host
	c.dial()
}

// syncMarkets subscribes every market that has listeners but is not
// subscribed on the current connection.
func (c *Client) syncMarkets() error {
	var pending []*quoteMarket
	for _, m := range c.quotes {
		if !m.added {
			pending = append(pending, m)
		}
	}
	if len(pending) > 0 {
		if err := c.ensureQuoteSession(); err != nil {
			return err
		}
		if err := c.addQuoteSymbols(pending...); err != nil {
			return err
		}
	}
	for _, m := range c.klines {
		if m.chart == "" {
			if err := c.openChart(m); err != nil {
				return err
			}
		}
	}
	return nil
}

// send writes one {"m","p"} message. On a write error the connection is
// dropped and a reconnect scheduled.
func (c *Client) send(method string, params ...any) error {
	frame, err := encodeMessage(method, params...)
	if err != nil {
		c.reportError(err, SendMessageErrorContext)
		return err
	}
	return c.write(frame)
}

func (c *Client) writeFrame(payload string) error {
	return c.write(encodeFrame(payload))
}

func (c *Client) write(frame []byte) error {
	if c.state != stateReady || c.conn == nil {
		return errNotConnected
	}
	_ = c.conn.SetWriteDeadline(time.Now().Add(c.cfg.writeWait))
	if err := c.conn.WriteMessage(websocket.TextMessage, frame); err != nil {
		c.connectionFailed(err, SendMessageErrorContext)
		return err
	}
	return nil
}

func (c *Client) sendPing() {
	if c.state != stateReady || c.conn == nil {
		return
	}
	payload := strconv.FormatInt(time.Now().UnixNano(), 10)
	if err := c.conn.WriteControl(websocket.PingMessage, []byte(payload), time.Now().Add(c.cfg.writeWait)); err != nil {
		c.connectionFailed(err, WebSocketPingErrorContext)
	}
}

func (c *Client) recordPong(data []byte) {
	sent, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return
	}
	rtt := time.Since(time.Unix(0, sent))
	if rtt < 0 {
		return
	}
	p := &c.ping
	if p.Count == 0 || rtt < p.Min {
		p.Min = rtt
	}
	if rtt > p.Max {
		p.Max = rtt
	}
	p.Last = rtt
	p.Count++
	p.Avg += (float64(rtt) - p.Avg) / float64(p.Count)
}
