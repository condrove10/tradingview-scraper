package tradingview

import "time"

const (
	defaultWebSocketHost  = "data.tradingview.com"
	defaultReconnectHost  = "history-data.tradingview.com"
	defaultBuildTime      = "2025_03_05-11_31"
	defaultFromParam      = "chart/"
	defaultConnectionType = "main"
	defaultAuthToken      = "unauthorized_user_token"

	defaultReadTimeout       = 20 * time.Second // same idle timeout as the JS client
	defaultPingInterval      = 10 * time.Second
	defaultWriteWait         = 10 * time.Second
	defaultHandshakeTimeout  = 10 * time.Second
	defaultReconnectDelay    = 5 * time.Second // the JS client waits 5s before reconnecting
	defaultMaxReconnectDelay = 60 * time.Second
	defaultMaxReconnects     = 20 // the JS client's isMaxReconnects threshold
	defaultMaxRedirects      = 20 // the JS client's isMaxRedirects threshold
	defaultListenerBuffer    = 1024
)

// config holds the settings shared by Client and the legacy Socket.
type config struct {
	scheme         string
	host           string
	reconnectHost  string
	buildTime      string
	fromParam      string
	connectionType string
	authToken      string

	readTimeout          time.Duration
	pingInterval         time.Duration
	writeWait            time.Duration
	handshakeTimeout     time.Duration
	reconnectDelay       time.Duration
	maxReconnectDelay    time.Duration
	maxReconnectAttempts int
	maxRedirects         int
	listenerBuffer       int

	onError OnErrorCallback
}

func defaultConfig() config {
	return config{
		scheme:               "wss",
		host:                 defaultWebSocketHost,
		reconnectHost:        defaultReconnectHost,
		buildTime:            defaultBuildTime,
		fromParam:            defaultFromParam,
		connectionType:       defaultConnectionType,
		authToken:            defaultAuthToken,
		readTimeout:          defaultReadTimeout,
		pingInterval:         defaultPingInterval,
		writeWait:            defaultWriteWait,
		handshakeTimeout:     defaultHandshakeTimeout,
		reconnectDelay:       defaultReconnectDelay,
		maxReconnectDelay:    defaultMaxReconnectDelay,
		maxReconnectAttempts: defaultMaxReconnects,
		maxRedirects:         defaultMaxRedirects,
		listenerBuffer:       defaultListenerBuffer,
	}
}

// Option configures a Client or a Socket. It is an alias of func(*Socket) so
// that options written for earlier versions of this package keep compiling.
type Option = func(*Socket)

// WithHost sets the WebSocket host (default data.tradingview.com).
func WithHost(host string) Option {
	return func(s *Socket) { s.cfg.host = host }
}

// WithReconnectHost sets the host used after more than 3 failed reconnects
// (default history-data.tradingview.com). An empty host disables the switch.
func WithReconnectHost(host string) Option {
	return func(s *Socket) { s.cfg.reconnectHost = host }
}

// WithBuildTime sets the "date" query parameter (TradingView's BUILD_TIME).
func WithBuildTime(time string) Option {
	return func(s *Socket) { s.cfg.buildTime = time }
}

// WithFromParam sets the "from" query parameter (the page path, e.g. "chart/").
func WithFromParam(param string) Option {
	return func(s *Socket) { s.cfg.fromParam = param }
}

// WithConnectionType sets the "type" query parameter (default "main").
func WithConnectionType(connType string) Option {
	return func(s *Socket) { s.cfg.connectionType = connType }
}

// WithAuthToken sets the token sent with set_auth_token. The default is the
// anonymous "unauthorized_user_token", which TradingView limits to one candle
// series per chart session; the client opens one chart session per market.
func WithAuthToken(token string) Option {
	return func(s *Socket) { s.cfg.authToken = token }
}

// WithErrorHandler sets the callback for connection and protocol errors.
// It is called from its own goroutine, never concurrently with itself.
func WithErrorHandler(fn OnErrorCallback) Option {
	return func(s *Socket) { s.cfg.onError = fn }
}

// WithListenerBuffer sets how many undelivered events each listener may queue
// before the oldest are dropped (default 1024).
func WithListenerBuffer(n int) Option {
	return func(s *Socket) {
		if n > 0 {
			s.cfg.listenerBuffer = n
		}
	}
}

// WithReconnect sets the reconnect policy: the first delay, the maximum delay
// (the delay doubles after each failed attempt) and the number of attempts
// before the client gives up. maxAttempts <= 0 means retry forever.
func WithReconnect(delay, maxDelay time.Duration, maxAttempts int) Option {
	return func(s *Socket) {
		if delay > 0 {
			s.cfg.reconnectDelay = delay
		}
		if maxDelay > 0 {
			s.cfg.maxReconnectDelay = maxDelay
		}
		s.cfg.maxReconnectAttempts = maxAttempts
	}
}

// WithReadTimeout sets how long the connection may stay silent (no data, no
// heartbeat, no pong) before it is considered dead (default 20s).
func WithReadTimeout(d time.Duration) Option {
	return func(s *Socket) {
		if d > 0 {
			s.cfg.readTimeout = d
		}
	}
}

// WithPingInterval sets how often a WebSocket ping is sent (default 10s).
func WithPingInterval(d time.Duration) Option {
	return func(s *Socket) {
		if d > 0 {
			s.cfg.pingInterval = d
		}
	}
}

func buildConfig(options []Option) config {
	s := &Socket{cfg: defaultConfig()}
	for _, option := range options {
		if option != nil {
			option(s)
		}
	}
	return s.cfg
}
