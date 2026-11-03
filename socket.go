package tradingview

import (
	"context"
	"fmt"
	"sync"
)

// Socket is the original callback API, kept for compatibility. It wraps a
// Client: each AddSymbol is a quote listener, OnReceiveMarketDataCallback is
// never called concurrently with itself, and the connection reconnects and
// resubscribes on its own. New code can use Client directly to share
// markets between listeners and to receive candles.
type Socket struct {
	OnReceiveMarketDataCallback OnReceiveDataCallback
	OnErrorCallback             OnErrorCallback

	cfg config

	mu      sync.Mutex
	client  *Client
	symbols map[string]*Subscription // nil until the client exists
	closed  bool

	callbackMu sync.Mutex
}

// NewSocket returns a Socket that is not connected yet; call Init.
func NewSocket(options ...Option) *Socket {
	return &Socket{cfg: buildConfig(options), symbols: make(map[string]*Subscription)}
}

// Connect creates a Socket, connects it and returns it. If the first
// connection attempt fails, nothing keeps running in the background.
func Connect(
	onReceiveMarketData OnReceiveDataCallback,
	onError OnErrorCallback,
	options ...Option,
) (SocketInterface, error) {
	s := NewSocket(options...)
	s.OnReceiveMarketDataCallback = onReceiveMarketData
	s.OnErrorCallback = onError
	if err := s.Init(); err != nil {
		return nil, err
	}
	return s, nil
}

// Init connects and subscribes the symbols added so far. It returns the
// error of the first failed attempt; after a successful Init the socket
// reconnects on its own.
func (s *Socket) Init() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	if s.client != nil {
		s.mu.Unlock()
		return nil
	}
	cfg := s.cfg
	if s.OnErrorCallback != nil {
		cfg.onError = s.OnErrorCallback
	}
	client := newClient(cfg)
	for symbol := range s.symbols {
		sub, err := client.SubscribeQuotes(symbol, s.dispatch)
		if err != nil {
			s.mu.Unlock()
			_ = client.Close()
			return err
		}
		s.symbols[symbol] = sub
	}
	s.client = client
	s.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), cfg.handshakeTimeout+cfg.readTimeout)
	defer cancel()
	if err := client.connect(ctx, true); err != nil {
		_ = client.Close()
		s.mu.Lock()
		if s.client == client {
			s.client = nil
			for symbol := range s.symbols {
				s.symbols[symbol] = nil
			}
		}
		s.mu.Unlock()
		return fmt.Errorf("connection failed: %w", err)
	}
	return nil
}

func (s *Socket) dispatch(quote *QuoteData) {
	s.callbackMu.Lock()
	defer s.callbackMu.Unlock()
	if fn := s.OnReceiveMarketDataCallback; fn != nil {
		fn(quote)
	}
}

// AddSymbol subscribes to quote updates for symbol, e.g. "BINANCE:BTCUSDT".
// Adding a symbol twice has no effect.
func (s *Socket) AddSymbol(symbol string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if _, ok := s.symbols[symbol]; ok {
		return nil
	}
	if s.client == nil {
		s.symbols[symbol] = nil
		return nil
	}
	sub, err := s.client.SubscribeQuotes(symbol, s.dispatch)
	if err != nil {
		return err
	}
	s.symbols[symbol] = sub
	return nil
}

// RemoveSymbol stops quote updates for symbol.
func (s *Socket) RemoveSymbol(symbol string) error {
	s.mu.Lock()
	sub, ok := s.symbols[symbol]
	delete(s.symbols, symbol)
	s.mu.Unlock()
	if !ok || sub == nil {
		return nil
	}
	return sub.Unsubscribe()
}

// Close disconnects. A closed Socket cannot be reused.
func (s *Socket) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	client := s.client
	s.client = nil
	s.mu.Unlock()
	if client != nil {
		return client.Close()
	}
	return nil
}

func (s *Socket) currentClient() *Client {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.client
}

// GetSessionID returns the current quote session id.
func (s *Socket) GetSessionID() string {
	if c := s.currentClient(); c != nil {
		return c.SessionID()
	}
	return ""
}

// GetPingInfo returns WebSocket ping round-trip statistics.
func (s *Socket) GetPingInfo() PingInfo {
	if c := s.currentClient(); c != nil {
		return c.PingInfo()
	}
	return PingInfo{}
}
