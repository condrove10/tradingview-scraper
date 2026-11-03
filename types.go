package tradingview

import (
	"fmt"
	"time"
)

// SocketInterface is implemented by Socket, the original callback API.
type SocketInterface interface {
	AddSymbol(symbol string) error
	RemoveSymbol(symbol string) error
	Init() error
	Close() error
}

// SocketMessage is a protocol message: {"m": method, "p": params}.
type SocketMessage struct {
	Message string      `json:"m"`
	Payload interface{} `json:"p"`
}

// QuoteMessage is the payload of a "qsd" (quote data) message.
type QuoteMessage struct {
	Symbol string     `mapstructure:"n"`
	Status string     `mapstructure:"s"`
	Data   *QuoteData `mapstructure:"v"`
}

// QuoteData is one quote update. Fields that did not change are nil.
type QuoteData struct {
	Date   time.Time `json:"date"`
	Symbol string    `json:"symbol"` // ticker without exchange, e.g. "BTCUSDT"
	Source string    `json:"source"` // exchange prefix, e.g. "BINANCE"; "" if none
	Price  *float64  `json:"price" mapstructure:"lp"`
	Volume *float64  `json:"volume" mapstructure:"volume"`
	Bid    *float64  `json:"bid" mapstructure:"bid"`
	Ask    *float64  `json:"ask" mapstructure:"ask"`
}

// Flags ...
type Flags struct {
	Flags []string `json:"flags"`
}

// Kline is one candle.
type Kline struct {
	Time   time.Time `json:"time"` // bar open time, UTC
	Open   float64   `json:"open"`
	High   float64   `json:"high"`
	Low    float64   `json:"low"`
	Close  float64   `json:"close"`
	Volume float64   `json:"volume"`
	Closed bool      `json:"closed"` // final: a newer bar has started (or, in a snapshot, the close time had passed)
}

// KlineUpdate is delivered to kline listeners.
type KlineUpdate struct {
	Symbol   string  `json:"symbol"`   // as subscribed, e.g. "BINANCE:BTCUSDT"
	Interval string  `json:"interval"` // as subscribed, e.g. "1"
	Snapshot bool    `json:"snapshot"` // Bars is the full history, replacing earlier bars
	Bars     []Kline `json:"bars"`     // otherwise: bars that changed, oldest first
}

// PingInfo holds round-trip statistics of the WebSocket ping frames.
type PingInfo struct {
	Last  time.Duration
	Min   time.Duration
	Max   time.Duration
	Avg   float64 // nanoseconds
	Count int
}

// SymbolError reports a symbol the server rejected (for example
// "no_such_symbol" or "invalid symbol"). The listeners stay registered.
type SymbolError struct {
	Symbol   string
	Interval string // empty for quotes
	Reason   string
}

func (e *SymbolError) Error() string {
	if e.Interval != "" {
		return fmt.Sprintf("tradingview: symbol %s (%s): %s", e.Symbol, e.Interval, e.Reason)
	}
	return fmt.Sprintf("tradingview: symbol %s: %s", e.Symbol, e.Reason)
}

// ServerError reports an error message sent by the server. For
// critical_error and protocol_error the client reconnects.
type ServerError struct {
	Method string
	Detail string
}

func (e *ServerError) Error() string {
	return fmt.Sprintf("tradingview: server %s: %s", e.Method, e.Detail)
}

// OnReceiveDataCallback receives quote updates.
type OnReceiveDataCallback func(quote *QuoteData)

// OnKlineCallback receives candle updates.
type OnKlineCallback func(update KlineUpdate)

// OnErrorCallback receives errors with a short description of what failed.
type OnErrorCallback func(err error, context string)
