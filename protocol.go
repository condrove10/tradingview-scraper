package tradingview

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Wire format, as used by TradingView's own JavaScript client:
//
//	~m~<length>~m~<payload>[~m~<length>~m~<payload>...]
//
// A single WebSocket message can carry several frames. <length> counts UTF-16
// code units (JavaScript string length), not bytes; this was confirmed against
// the live server with a payload containing non-ASCII text. A payload starting
// with ~h~ is a heartbeat that must be echoed back as its own frame; a payload
// starting with ~j~ is JSON with a type prefix; anything else is JSON or text.
const (
	frameMarker     = "~m~"
	heartbeatPrefix = "~h~"
	jsonPrefix      = "~j~"
)

// wireMessage is a decoded protocol message: {"m": method, "p": params}.
type wireMessage struct {
	Method string            `json:"m"`
	Params []json.RawMessage `json:"p"`
}

// helloMessage is the first frame the server sends on a new connection.
type helloMessage struct {
	SessionID string `json:"session_id"`
	Redirect  string `json:"redirect"`
}

// utf16Len returns the length of s in UTF-16 code units.
func utf16Len(s string) int {
	n := 0
	for _, r := range s {
		if r >= 0x10000 {
			n += 2
		} else {
			n++
		}
	}
	return n
}

// encodeFrame wraps one payload in the ~m~<length>~m~ envelope.
func encodeFrame(payload string) []byte {
	length := strconv.Itoa(utf16Len(payload))
	b := make([]byte, 0, len(frameMarker)*2+len(length)+len(payload))
	b = append(b, frameMarker...)
	b = append(b, length...)
	b = append(b, frameMarker...)
	b = append(b, payload...)
	return b
}

// encodeMessage marshals {"m": method, "p": params} and frames it.
func encodeMessage(method string, params ...any) ([]byte, error) {
	if params == nil {
		params = []any{}
	}
	payload, err := json.Marshal(SocketMessage{Message: method, Payload: params})
	if err != nil {
		return nil, err
	}
	return encodeFrame(string(payload)), nil
}

// decodeFrames splits one WebSocket message into its payloads. On malformed
// input it returns the payloads decoded so far together with an error.
func decodeFrames(packet string) ([]string, error) {
	var frames []string
	for packet != "" {
		if !strings.HasPrefix(packet, frameMarker) {
			return frames, fmt.Errorf("tradingview: frame does not start with %s: %q", frameMarker, truncate(packet, 64))
		}
		rest := packet[len(frameMarker):]
		end := strings.Index(rest, frameMarker)
		if end <= 0 {
			return frames, fmt.Errorf("tradingview: frame length not terminated: %q", truncate(packet, 64))
		}
		n, err := strconv.Atoi(rest[:end])
		if err != nil || n < 0 {
			return frames, fmt.Errorf("tradingview: invalid frame length %q", truncate(rest[:end], 32))
		}
		body := rest[end+len(frameMarker):]
		size, ok := frameSize(body, n)
		if !ok {
			return frames, fmt.Errorf("tradingview: frame length %d exceeds the remaining %d bytes", n, len(body))
		}
		frames = append(frames, body[:size])
		packet = body[size:]
	}
	return frames, nil
}

// frameSize returns how many bytes of body make up a payload whose header
// says n. It counts n UTF-16 code units (the server's unit). If that does not
// end on a frame boundary but n bytes does, it falls back to bytes, so a
// server that counts bytes is still decoded correctly.
func frameSize(body string, n int) (int, bool) {
	off, units := 0, 0
	for units < n && off < len(body) {
		r, w := utf8.DecodeRuneInString(body[off:])
		if r >= 0x10000 {
			units += 2
		} else {
			units++
		}
		off += w
	}
	unitsOK := units == n
	bytesOK := n <= len(body)
	switch {
	case unitsOK && atBoundary(body, off):
		return off, true
	case bytesOK && atBoundary(body, n):
		return n, true
	case unitsOK:
		return off, true
	case bytesOK:
		return n, true
	}
	return 0, false
}

func atBoundary(body string, i int) bool {
	return i == len(body) || strings.HasPrefix(body[i:], frameMarker)
}

// splitSymbol splits "EXCHANGE:TICKER" into its parts. A symbol without an
// exchange prefix (for example "AAPL", which the server echoes back as is)
// returns an empty source instead of panicking.
func splitSymbol(full string) (source, symbol string) {
	if before, after, found := strings.Cut(full, ":"); found {
		return before, after
	}
	return "", full
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

var errNotConnected = errors.New("tradingview: not connected")
