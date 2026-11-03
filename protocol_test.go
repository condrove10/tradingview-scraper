package tradingview

import (
	"reflect"
	"strconv"
	"strings"
	"testing"
)

// livePacket reproduces a packet captured from the live server: the first
// frame contains "台積電" (3 characters, 9 UTF-8 bytes) and its length header
// (164) counts UTF-16 code units.
const livePayload = `{"m":"qsd","p":["qs_probe2222",{"n":"TWSE:2330","s":"ok","v":{"lp":2295.0,"local_description":"台積電","description":"Taiwan Semiconductor Manufacturing Co., Ltd."}}]}`

func TestDecodeFramesCountsUTF16Units(t *testing.T) {
	if got := utf16Len(livePayload); got != 164 {
		t.Fatalf("fixture length = %d UTF-16 units, want the live header value 164", got)
	}
	next := `{"m":"quote_completed","p":["qs_probe2222","TWSE:2330"]}`
	packet := "~m~164~m~" + livePayload + "~m~56~m~" + next
	frames, err := decodeFrames(packet)
	if err != nil {
		t.Fatalf("decodeFrames: %v", err)
	}
	if want := []string{livePayload, next}; !reflect.DeepEqual(frames, want) {
		t.Fatalf("frames = %q\nwant %q", frames, want)
	}
}

func TestDecodeFramesAcceptsByteLengths(t *testing.T) {
	payload := `{"d":"台積電"}`
	packet := "~m~" + strconv.Itoa(len(payload)) + "~m~" + payload + "~m~4~m~~h~1"
	frames, err := decodeFrames(packet)
	if err != nil {
		t.Fatalf("decodeFrames: %v", err)
	}
	if want := []string{payload, "~h~1"}; !reflect.DeepEqual(frames, want) {
		t.Fatalf("frames = %q, want %q", frames, want)
	}
}

func TestDecodeFramesMixedHeartbeat(t *testing.T) {
	data := `{"m":"qsd","p":["qs_x",{"n":"EURONEXT:GLE","s":"ok","v":{"lp":61.07}}]}`
	packet := string(encodeFrame(data)) + string(encodeFrame("~h~2"))
	frames, err := decodeFrames(packet)
	if err != nil {
		t.Fatalf("decodeFrames: %v", err)
	}
	if want := []string{data, "~h~2"}; !reflect.DeepEqual(frames, want) {
		t.Fatalf("frames = %q, want %q", frames, want)
	}
}

func TestDecodeFramesRejectsMalformed(t *testing.T) {
	for _, packet := range []string{"x", "~m~", "~m~5", "~m~a~m~abc", "~m~-1~m~", "~m~10~m~short", "~m~2~m~ok garbage"} {
		if _, err := decodeFrames(packet); err == nil {
			t.Errorf("decodeFrames(%q): want error", packet)
		}
	}
}

func TestEncodeFrameUsesUTF16Length(t *testing.T) {
	cases := map[string]string{
		"~h~1":    "~m~4~m~~h~1",
		"台積電":     "~m~3~m~台積電",
		"a😀b":     "~m~4~m~a😀b", // the emoji is a surrogate pair: 2 units
		"":        "~m~0~m~",
		`{"m":1}`: `~m~7~m~{"m":1}`,
	}
	for in, want := range cases {
		if got := string(encodeFrame(in)); got != want {
			t.Errorf("encodeFrame(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSplitSymbol(t *testing.T) {
	cases := []struct{ in, source, symbol string }{
		{"BINANCE:BTCUSDT", "BINANCE", "BTCUSDT"},
		{"AAPL", "", "AAPL"},
		{"", "", ""},
	}
	for _, c := range cases {
		source, symbol := splitSymbol(c.in)
		if source != c.source || symbol != c.symbol {
			t.Errorf("splitSymbol(%q) = %q, %q; want %q, %q", c.in, source, symbol, c.source, c.symbol)
		}
	}
}

func FuzzDecodeFrames(f *testing.F) {
	f.Add("~m~164~m~" + livePayload)
	f.Add("~m~4~m~~h~1~m~3~m~台積電")
	f.Add("~m~2~m~😀")
	f.Fuzz(func(t *testing.T, packet string) {
		frames, err := decodeFrames(packet) // must not panic
		if err != nil {
			return
		}
		// Every decoded packet re-encodes to frames that decode the same.
		var rebuilt strings.Builder
		for _, fr := range frames {
			rebuilt.Write(encodeFrame(fr))
		}
		again, err := decodeFrames(rebuilt.String())
		if err != nil || !reflect.DeepEqual(again, frames) {
			t.Fatalf("round trip of %q: %q, %v", packet, again, err)
		}
	})
}
