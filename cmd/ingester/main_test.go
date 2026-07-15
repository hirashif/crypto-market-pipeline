package main

import "testing"

func TestParseTick(t *testing.T) {
	cases := []struct {
		name  string
		raw   string
		ok    bool
		sym   string
		price float64
	}{
		{"valid tick", `{"type":"ticker","product_id":"BTC-USD","price":"107000.5","time":"2026-07-15T00:00:00Z"}`, true, "BTC-USD", 107000.5},
		{"subscription ack", `{"type":"subscriptions","channels":[]}`, false, "", 0},
		{"heartbeat", `{"type":"heartbeat","sequence":123}`, false, "", 0},
		{"bad price", `{"type":"ticker","product_id":"BTC-USD","price":"not-a-number"}`, false, "", 0},
		{"empty price", `{"type":"ticker","product_id":"ETH-USD","price":""}`, false, "", 0},
		{"garbage json", `{nope`, false, "", 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tr, ok := parseTick([]byte(c.raw))
			if ok != c.ok {
				t.Fatalf("ok = %v, want %v", ok, c.ok)
			}
			if ok && (tr.Symbol != c.sym || tr.Price != c.price) {
				t.Fatalf("got %+v, want %s @ %v", tr, c.sym, c.price)
			}
		})
	}
}
