package trade

import (
	"encoding/json"
	"testing"
)

func TestTradeJSONRoundTrip(t *testing.T) {
	in := Trade{Symbol: "BTC-USD", Price: 12345.67, Time: "2024-01-01T00:00:00Z"}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out Trade
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out != in {
		t.Fatalf("round trip mismatch: got %+v want %+v", out, in)
	}
}
