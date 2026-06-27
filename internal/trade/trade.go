package trade

// kafka topic for ticks
const Topic = "trades"

// normalized tick passed ingester -> processor
type Trade struct {
	Symbol string  `json:"symbol"`
	Price  float64 `json:"price"`
	Time   string  `json:"time"`
}
