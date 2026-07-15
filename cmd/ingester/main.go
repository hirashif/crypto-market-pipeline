package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/segmentio/kafka-go"

	"github.com/hirashif/crypto-market-pipeline/internal/obs"
	"github.com/hirashif/crypto-market-pipeline/internal/trade"
)

const coinbaseWS = "wss://ws-feed.exchange.coinbase.com"

var ticksPublished = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "ingester_ticks_published_total",
	Help: "ticks published to kafka",
}, []string{"symbol"})

// fields we use from the coinbase ticker msg
type tickerMsg struct {
	Type      string `json:"type"`
	ProductID string `json:"product_id"`
	Price     string `json:"price"`
	Time      string `json:"time"`
}

// raw ws message -> trade, false if its not a usable tick
func parseTick(raw []byte) (trade.Trade, bool) {
	var m tickerMsg
	if err := json.Unmarshal(raw, &m); err != nil || m.Type != "ticker" {
		return trade.Trade{}, false
	}
	price, err := strconv.ParseFloat(m.Price, 64)
	if err != nil {
		return trade.Trade{}, false
	}
	return trade.Trade{Symbol: m.ProductID, Price: price, Time: m.Time}, true
}

func main() {
	brokers := strings.Split(obs.Env("KAFKA_BROKERS", "localhost:9092"), ",")
	symbols := strings.Split(obs.Env("SYMBOLS", "BTC-USD,ETH-USD,SOL-USD"), ",")
	obs.ServeMetrics(obs.Env("METRICS_ADDR", ":2112"))

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  trade.Topic,
		Balancer:               &kafka.Hash{},
		AllowAutoTopicCreation: true,
		BatchTimeout:           200 * time.Millisecond,
	}
	defer writer.Close()

	log.Printf("[ingester] streaming %v from coinbase -> kafka %v", symbols, brokers)
	backoff := time.Second
	for {
		// reconnect on any ws/kafka error, back off so we dont hammer the feed
		connected, err := stream(writer, symbols)
		if connected {
			backoff = time.Second // good session, start fresh
		}
		jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
		log.Printf("[ingester] stream error: %v; reconnecting in %s", err, backoff+jitter)
		time.Sleep(backoff + jitter)
		backoff = min(backoff*2, 30*time.Second)
	}
}

// one ws session, pumps ticks to kafka until it errors
// connected reports whether we ever got a live subscription (resets the backoff)
func stream(writer *kafka.Writer, symbols []string) (bool, error) {
	conn, _, err := websocket.DefaultDialer.Dial(coinbaseWS, nil)
	if err != nil {
		return false, err
	}
	defer conn.Close()

	sub := map[string]any{
		"type":        "subscribe",
		"product_ids": symbols,
		"channels":    []string{"ticker"},
	}
	if err := conn.WriteJSON(sub); err != nil {
		return false, err
	}

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return true, err
		}
		t, ok := parseTick(raw)
		if !ok {
			continue
		}
		val, _ := json.Marshal(t)
		if err := writer.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(t.Symbol),
			Value: val,
		}); err != nil {
			return true, err
		}
		ticksPublished.WithLabelValues(t.Symbol).Inc()
	}
}
