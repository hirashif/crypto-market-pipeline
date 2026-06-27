package main

import (
	"context"
	"encoding/json"
	"log"
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
	for {
		// reconnect on any ws/kafka error
		if err := stream(writer, symbols); err != nil {
			log.Printf("[ingester] stream error: %v; reconnecting in 3s", err)
			time.Sleep(3 * time.Second)
		}
	}
}

// one ws session, pumps ticks to kafka until it errors
func stream(writer *kafka.Writer, symbols []string) error {
	conn, _, err := websocket.DefaultDialer.Dial(coinbaseWS, nil)
	if err != nil {
		return err
	}
	defer conn.Close()

	sub := map[string]any{
		"type":        "subscribe",
		"product_ids": symbols,
		"channels":    []string{"ticker"},
	}
	if err := conn.WriteJSON(sub); err != nil {
		return err
	}

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		var m tickerMsg
		if err := json.Unmarshal(raw, &m); err != nil || m.Type != "ticker" {
			continue
		}
		price, err := strconv.ParseFloat(m.Price, 64)
		if err != nil {
			continue
		}
		val, _ := json.Marshal(trade.Trade{Symbol: m.ProductID, Price: price, Time: m.Time})
		if err := writer.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(m.ProductID),
			Value: val,
		}); err != nil {
			return err
		}
		ticksPublished.WithLabelValues(m.ProductID).Inc()
	}
}
