package main

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"github.com/hirashif/crypto-market-pipeline/internal/obs"
	"github.com/hirashif/crypto-market-pipeline/internal/trade"
)

var ticksProcessed = promauto.NewCounter(prometheus.CounterOpts{
	Name: "processor_ticks_processed_total",
	Help: "ticks consumed from kafka and written to redis",
})

func main() {
	brokers := strings.Split(obs.Env("KAFKA_BROKERS", "localhost:9092"), ",")
	rdb := redis.NewClient(&redis.Options{Addr: obs.Env("REDIS_ADDR", "localhost:6379")})
	obs.ServeMetrics(obs.Env("METRICS_ADDR", ":2112"))

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  "processor",
		Topic:    trade.Topic,
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  500 * time.Millisecond,
	})
	defer reader.Close()

	ctx := context.Background()
	log.Printf("[processor] consuming %q from %v -> redis %s", trade.Topic, brokers, rdb.Options().Addr)
	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Printf("[processor] read error: %v", err)
			time.Sleep(time.Second)
			continue
		}
		var t trade.Trade
		if err := json.Unmarshal(m.Value, &t); err != nil {
			continue
		}
		pipe := rdb.Pipeline()
		pipe.HSet(ctx, "price:"+t.Symbol, map[string]any{
			"price": strconv.FormatFloat(t.Price, 'f', -1, 64),
			"time":  t.Time,
		})
		pipe.SAdd(ctx, "symbols", t.Symbol)
		pipe.LPush(ctx, "history:"+t.Symbol, t.Price) // most recent first
		pipe.LTrim(ctx, "history:"+t.Symbol, 0, 99)   // keep last 100
		if _, err := pipe.Exec(ctx); err != nil {
			log.Printf("[processor] redis error: %v", err)
			continue
		}
		ticksProcessed.Inc()
	}
}
