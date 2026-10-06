package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
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

	// k8s sends sigterm on rollouts, drain instead of dying mid-write
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("[processor] consuming %q from %v -> redis %s", trade.Topic, brokers, rdb.Options().Addr)
	for {
		// fetch does not commit, the offset only moves once redis has the tick
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Printf("[processor] shutting down")
				return
			}
			log.Printf("[processor] read error: %v", err)
			time.Sleep(time.Second)
			continue
		}
		var t trade.Trade
		if err := json.Unmarshal(m.Value, &t); err != nil {
			// a bad payload never parses, commit it so it is not redelivered forever
			commit(ctx, reader, m)
			continue
		}
		// retry until redis takes it, skipping ahead would lose the tick
		// because committing a later offset covers this one too
		for {
			err := write(ctx, rdb, t)
			if err == nil {
				break
			}
			if ctx.Err() != nil {
				// not committed, so it is redelivered on the next start
				log.Printf("[processor] shutting down")
				return
			}
			log.Printf("[processor] redis error, retrying: %v", err)
			time.Sleep(time.Second)
		}
		commit(ctx, reader, m)
		ticksProcessed.Inc()
	}
}

// one round trip per tick: latest price, known symbols, rolling history
func write(ctx context.Context, rdb *redis.Client, t trade.Trade) error {
	pipe := rdb.Pipeline()
	pipe.HSet(ctx, "price:"+t.Symbol, map[string]any{
		"price": strconv.FormatFloat(t.Price, 'f', -1, 64),
		"time":  t.Time,
	})
	pipe.SAdd(ctx, "symbols", t.Symbol)
	pipe.LPush(ctx, "history:"+t.Symbol, t.Price) // most recent first
	pipe.LTrim(ctx, "history:"+t.Symbol, 0, 99)   // keep last 100
	_, err := pipe.Exec(ctx)
	return err
}

// a failed commit is not fatal, the tick is redelivered and the write is safe to repeat
func commit(ctx context.Context, r *kafka.Reader, m kafka.Message) {
	if err := r.CommitMessages(ctx, m); err != nil {
		log.Printf("[processor] commit error: %v", err)
	}
}
