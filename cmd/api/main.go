package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"github.com/hirashif/crypto-market-pipeline/internal/obs"
)

var httpRequests = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "api_http_requests_total",
	Help: "api http requests by route and status",
}, []string{"route", "status"})

type priceResp struct {
	Symbol string  `json:"symbol"`
	Price  float64 `json:"price"`
	Time   string  `json:"time"`
}

type historyResp struct {
	Symbol string    `json:"symbol"`
	Prices []float64 `json:"prices"` // most recent first
}

func main() {
	addr := obs.Env("HTTP_ADDR", ":8080")
	rdb := redis.NewClient(&redis.Options{Addr: obs.Env("REDIS_ADDR", "localhost:6379")})

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("GET /prices", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		symbols, _ := rdb.SMembers(ctx, "symbols").Result()
		out := make([]priceResp, 0, len(symbols))
		for _, s := range symbols {
			if p, err := readPrice(ctx, rdb, s); err == nil {
				out = append(out, p)
			}
		}
		writeJSON(w, out)
		httpRequests.WithLabelValues("/prices", "200").Inc()
	})

	mux.HandleFunc("GET /prices/{symbol}", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		p, err := readPrice(ctx, rdb, r.PathValue("symbol"))
		if err != nil {
			httpRequests.WithLabelValues("/prices/{symbol}", "404").Inc()
			http.Error(w, "symbol not found", http.StatusNotFound)
			return
		}
		writeJSON(w, p)
		httpRequests.WithLabelValues("/prices/{symbol}", "200").Inc()
	})

	// rolling window the processor keeps in redis (last 100 ticks)
	mux.HandleFunc("GET /prices/{symbol}/history", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		symbol := r.PathValue("symbol")
		raw, err := rdb.LRange(ctx, "history:"+symbol, 0, -1).Result()
		if err != nil || len(raw) == 0 {
			httpRequests.WithLabelValues("/prices/{symbol}/history", "404").Inc()
			http.Error(w, "no history for symbol", http.StatusNotFound)
			return
		}
		prices := make([]float64, 0, len(raw))
		for _, v := range raw {
			if p, err := strconv.ParseFloat(v, 64); err == nil {
				prices = append(prices, p)
			}
		}
		writeJSON(w, historyResp{Symbol: symbol, Prices: prices})
		httpRequests.WithLabelValues("/prices/{symbol}/history", "200").Inc()
	})

	log.Printf("[api] listening on %s", addr)
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(srv.ListenAndServe())
}

func readPrice(ctx context.Context, rdb *redis.Client, symbol string) (priceResp, error) {
	vals, err := rdb.HGetAll(ctx, "price:"+symbol).Result()
	if err != nil {
		return priceResp{}, err
	}
	if len(vals) == 0 {
		return priceResp{}, redis.Nil
	}
	price, _ := strconv.ParseFloat(vals["price"], 64)
	return priceResp{Symbol: symbol, Price: price, Time: vals["time"]}, nil
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
