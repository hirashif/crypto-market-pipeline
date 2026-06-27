package obs

import (
	"log"
	"net/http"
	"os"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// env var or fallback
func Env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// serve prometheus /metrics and /healthz in the background
func ServeMetrics(addr string) {
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("ok"))
		})
		log.Printf("[metrics] listening on %s", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Printf("[metrics] server error: %v", err)
		}
	}()
}
