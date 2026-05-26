package api

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"quote-ticker/internal/ws"
)

type Router struct {
	mux *http.ServeMux
}

func NewRouter(h *Handler, hub *ws.Hub) *Router {
	mux := http.NewServeMux()

	// REST API - kline query
	mux.HandleFunc("GET /api/klines", h.HandleQueryKlines)
	mux.HandleFunc("GET /api/kline/{symbol}/{interval}", h.HandleGetKline)

	// Prometheus metrics
	mux.Handle("GET /metrics", promhttp.Handler())

	// Health
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok"}`))
	})

	// WebSocket (fallback to /ws path)
	mux.HandleFunc("/ws", hub.HandleWS)

	return &Router{mux: mux}
}

func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mux.ServeHTTP(w, req)
}
