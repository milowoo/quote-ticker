package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"quote-ticker/internal/model"
	"quote-ticker/internal/repository"
)

// KlineQueryResult is the JSON response for kline queries.
type KlineQueryResult struct {
	Symbol   string                `json:"s"`
	Interval string                `json:"i"`
	Klines   []map[string]interface{} `json:"k"`
}

// Handler holds HTTP handlers.
type Handler struct {
	repo *repository.KlineRepo
}

func NewHandler(repo *repository.KlineRepo) *Handler {
	return &Handler{repo: repo}
}

// HandleQueryKlines handles GET /api/klines?symbol=XXX&interval=1h&startTime=...&endTime=...&limit=...
func (h *Handler) HandleQueryKlines(w http.ResponseWriter, r *http.Request) {
	symbol := strings.ToUpper(r.URL.Query().Get("symbol"))
	interval := r.URL.Query().Get("interval")

	if symbol == "" || interval == "" {
		http.Error(w, `{"error":"symbol and interval required"}`, http.StatusBadRequest)
		return
	}

	now := time.Now().UnixMilli()
	startTime := parseInt64(r.URL.Query().Get("startTime"), now-24*60*60*1000)
	endTime := parseInt64(r.URL.Query().Get("endTime"), now)
	limit := parseInt(r.URL.Query().Get("limit"), 500)
	if limit > 1000 {
		limit = 1000
	}

	klines, err := h.repo.Query(r.Context(), symbol, interval, startTime, endTime, limit)
	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}

	resp := KlineQueryResult{
		Symbol:   symbol,
		Interval: interval,
		Klines:   marshalKlines(klines),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// HandleGetKline handles GET /api/kline/{symbol}/{interval}?startTime=...&endTime=...
func (h *Handler) HandleGetKline(w http.ResponseWriter, r *http.Request) {
	symbol := strings.ToUpper(r.PathValue("symbol"))
	interval := r.PathValue("interval")

	if symbol == "" || interval == "" {
		http.Error(w, `{"error":"symbol and interval required"}`, http.StatusBadRequest)
		return
	}

	now := time.Now().UnixMilli()
	startTime := parseInt64(r.URL.Query().Get("startTime"), now-24*60*60*1000)
	endTime := parseInt64(r.URL.Query().Get("endTime"), now)
	limit := parseInt(r.URL.Query().Get("limit"), 500)
	if limit > 1000 {
		limit = 1000
	}

	klines, err := h.repo.Query(r.Context(), symbol, interval, startTime, endTime, limit)
	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(klines)
}

func marshalKlines(klines []*model.Kline) []map[string]interface{} {
	out := make([]map[string]interface{}, len(klines))
	for i, k := range klines {
		out[i] = map[string]interface{}{
			"t": k.StartTime,
			"T": k.CloseTime,
			"o": k.Open.Trimmed(),
			"h": k.High.Trimmed(),
			"l": k.Low.Trimmed(),
			"c": k.Close.Trimmed(),
			"v": k.Volume.Trimmed(),
			"q": k.Amount.Trimmed(),
			"n": k.TradeCount,
			"V": k.BuyTakerVol.Trimmed(),
			"Q": k.BuyTakerAmt.Trimmed(),
			"w": k.WeightedAvg.Trimmed(),
		}
	}
	return out
}

func parseInt64(s string, def int64) int64 {
	if s == "" {
		return def
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return def
	}
	return v
}

func parseInt(s string, def int) int {
	if s == "" {
		return def
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return v
}
