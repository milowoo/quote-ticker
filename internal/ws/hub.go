package ws

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"quote-ticker/internal/metrics"
	"quote-ticker/internal/model"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 8192
)

// Hub manages WebSocket connections and symbol subscriptions.
type Hub struct {
	mu    sync.RWMutex
	conns map[*conn]bool
	subs  map[string]map[*conn]bool // symbol -> conns

	// Per-symbol broadcast channels (avoids serializing JSON on the hot path).
	broadcastChans map[string]chan model.Trade
	broadcastOnce  sync.Once
}

type conn struct {
	hub    *Hub
	ws     *websocket.Conn
	send   chan []byte
	subbed map[string]bool
}

func NewHub() *Hub {
	h := &Hub{
		conns:          make(map[*conn]bool),
		subs:           make(map[string]map[*conn]bool),
		broadcastChans: make(map[string]chan model.Trade),
	}
	return h
}

func (h *Hub) ensureBroadcaster(symbol string) chan model.Trade {
	h.mu.Lock()
	defer h.mu.Unlock()

	if ch, ok := h.broadcastChans[symbol]; ok {
		return ch
	}
	ch := make(chan model.Trade, 256)
	h.broadcastChans[symbol] = ch
	go h.broadcastLoop(symbol, ch)
	return ch
}

// broadcastLoop is a per-symbol goroutine that serializes and dispatches trades.
func (h *Hub) broadcastLoop(symbol string, ch <-chan model.Trade) {
	var buf []byte
	for trade := range ch {
		// Reuse buffer across marshals (in practice, json.Marshal allocates anyway,
		// but this isolates the cost to the broadcast goroutine).
		var err error
		buf, err = json.Marshal(map[string]interface{}{
			"type": "trade",
			"data": trade,
		})
		if err != nil {
			continue
		}

		h.mu.RLock()
		subs := h.subs[symbol]
		h.mu.RUnlock()

		for c := range subs {
			select {
			case c.send <- buf:
			default:
				// Slow consumer — drop message.
			}
		}
	}
}

// BroadcastTrade pushes a trade to all connections subscribed to its symbol.
func (h *Hub) BroadcastTrade(trade model.Trade) {
	ch := h.ensureBroadcaster(trade.Symbol)
	select {
	case ch <- trade:
	default:
		// Channel full — drop trade.
	}
}

// HandleWS is the HTTP handler for WebSocket upgrade.
func (h *Hub) HandleWS(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("ws upgrade: %v", err)
		return
	}

	c := &conn{
		hub:    h,
		ws:     ws,
		send:   make(chan []byte, 256),
		subbed: make(map[string]bool),
	}

	h.mu.Lock()
	h.conns[c] = true
	metrics.WSConnections.Set(float64(len(h.conns)))
	h.mu.Unlock()

	go c.writePump()
	c.readPump()

	h.mu.Lock()
	metrics.WSConnections.Set(float64(len(h.conns)))
	h.mu.Unlock()
}

func (h *Hub) subscribe(c *conn, symbol string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.subs[symbol]; !ok {
		h.subs[symbol] = make(map[*conn]bool)
	}
	h.subs[symbol][c] = true
	c.subbed[symbol] = true
	metrics.WSSubscriptions.WithLabelValues(symbol).Set(float64(len(h.subs[symbol])))
}

func (h *Hub) unsubscribe(c *conn, symbol string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if subs, ok := h.subs[symbol]; ok {
		delete(subs, c)
		if len(subs) == 0 {
			delete(h.subs, symbol)
			metrics.WSSubscriptions.DeleteLabelValues(symbol)
			return
		}
		metrics.WSSubscriptions.WithLabelValues(symbol).Set(float64(len(subs)))
	}
	delete(c.subbed, symbol)
}

func (h *Hub) removeConn(c *conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for sym := range c.subbed {
		if subs, ok := h.subs[sym]; ok {
			delete(subs, c)
			if len(subs) == 0 {
				delete(h.subs, sym)
			}
		}
	}
	delete(h.conns, c)
	close(c.send)
}

func (c *conn) readPump() {
	defer func() {
		c.hub.removeConn(c)
		c.ws.Close()
	}()

	c.ws.SetReadLimit(maxMessageSize)
	c.ws.SetReadDeadline(time.Now().Add(pongWait))
	c.ws.SetPongHandler(func(string) error {
		c.ws.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, msg, err := c.ws.ReadMessage()
		if err != nil {
			break
		}

		var req struct {
			Action string `json:"action"`
			Symbol string `json:"symbol"`
		}
		if err := json.Unmarshal(msg, &req); err != nil {
			continue
		}

		switch req.Action {
		case "subscribe":
			c.hub.subscribe(c, req.Symbol)
		case "unsubscribe":
			c.hub.unsubscribe(c, req.Symbol)
		}
	}
}

func (c *conn) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.ws.Close()
	}()

	for {
		select {
		case msg, ok := <-c.send:
			c.ws.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.ws.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.ws.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			c.ws.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.ws.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
