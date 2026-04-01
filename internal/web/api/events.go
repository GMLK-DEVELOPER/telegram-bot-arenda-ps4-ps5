package api

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
)

// ── SSE Event Hub ─────────────────────────────────────────────────────────────

type eventHub struct {
	mu      sync.Mutex
	clients map[chan string]struct{}
}

var hub = &eventHub{
	clients: make(map[chan string]struct{}),
}

func (h *eventHub) subscribe() chan string {
	ch := make(chan string, 4)
	h.mu.Lock()
	h.clients[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *eventHub) unsubscribe(ch chan string) {
	h.mu.Lock()
	delete(h.clients, ch)
	h.mu.Unlock()
	close(ch)
}

func (h *eventHub) broadcast(event, data string) {
	msg := fmt.Sprintf("event: %s\ndata: %s\n\n", event, data)
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.clients {
		select {
		case ch <- msg:
		default: // drop if client is slow
		}
	}
}

// BroadcastEvent sends an SSE event to all connected admin browsers.
// Called from storage/bot when something important happens.
func BroadcastEvent(event, data string) {
	hub.broadcast(event, data)
}

// SSEStream is the GET /api/events endpoint — admin browser connects here.
func SSEStream(c *gin.Context) {
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")

	ch := hub.subscribe()
	defer hub.unsubscribe(ch)

	// Send initial ping so browser knows connection is alive
	fmt.Fprintf(c.Writer, "event: ping\ndata: ok\n\n")
	c.Writer.Flush()

	notify := c.Request.Context().Done()
	for {
		select {
		case <-notify:
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			fmt.Fprint(c.Writer, msg)
			c.Writer.Flush()
		}
	}
}

// SSEHealth is a simple check that SSE is working
func SSEHealth(c *gin.Context) {
	hub.mu.Lock()
	n := len(hub.clients)
	hub.mu.Unlock()
	c.JSON(http.StatusOK, gin.H{"clients": n})
}
