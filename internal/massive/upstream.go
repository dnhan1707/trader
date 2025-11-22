package massive

import (
	"log"

	"github.com/dnhan1707/trader/internal/ws"
	"github.com/gorilla/websocket"
)

// Cluster URLs
const (
	urlStocks  = "wss://socket.massive.com/stocks"
	urlIndices = "wss://socket.massive.com/indices"
)

// ListenStocks handles the Stocks Cluster
func ListenStocks(apiKey string, hub *ws.Hub, subRequests chan string) {
	connectAndListen(apiKey, urlStocks, hub, subRequests, "Stocks")
}

// ListenIndices handles the Indices Cluster
func ListenIndices(apiKey string, hub *ws.Hub, subRequests chan string) {
	connectAndListen(apiKey, urlIndices, hub, subRequests, "Indices")
}

// Shared logic to avoid code duplication
func connectAndListen(apiKey, url string, hub *ws.Hub, subRequests chan string, name string) {
	log.Printf("[%s] Connecting...", name)
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		log.Printf("[%s] Connection failed: %v", name, err)
		return
	}
	defer conn.Close()

	// Auth
	if err := conn.WriteJSON(map[string]string{"action": "auth", "params": apiKey}); err != nil {
		log.Printf("[%s] Auth failed: %v", name, err)
		return
	}
	log.Printf("[%s] Authenticated!", name)

	// Read Pump (Background)
	go func() {
		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Printf("[%s] Read error: %v", name, err)
				return
			}
			// Push data to the shared Hub
			hub.Broadcast <- message
		}
	}()

	// Write Loop (Responds to Subscribe Requests)
	for ticker := range subRequests {
		log.Printf("[%s] Subscribing to %s", name, ticker)

		// Determine the channels based on the cluster
		var params string
		if name == "Indices" {
			// Indices: V = Value, A = Aggregates (Per Second)
			// Example: "V.I:SPX,A.I:SPX"
			params = "V." + ticker + ",A." + ticker
		} else {
			// Stocks: T = Trades, AM = Aggregates (Minute)
			// Example: "T.AAPL,AM.AAPL"
			params = "T." + ticker + ",AM." + ticker
		}

		msg := map[string]string{
			"action": "subscribe",
			"params": params,
		}
		conn.WriteJSON(msg)
	}
}
