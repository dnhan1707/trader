package ws

import (
	"encoding/json"
	"time"

	"github.com/gofiber/contrib/websocket"
)

const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer.
	pongWait = 60 * time.Second

	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = (pongWait * 9) / 10
)

// Client is a middleman between the websocket connection and the hub.
type Client struct {
	hub *Hub

	// The websocket connection.
	conn *websocket.Conn

	// Buffered channel of outbound messages.
	send chan []byte
}

// WritePump pumps messages from the Hub to the websocket connection.
// A goroutine running WritePump is started for each connection.
// in other term: Delivers stock stuff to the user.
func (c *Client) WritePump() {
	// A ticker that ticks every 54 seconds (pingPeriod)
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()

	for {
		select {
		// CASE 1: The Hub sent us a Stock Price
		case message, ok := <-c.send:
			// Security: Give the write operation 10 seconds to finish.
			// If the user's internet is so slow that it takes >10s to receive 1kb,
			// kill the connection so we don't waste server resources.
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))

			// Check if the Hub closed the channel (Server shutdown)
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			// Actually write the JSON to the network
			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				return
			}

		// CASE 2: The Ticker ticked (Heartbeat)
		case <-ticker.C:
			// Send a "Ping" message.
			// This is a tiny packet that says "Are you still there?"
			// The browser automatically replies with "Pong" (handled in ReadPump).
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// ReadPump pumps messages from the websocket connection to the hub.
// We need this to handle Disconnects and incoming Subscribe requests.
func (c *Client) ReadPump(stockSubs chan string, indexSubs chan string) {
	// CLEANUP: When this function exits (for any reason),
	// unregister the user so the Hub stops trying to send them data.
	defer func() {
		c.hub.Unregister <- c
		c.conn.Close()
	}()

	// CONFIG: Don't let users send massive 10MB messages (Security)
	c.conn.SetReadLimit(512)

	// THE DEAD MAN'S SWITCH:
	// 1. Set a deadline: "If I don't hear ANYTHING for 60 seconds, kill this connection."
	c.conn.SetReadDeadline(time.Now().Add(pongWait))

	// 2. Define the Pong Handler:
	// "If I receive a 'Pong' frame (automatic reply to our Ping),
	//  RESET the deadline for another 60 seconds."
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			break
		}

		var req struct {
			Ticker string `json:"ticker"`
		}
		if err := json.Unmarshal(message, &req); err != nil {
			continue
		}

		// ROUTING LOGIC
		if req.Ticker != "" {
			// Indices in Massive always start with "I:"
			if len(req.Ticker) > 2 && req.Ticker[:2] == "I:" {
				indexSubs <- req.Ticker
			} else {
				stockSubs <- req.Ticker
			}
		}
	}
}
