/*
This is the generic "Chat Room" for our stock data. It doesn't care where the data comes from
*/

package ws

type Hub struct {
	// registered clients
	clients map[*Client]bool

	// upstream will push data into this channel
	Broadcast chan []byte

	// register request from the clients
	Register chan *Client

	// unregister requests from clients
	Unregister chan *Client
}

func NewHub() *Hub {
	return &Hub{
		Broadcast:  make(chan []byte),
		Register:   make(chan *Client),
		Unregister: make(chan *Client),
		clients:    make(map[*Client]bool),
	}
}

func (h *Hub) Run() {
	for {
		select {
		// new user connected
		case client := <-h.Register:
			h.clients[client] = true

		// a user disconnected
		case client := <-h.Unregister:
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
			}

		// data arrived from upstream.go, this is the Fan-Out idea
		case message := <-h.Broadcast:
			for client := range h.clients {
				select {
				case client.send <- message:
				// If the client's buffer is full or connection is dead,
				// kick them out to prevent blocking the whole server.
				default:
					close(client.send)
					delete(h.clients, client)
				}
			}
		}
	}
}
