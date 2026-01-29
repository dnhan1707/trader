package chat

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/dnhan1707/trader/internal/services"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// rooms[roomID] = set of connections in that room
var rooms = make(map[string]map[*websocket.Conn]bool)

type broadcastMessage struct {
	room    string
	payload []byte
}

var broadcast = make(chan broadcastMessage) // Broadcast channel
var mutex = &sync.Mutex{}                   // Protect rooms map

// DM service (for persistence)
var dmSvc *services.DMService

// Incoming JSON from frontend
type IncomingMessage struct {
	Type    string `json:"type"`
	Sender  string `json:"sender"`  // IMPORTANT: treat this as senderId (user ID)
	Room    string `json:"room"`    // DM threadId / room ID
	Content string `json:"content"` // message text (for type == "message")
}

// Outgoing JSON to all clients
type OutgoingMessage struct {
	Type      string    `json:"type"`
	Sender    string    `json:"sender"`
	Room      string    `json:"room"`
	Content   string    `json:"content"`
	Timestamp time.Time `json:"timestamp"`
}

func wsHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgradeConnection(w, r)
	if err != nil {
		fmt.Println("Error upgrading:", err)
		return
	}
	defer conn.Close()
	defer cleanupConnection(conn)

	handleConnection(conn)
}

func upgradeConnection(w http.ResponseWriter, r *http.Request) (*websocket.Conn, error) {
	return upgrader.Upgrade(w, r, nil)
}

func cleanupConnection(conn *websocket.Conn) {
	mutex.Lock()
	defer mutex.Unlock()

	for roomID, conns := range rooms {
		if _, ok := conns[conn]; ok {
			delete(conns, conn)
			if len(conns) == 0 {
				delete(rooms, roomID)
			}
		}
	}
}

func handleConnection(conn *websocket.Conn) {
	for {
		raw, err := readRawMessage(conn)
		if err != nil {
			break
		}

		in, ok := parseIncomingMessage(raw)
		if !ok || !isValidIncoming(in) {
			continue
		}

		// Handle different message types.
		switch in.Type {
		case "join":
			// Client is subscribing to a room (DM thread) without sending a message.
			registerInRoom(conn, in.Room)
			continue

		case "message":
			// Ensure connection is registered in the room, then persist + broadcast.
			registerInRoom(conn, in.Room)

			payload, ok := buildOutgoingPayload(in)
			if !ok {
				continue
			}

			broadcast <- broadcastMessage{room: in.Room, payload: payload}

		default:
			// should be filtered by isValidIncoming, but guard anyway
			continue
		}
	}
}

func readRawMessage(conn *websocket.Conn) ([]byte, error) {
	_, raw, err := conn.ReadMessage()
	return raw, err
}

func parseIncomingMessage(raw []byte) (IncomingMessage, bool) {
	var in IncomingMessage
	if err := json.Unmarshal(raw, &in); err != nil {
		return IncomingMessage{}, false
	}
	return in, true
}

func isValidIncoming(in IncomingMessage) bool {
	switch in.Type {
	case "join":
		// Join messages only need a room.
		return in.Room != ""
	case "message":
		// Chat messages need both room and non-empty content.
		return in.Room != "" && in.Content != ""
	default:
		return false
	}
}

func registerInRoom(conn *websocket.Conn, room string) {
	mutex.Lock()
	defer mutex.Unlock()

	if rooms[room] == nil {
		rooms[room] = make(map[*websocket.Conn]bool)
	}
	rooms[room][conn] = true
}

func buildOutgoingPayload(in IncomingMessage) ([]byte, bool) {
	// Persist message via DMService if available.
	// Room = threadId, Sender = senderId (user ID).
	var ts time.Time

	if dmSvc != nil {
		msg, err := dmSvc.CreateMessage(context.Background(), in.Room, in.Sender, in.Content)
		if err != nil {
			log.Printf("chat WS: failed to persist DM message (thread=%s, sender=%s): %v", in.Room, in.Sender, err)
			return nil, false
		}
		ts = msg.CreatedAt
	} else {
		// Fallback: no persistence, just use current time
		ts = time.Now().UTC()
	}

	out := OutgoingMessage{
		Type:      "message",
		Sender:    in.Sender,
		Room:      in.Room,
		Content:   in.Content,
		Timestamp: ts,
	}
	payload, err := json.Marshal(out)
	if err != nil {
		return nil, false
	}
	return payload, true
}

func handleMessages() {
	for {
		msg := <-broadcast

		mutex.Lock()
		conns := rooms[msg.room]
		for client := range conns {
			if err := client.WriteMessage(websocket.TextMessage, msg.payload); err != nil {
				client.Close()
				delete(conns, client)
			}
		}
		if len(conns) == 0 {
			delete(rooms, msg.room)
		}
		mutex.Unlock()
	}
}

// Start runs the chat WS server on addr and uses dmService for persistence.
func Start(addr string, svc *services.DMService) {
	dmSvc = svc

	http.HandleFunc("/ws/chat", wsHandler)
	go handleMessages()
	log.Printf("Chat WS server listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
