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

// userConnections[userID] = set of connections for that user
var userConnections = make(map[string]map[*websocket.Conn]bool)

type broadcastMessage struct {
	room    string
	payload []byte
}

var broadcast = make(chan broadcastMessage) // Broadcast channel
var mutex = &sync.Mutex{}                   // Protect rooms map and userConnections

// DM service (for persistence)
var dmSvc *services.DMService

// Group service (for group message persistence)
var groupSvc *services.GroupService

// Incoming JSON from frontend
type IncomingMessage struct {
	Type    string `json:"type"`
	Sender  string `json:"sender"`  // IMPORTANT: treat this as senderId (user ID)
	Room    string `json:"room"`    // DM threadId / room ID
	Content string `json:"content"` // message text (for type == "message")
	UserID  string `json:"userId"`  // used for "register" type
	IsGroup bool   `json:"isGroup"` // true if message is for a group thread
}

// Outgoing JSON to all clients
type OutgoingMessage struct {
	Type      string    `json:"type"`
	Sender    string    `json:"sender"`
	Room      string    `json:"room"`
	Content   string    `json:"content"`
	Timestamp time.Time `json:"timestamp"`
}

// ThreadCreatedMessage is sent to a user when someone creates a DM thread with them
type ThreadCreatedMessage struct {
	Type          string `json:"type"`
	ThreadID      string `json:"threadId"`
	OtherUserID   string `json:"otherUserId"`
	OtherUsername string `json:"otherUsername"`
}

// GroupMemberInfo contains user info for group notifications
type GroupMemberInfo struct {
	UserID   string `json:"userId"`
	Username string `json:"username"`
}

// GroupThreadCreatedMessage is sent to users when they are added to a group
type GroupThreadCreatedMessage struct {
	Type            string            `json:"type"`
	ThreadID        string            `json:"threadId"`
	Name            string            `json:"name"`
	CreatorID       string            `json:"creatorId"`
	CreatorUsername string            `json:"creatorUsername"`
	Members         []GroupMemberInfo `json:"members"`
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

	// Also remove from userConnections
	for userID, conns := range userConnections {
		if _, ok := conns[conn]; ok {
			delete(conns, conn)
			if len(conns) == 0 {
				delete(userConnections, userID)
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
		case "register":
			// Client is registering their userId for direct notifications
			registerUserConnection(conn, in.UserID)
			continue

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
	case "register":
		// Register messages only need a userId.
		return in.UserID != ""
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

func registerUserConnection(conn *websocket.Conn, userID string) {
	mutex.Lock()
	defer mutex.Unlock()

	if userConnections[userID] == nil {
		userConnections[userID] = make(map[*websocket.Conn]bool)
	}
	userConnections[userID][conn] = true
}

func buildOutgoingPayload(in IncomingMessage) ([]byte, bool) {
	// Persist message via appropriate service based on isGroup flag.
	// Room = threadId, Sender = senderId (user ID).
	var ts time.Time

	if in.IsGroup {
		// Group message
		if groupSvc != nil {
			msg, err := groupSvc.CreateGroupMessage(context.Background(), in.Room, in.Sender, in.Content)
			if err != nil {
				log.Printf("chat WS: failed to persist group message (thread=%s, sender=%s): %v", in.Room, in.Sender, err)
				return nil, false
			}
			ts = msg.CreatedAt
		} else {
			ts = time.Now().UTC()
		}
	} else {
		// DM message
		if dmSvc != nil {
			msg, err := dmSvc.CreateMessage(context.Background(), in.Room, in.Sender, in.Content)
			if err != nil {
				log.Printf("chat WS: failed to persist DM message (thread=%s, sender=%s): %v", in.Room, in.Sender, err)
				return nil, false
			}
			ts = msg.CreatedAt
		} else {
			ts = time.Now().UTC()
		}
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

// NotifyUser sends a message to all WebSocket connections for a specific user.
// Returns true if at least one connection was notified.
func NotifyUser(userID string, payload []byte) bool {
	mutex.Lock()
	defer mutex.Unlock()

	conns, ok := userConnections[userID]
	if !ok || len(conns) == 0 {
		return false
	}

	notified := false
	for conn := range conns {
		if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
			conn.Close()
			delete(conns, conn)
		} else {
			notified = true
		}
	}

	if len(conns) == 0 {
		delete(userConnections, userID)
	}

	return notified
}

// NotifyThreadCreated sends a thread_created notification to a specific user.
func NotifyThreadCreated(userID, threadID, otherUserID, otherUsername string) bool {
	msg := ThreadCreatedMessage{
		Type:          "thread_created",
		ThreadID:      threadID,
		OtherUserID:   otherUserID,
		OtherUsername: otherUsername,
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		log.Printf("chat WS: failed to marshal thread_created message: %v", err)
		return false
	}
	return NotifyUser(userID, payload)
}

// NotifyGroupThreadCreated sends a group_thread_created notification to a specific user.
func NotifyGroupThreadCreated(userID, threadID, name, creatorID, creatorUsername string, members []GroupMemberInfo) bool {
	msg := GroupThreadCreatedMessage{
		Type:            "group_thread_created",
		ThreadID:        threadID,
		Name:            name,
		CreatorID:       creatorID,
		CreatorUsername: creatorUsername,
		Members:         members,
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		log.Printf("chat WS: failed to marshal group_thread_created message: %v", err)
		return false
	}
	return NotifyUser(userID, payload)
}

// Start runs the chat WS server on addr and uses dmService and groupService for persistence.
func Start(addr string, dmService *services.DMService, groupService *services.GroupService) {
	dmSvc = dmService
	groupSvc = groupService

	http.HandleFunc("/ws/chat", wsHandler)
	go handleMessages()
	log.Printf("Chat WS server listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
