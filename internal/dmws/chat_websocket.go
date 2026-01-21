package dmws

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/dnhan1707/trader/internal/auth"
	"github.com/dnhan1707/trader/internal/services"
	"github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
)

type Handler struct {
	dmService *services.DMService
	jwtSecret string

	mutex   sync.Mutex
	clients map[string]map[*websocket.Conn]bool // threadID -> set of conns
}

func NewDMWebsocketHandler(dmService *services.DMService, jwtSecret string) fiber.Handler {
	handler := &Handler{
		dmService: dmService,
		jwtSecret: jwtSecret,
		clients:   make(map[string]map[*websocket.Conn]bool),
	}

	return func(ctx *fiber.Ctx) error {
		authHeader := ctx.Get("Authorization")
		if authHeader == "" {
			return ctx.Status(http.StatusUnauthorized).JSON(fiber.Map{"error": "missing authentication token"})
		}

		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			return ctx.Status(http.StatusUnauthorized).JSON(fiber.Map{"error": "invalid auth header"})
		}

		claims, err := auth.ParseToken(parts[1], handler.jwtSecret)
		if err != nil {
			return ctx.Status(http.StatusUnauthorized).JSON(fiber.Map{"error": "invalid token"})
		}
		userID := claims.UserId

		// 2) Get threadId from query param
		threadID := ctx.Query("threadId")
		if threadID == "" {
			return ctx.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "threadId query param required"})
		}

		// check membership before upgrading
		okIn, err := handler.dmService.IsUserInThread(context.Background(), threadID, userID)
		if err != nil {
			return ctx.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "could not verify access"})
		}
		if !okIn {
			return ctx.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": "not a member of this thread"})
		}

		ctx.Locals("userID", userID)
		ctx.Locals("threadID", threadID)

		if websocket.IsWebSocketUpgrade(ctx) {
			return websocket.New(handler.handleConn)(ctx)
		}
		return fiber.ErrUpgradeRequired
	}
}

type incomingMessage struct {
	Type    string `json:"type"`
	Content string `json:"content"`
}

type outgoingMessage struct {
	Type      string    `json:"type"`
	ID        int64     `json:"id"`
	ThreadID  string    `json:"threadId"`
	SenderID  string    `json:"senderId"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"createdAt"`
}

func (h *Handler) addClient(threadID string, conn *websocket.Conn) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if h.clients[threadID] == nil {
		h.clients[threadID] = make(map[*websocket.Conn]bool)
	}
	h.clients[threadID][conn] = true
}

func (h *Handler) removeClient(threadID string, conn *websocket.Conn) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if conns, ok := h.clients[threadID]; ok {
		delete(conns, conn)
		if len(conns) == 0 {
			delete(h.clients, threadID)
		}
	}
}

func (h *Handler) broadcastToThread(threadID string, out outgoingMessage) {
	h.mutex.Lock()
	conns := make([]*websocket.Conn, 0)
	if m, ok := h.clients[threadID]; ok {
		for c := range m {
			conns = append(conns, c)
		}
	}
	h.mutex.Unlock()

	for _, c := range conns {
		if err := c.WriteJSON(out); err != nil {
			log.Printf("failed to broadcast WS DM message: %v", err)
		}
	}
}

func (h *Handler) handleConn(conn *websocket.Conn) {
	defer conn.Close()

	userID, _ := conn.Locals("userID").(string)
	threadID, _ := conn.Locals("threadID").(string)

	log.Printf("WS DM connection opened: user=%s thread=%s", userID, threadID)
	h.addClient(threadID, conn)
	defer h.removeClient(threadID, conn)

	// Step 1: minimal implementation — just read until client disconnects.
	// (No echo/broadcast yet; that will come in the next steps.)
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			log.Printf("WS DM connection closed: user=%s thread=%s err=%v", userID, threadID, err)
			break
		}

		var in incomingMessage
		if err := json.Unmarshal(data, &in); err != nil {
			_ = conn.WriteJSON(fiber.Map{"type": "error", "error": "invalid json"})
			continue
		}

		if in.Type != "message" {
			_ = conn.WriteJSON(fiber.Map{"type": "error", "error": "unsupported message type"})
			continue
		}

		if strings.TrimSpace(in.Content) == "" {
			_ = conn.WriteJSON(fiber.Map{"type": "error", "error": "content required"})
			continue
		}

		msg, err := h.dmService.CreateMessage(context.Background(), threadID, userID, in.Content)
		if err != nil {
			log.Printf("failed to create DM message: %v", err)
			_ = conn.WriteJSON(fiber.Map{"type": "error", "error": "could not save message"})
			continue
		}

		out := outgoingMessage{
			Type:      "message",
			ID:        msg.ID,
			ThreadID:  msg.ThreadID,
			SenderID:  msg.SenderID,
			Content:   msg.Content,
			CreatedAt: msg.CreatedAt,
		}

		h.broadcastToThread(threadID, out)
	}
}
