package api

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/dnhan1707/trader/internal/services"
	"github.com/gofiber/fiber/v2"
)

type DMHandler struct {
	dmService *services.DMService
}

func NewDMHandler(dmService *services.DMService) *DMHandler {
	return &DMHandler{dmService: dmService}
}

type createDMThreadRequest struct {
	OtherUserID string `json:"otherUserId"`
}

func (h *DMHandler) SearchUsers(ctx *fiber.Ctx) error {
	q := ctx.Query("q")
	if q == "" {
		return ctx.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "q query param required"})
	}

	limitStr := ctx.Query("limit", "20")
	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit <= 0 {
		limit = 20
	}

	users, err := h.dmService.SearchUsers(context.Background(), q, limit)
	if err != nil {
		return ctx.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "could not search users"})
	}

	return ctx.JSON(users)
}

func (handler *DMHandler) CreateThread(ctx *fiber.Ctx) error {
	var req createDMThreadRequest
	if err := ctx.BodyParser(&req); err != nil {
		return ctx.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "invalid body"})
	}
	if req.OtherUserID == "" {
		return ctx.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "otherUserId required"})
	}
	currentUserID := ctx.Locals("userID").(string)
	thread, err := handler.dmService.GetOrCreateThreadForUsers(context.Background(), currentUserID, req.OtherUserID)
	if err != nil {
		return ctx.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "could not create thread"})
	}

	return ctx.JSON(thread)
}

type sendDMMessageRequest struct {
	Content string `json:"content"`
}

func (h *DMHandler) SendMessage(ctx *fiber.Ctx) error {
	threadID := ctx.Params("threadId")
	if threadID == "" {
		return ctx.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "threadId required"})
	}

	currentUserID, ok := ctx.Locals("userID").(string)
	if !ok || currentUserID == "" {
		return ctx.Status(http.StatusUnauthorized).JSON(fiber.Map{"error": "unauthorized"})
	}

	// check membership
	okIn, err := h.dmService.IsUserInThread(context.Background(), threadID, currentUserID)
	if err != nil {
		return ctx.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "could not verify access"})
	}
	if !okIn {
		return ctx.Status(http.StatusForbidden).JSON(fiber.Map{"error": "not a member of this thread"})
	}

	var req sendDMMessageRequest
	if err := ctx.BodyParser(&req); err != nil {
		return ctx.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "invalid body"})
	}
	if req.Content == "" {
		return ctx.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "content required"})
	}

	msg, err := h.dmService.CreateMessage(context.Background(), threadID, currentUserID, req.Content)
	if err != nil {
		return ctx.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "could not send message"})
	}

	return ctx.JSON(msg)
}

func (h *DMHandler) ListMessages(ctx *fiber.Ctx) error {
	threadID := ctx.Params("threadId")
	if threadID == "" {
		return ctx.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "threadId required"})
	}

	currentUserID, ok := ctx.Locals("userID").(string)
	if !ok || currentUserID == "" {
		return ctx.Status(http.StatusUnauthorized).JSON(fiber.Map{"error": "unauthorized"})
	}

	okIn, err := h.dmService.IsUserInThread(context.Background(), threadID, currentUserID)
	if err != nil {
		return ctx.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "could not verify access"})
	}
	if !okIn {
		return ctx.Status(http.StatusForbidden).JSON(fiber.Map{"error": "not a member of this thread"})
	}

	limitStr := ctx.Query("limit", "50")
	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit <= 0 {
		limit = 50
	}

	msgs, err := h.dmService.ListMessages(context.Background(), threadID, limit)
	if err != nil {
		return ctx.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "could not list messages"})
	}

	return ctx.JSON(msgs)
}
func (h *DMHandler) ListThreads(ctx *fiber.Ctx) error {
	currentUserID, ok := ctx.Locals("userID").(string)
	if !ok || currentUserID == "" {
		return ctx.Status(http.StatusUnauthorized).JSON(fiber.Map{"error": "unauthorized"})
	}

	summaries, err := h.dmService.ListThreadSummariesForUser(context.Background(), currentUserID)
	if err != nil {
		return ctx.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "could not list threads"})
	}

	return ctx.JSON(summaries)
}

func (h *DMHandler) MarkThreadRead(ctx *fiber.Ctx) error {
	threadID := ctx.Params("threadId")
	if threadID == "" {
		return ctx.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "threadId required"})
	}

	currentUserID, ok := ctx.Locals("userID").(string)
	if !ok || currentUserID == "" {
		return ctx.Status(http.StatusUnauthorized).JSON(fiber.Map{"error": "unauthorized"})
	}

	okIn, err := h.dmService.IsUserInThread(context.Background(), threadID, currentUserID)
	if err != nil {
		return ctx.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "could not verify access"})
	}
	if !okIn {
		return ctx.Status(http.StatusForbidden).JSON(fiber.Map{"error": "not a member of this thread"})
	}

	if err := h.dmService.MarkThreadRead(context.Background(), currentUserID, threadID, time.Now().UTC()); err != nil {
		return ctx.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "could not mark thread read"})
	}

	return ctx.SendStatus(http.StatusNoContent)
}
