package api

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/dnhan1707/trader/internal/chat"
	"github.com/dnhan1707/trader/internal/services"
	"github.com/gofiber/fiber/v2"
)

type GroupHandler struct {
	groupService *services.GroupService
}

func NewGroupHandler(groupService *services.GroupService) *GroupHandler {
	return &GroupHandler{groupService: groupService}
}

type createGroupThreadRequest struct {
	Name      string   `json:"name"`
	MemberIDs []string `json:"memberIds"`
	CreatorID string   `json:"creatorId"` // Required: the user creating the group
}

type addMembersRequest struct {
	MemberIDs []string `json:"memberIds"`
	UserID    string   `json:"userId"` // Required: the user adding members
}

// getUserID gets user ID from query param or falls back to auth context
func getUserID(ctx *fiber.Ctx) string {
	// First try query param
	if uid := ctx.Query("userId"); uid != "" {
		return uid
	}
	// Fall back to auth context (if middleware is used)
	if uid, ok := ctx.Locals("userID").(string); ok {
		return uid
	}
	return ""
}

// CreateGroupThread creates a new group chat with multiple members.
// POST /api/chat/group/thread
func (h *GroupHandler) CreateGroupThread(ctx *fiber.Ctx) error {
	var req createGroupThreadRequest
	if err := ctx.BodyParser(&req); err != nil {
		return ctx.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "invalid body"})
	}
	if req.Name == "" {
		return ctx.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "name required"})
	}
	if len(req.MemberIDs) == 0 {
		return ctx.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "at least one member required"})
	}
	if req.CreatorID == "" {
		return ctx.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "creatorId required"})
	}

	currentUserID := req.CreatorID

	thread, err := h.groupService.CreateGroupThread(context.Background(), req.Name, currentUserID, req.MemberIDs)
	if err != nil {
		return ctx.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "could not create group thread"})
	}

	// Get creator's username for notifications
	creatorUsername, _ := h.groupService.GetUsernameByID(context.Background(), currentUserID)

	// Get all member usernames for the notification
	allMemberIDs := append([]string{currentUserID}, req.MemberIDs...)
	memberUsernames, _ := h.groupService.GetUsernamesByIDs(context.Background(), allMemberIDs)

	// Build members list for notification
	var members []chat.GroupMemberInfo
	for _, id := range allMemberIDs {
		members = append(members, chat.GroupMemberInfo{
			UserID:   id,
			Username: memberUsernames[id],
		})
	}

	// Notify all other members via WebSocket
	for _, memberID := range req.MemberIDs {
		if memberID != currentUserID {
			chat.NotifyGroupThreadCreated(memberID, thread.ID, req.Name, currentUserID, creatorUsername, members)
		}
	}

	// Return thread with members
	membersResult, _ := h.groupService.GetGroupMembers(context.Background(), thread.ID)

	return ctx.JSON(fiber.Map{
		"id":        thread.ID,
		"name":      thread.Name,
		"creatorId": thread.CreatorID,
		"createdAt": thread.CreatedAt,
		"members":   membersResult,
	})
}

// ListGroupThreads returns all group threads the user is a member of.
// GET /api/chat/group/threads?userId=xxx
func (h *GroupHandler) ListGroupThreads(ctx *fiber.Ctx) error {
	currentUserID := getUserID(ctx)
	if currentUserID == "" {
		return ctx.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "userId query param required"})
	}

	summaries, err := h.groupService.ListGroupThreadsForUser(context.Background(), currentUserID)
	if err != nil {
		return ctx.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "could not list group threads"})
	}

	return ctx.JSON(summaries)
}

// GetGroupThread returns details of a specific group thread.
// GET /api/chat/group/threads/:threadId?userId=xxx
func (h *GroupHandler) GetGroupThread(ctx *fiber.Ctx) error {
	threadID := ctx.Params("threadId")
	if threadID == "" {
		return ctx.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "threadId required"})
	}

	currentUserID := getUserID(ctx)
	if currentUserID == "" {
		return ctx.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "userId query param required"})
	}

	// Check membership
	isMember, err := h.groupService.IsUserInGroupThread(context.Background(), threadID, currentUserID)
	if err != nil {
		return ctx.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "could not verify access"})
	}
	if !isMember {
		return ctx.Status(http.StatusForbidden).JSON(fiber.Map{"error": "not a member of this group"})
	}

	thread, err := h.groupService.GetGroupThread(context.Background(), threadID)
	if err != nil {
		return ctx.Status(http.StatusNotFound).JSON(fiber.Map{"error": "group not found"})
	}

	members, err := h.groupService.GetGroupMembers(context.Background(), threadID)
	if err != nil {
		return ctx.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "could not get members"})
	}

	return ctx.JSON(fiber.Map{
		"id":        thread.ID,
		"name":      thread.Name,
		"creatorId": thread.CreatorID,
		"createdAt": thread.CreatedAt,
		"members":   members,
	})
}

// SendGroupMessage sends a message to a group thread.
// POST /api/chat/group/threads/:threadId/messages
func (h *GroupHandler) SendGroupMessage(ctx *fiber.Ctx) error {
	threadID := ctx.Params("threadId")
	if threadID == "" {
		return ctx.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "threadId required"})
	}

	var req struct {
		Content  string `json:"content"`
		SenderID string `json:"senderId"`
	}
	if err := ctx.BodyParser(&req); err != nil {
		return ctx.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "invalid body"})
	}
	if req.Content == "" {
		return ctx.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "content required"})
	}
	if req.SenderID == "" {
		return ctx.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "senderId required"})
	}

	currentUserID := req.SenderID

	// Check membership
	isMember, err := h.groupService.IsUserInGroupThread(context.Background(), threadID, currentUserID)
	if err != nil {
		return ctx.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "could not verify access"})
	}
	if !isMember {
		return ctx.Status(http.StatusForbidden).JSON(fiber.Map{"error": "not a member of this group"})
	}

	msg, err := h.groupService.CreateGroupMessage(context.Background(), threadID, currentUserID, req.Content)
	if err != nil {
		return ctx.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "could not send message"})
	}

	return ctx.JSON(msg)
}

// ListGroupMessages returns messages from a group thread.
// GET /api/chat/group/threads/:threadId/messages?userId=xxx
func (h *GroupHandler) ListGroupMessages(ctx *fiber.Ctx) error {
	threadID := ctx.Params("threadId")
	if threadID == "" {
		return ctx.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "threadId required"})
	}

	currentUserID := getUserID(ctx)
	if currentUserID == "" {
		return ctx.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "userId query param required"})
	}

	// Check membership
	isMember, err := h.groupService.IsUserInGroupThread(context.Background(), threadID, currentUserID)
	if err != nil {
		return ctx.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "could not verify access"})
	}
	if !isMember {
		return ctx.Status(http.StatusForbidden).JSON(fiber.Map{"error": "not a member of this group"})
	}

	limitStr := ctx.Query("limit", "50")
	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit <= 0 {
		limit = 50
	}

	msgs, err := h.groupService.ListGroupMessages(context.Background(), threadID, limit)
	if err != nil {
		return ctx.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "could not list messages"})
	}

	return ctx.JSON(msgs)
}

// MarkGroupThreadRead marks the group thread as read.
// POST /api/chat/group/threads/:threadId/read
func (h *GroupHandler) MarkGroupThreadRead(ctx *fiber.Ctx) error {
	threadID := ctx.Params("threadId")
	if threadID == "" {
		return ctx.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "threadId required"})
	}

	var req struct {
		UserID string `json:"userId"`
	}
	if err := ctx.BodyParser(&req); err != nil {
		return ctx.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "invalid body"})
	}
	currentUserID := req.UserID
	if currentUserID == "" {
		currentUserID = getUserID(ctx)
	}
	if currentUserID == "" {
		return ctx.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "userId required"})
	}

	// Check membership
	isMember, err := h.groupService.IsUserInGroupThread(context.Background(), threadID, currentUserID)
	if err != nil {
		return ctx.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "could not verify access"})
	}
	if !isMember {
		return ctx.Status(http.StatusForbidden).JSON(fiber.Map{"error": "not a member of this group"})
	}

	if err := h.groupService.MarkGroupThreadRead(context.Background(), currentUserID, threadID, time.Now().UTC()); err != nil {
		return ctx.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "could not mark thread read"})
	}

	return ctx.SendStatus(http.StatusNoContent)
}

// AddMembers adds new members to a group thread.
// POST /api/chat/group/threads/:threadId/members
func (h *GroupHandler) AddMembers(ctx *fiber.Ctx) error {
	threadID := ctx.Params("threadId")
	if threadID == "" {
		return ctx.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "threadId required"})
	}

	var req addMembersRequest
	if err := ctx.BodyParser(&req); err != nil {
		return ctx.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "invalid body"})
	}
	if len(req.MemberIDs) == 0 {
		return ctx.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "memberIds required"})
	}
	if req.UserID == "" {
		return ctx.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "userId required"})
	}

	currentUserID := req.UserID

	// Check membership
	isMember, err := h.groupService.IsUserInGroupThread(context.Background(), threadID, currentUserID)
	if err != nil {
		return ctx.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "could not verify access"})
	}
	if !isMember {
		return ctx.Status(http.StatusForbidden).JSON(fiber.Map{"error": "not a member of this group"})
	}

	if err := h.groupService.AddMembersToGroup(context.Background(), threadID, req.MemberIDs); err != nil {
		return ctx.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "could not add members"})
	}

	// Get group info for notification
	thread, _ := h.groupService.GetGroupThread(context.Background(), threadID)
	adderUsername, _ := h.groupService.GetUsernameByID(context.Background(), currentUserID)
	allMembers, _ := h.groupService.GetGroupMembers(context.Background(), threadID)

	var memberInfos []chat.GroupMemberInfo
	for _, m := range allMembers {
		memberInfos = append(memberInfos, chat.GroupMemberInfo{
			UserID:   m.UserID,
			Username: m.Username,
		})
	}

	// Notify new members
	for _, memberID := range req.MemberIDs {
		chat.NotifyGroupThreadCreated(memberID, threadID, thread.Name, currentUserID, adderUsername, memberInfos)
	}

	return ctx.JSON(fiber.Map{"success": true})
}

// LeaveGroup removes a user from a group thread.
// POST /api/chat/group/threads/:threadId/leave
func (h *GroupHandler) LeaveGroup(ctx *fiber.Ctx) error {
	threadID := ctx.Params("threadId")
	if threadID == "" {
		return ctx.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "threadId required"})
	}

	var req struct {
		UserID string `json:"userId"`
	}
	if err := ctx.BodyParser(&req); err != nil {
		return ctx.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "invalid body"})
	}
	if req.UserID == "" {
		return ctx.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "userId required"})
	}

	currentUserID := req.UserID

	// Check membership
	isMember, err := h.groupService.IsUserInGroupThread(context.Background(), threadID, currentUserID)
	if err != nil {
		return ctx.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "could not verify access"})
	}
	if !isMember {
		return ctx.Status(http.StatusForbidden).JSON(fiber.Map{"error": "not a member of this group"})
	}

	if err := h.groupService.RemoveMemberFromGroup(context.Background(), threadID, currentUserID); err != nil {
		return ctx.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "could not leave group"})
	}

	return ctx.SendStatus(http.StatusNoContent)
}
