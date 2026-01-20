package api

import (
	"context"
	"database/sql"
	"net/http"
	"time"

	"github.com/dnhan1707/trader/internal/auth"
	"github.com/dnhan1707/trader/internal/services"
	"github.com/gofiber/fiber/v2"
)

type AuthHandler struct {
	authService  *services.AuthService
	jwtSecret    string
	jwtExpiresIn string
}

func NewAuthHandler(authService *services.AuthService, jwtSecret, jwtExpiresIn string) *AuthHandler {
	return &AuthHandler{authService: authService, jwtSecret: jwtSecret, jwtExpiresIn: jwtExpiresIn}
}

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type signUpRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func (h *AuthHandler) SignUp(c *fiber.Ctx) error {
	var req signUpRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "invalid body", "details": err.Error()})
	}
	if req.Username == "" || req.Password == "" {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "username and password required"})
	}

	// Check if user already exists
	if _, err := h.authService.GetByUsername(context.Background(), req.Username); err == nil {
		return c.Status(http.StatusConflict).JSON(fiber.Map{"error": "username already exists"})
	} else if err != sql.ErrNoRows {
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "could not check username"})
	}

	user, err := h.authService.CreateUser(context.Background(), req.Username, req.Password)
	if err != nil {
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "could not create user"})
	}

	return c.Status(http.StatusCreated).JSON(fiber.Map{
		"user": fiber.Map{
			"id":       user.ID,
			"username": user.Username,
		},
	})
}

func (h *AuthHandler) Login(c *fiber.Ctx) error {
	var req loginRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "invalid body", "details": err.Error()})
	}
	if req.Username == "" || req.Password == "" {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "username and password required"})
	}

	u, err := h.authService.GetByUsername(context.Background(), req.Username)
	if err != nil {
		return c.Status(http.StatusUnauthorized).JSON(fiber.Map{"error": "invalid credentials"})
	}
	if err := h.authService.CheckPassword(u, req.Password); err != nil {
		return c.Status(http.StatusUnauthorized).JSON(fiber.Map{"error": "invalid credentials"})
	}

	duration, err := time.ParseDuration(h.jwtExpiresIn)
	if err != nil {
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "invalid token duration"})
	}

	token, err := auth.GenerateToken(u.ID, h.jwtSecret, duration)
	if err != nil {
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{"error": "token error"})
	}

	return c.JSON(fiber.Map{
		"accessToken": token,
		"user": fiber.Map{
			"id":       u.ID,
			"username": u.Username,
		},
	})
}

func (h *AuthHandler) Logout(c *fiber.Ctx) error {
	return c.SendStatus(http.StatusNoContent)
}
