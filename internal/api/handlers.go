package api

import (
	"encoding/json"
	"fmt"

	"github.com/dnhan1707/trader/internal/cache"
	"github.com/dnhan1707/trader/internal/massive"
	"github.com/gofiber/fiber/v2"
)

type Handler struct {
	cache   *cache.Cache
	massive *massive.Client
}

func New(c *cache.Cache, m *massive.Client) *Handler {
	return &Handler{cache: c, massive: m}
}

func (h *Handler) GetTickerDetails(c *fiber.Ctx) error {
	symbol := c.Params("symbol")
	if symbol == "" {
		return c.Status(400).JSON(fiber.Map{"error": "symbol is required"})
	}

	cacheKey := "ticker:" + symbol

	// 1. Try get from Redis
	if cached, err := h.cache.Get(cacheKey); err == nil {
		return c.Type("json").SendString(cached)
	}

	// 2. Not in cache fetch from Massive API
	data, err := h.massive.GetTickerDetails(symbol)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	// 3. Convert to JSON string
	jsonData, err := json.Marshal(data)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to marshal response"})
	}

	// 4. Save JSON string to Redis
	if err := h.cache.Set(cacheKey, string(jsonData)); err != nil {
		// Cache errors should NOT break user response
		fmt.Println("Redis set error:", err)
	}
	return c.JSON(data)
}
