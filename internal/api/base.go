package api

import (
	"encoding/json"
	"fmt"

	"github.com/dnhan1707/trader/internal/cache"
	"github.com/dnhan1707/trader/internal/massive"
	"github.com/dnhan1707/trader/internal/services"
	"github.com/gofiber/fiber/v2"
)

type Handler struct {
	cache            *cache.Cache
	massive          *massive.Client
	institutionalSvc *services.InstitutionalOwnershipService
	insiderSvc       *services.InsiderOwnershipService
}

func New(c *cache.Cache, m *massive.Client, inst *services.InstitutionalOwnershipService, insider *services.InsiderOwnershipService) *Handler {
	return &Handler{
		cache:            c,
		massive:          m,
		institutionalSvc: inst,
		insiderSvc:       insider,
	}
}

// cachedJSON: unified cache -> fetch -> async set -> respond flow
func (h *Handler) cachedJSON(c *fiber.Ctx, cacheKey string, fetch func() (interface{}, error)) error {
	if cached, err := h.cache.Get(cacheKey); err == nil {
		return c.Type("json").SendString(cached)
	}

	data, err := fetch()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to marshal response"})
	}

	// fire-and-forget cache write
	go func(k, v string) {
		if err := h.cache.Set(k, v); err != nil {
			fmt.Println("Redis set error:", err)
		}
	}(cacheKey, string(jsonData))

	return c.Type("json").SendString(string(jsonData))
}
