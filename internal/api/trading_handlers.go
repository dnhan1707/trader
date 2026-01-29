package api

import (
	"fmt"

	"github.com/gofiber/fiber/v2"
)

func (h *Handler) GetShortInterest(c *fiber.Ctx) error {
	// ...existing code for short interest parameters...
	extra := map[string]string{
		"ticker":        c.Query("ticker", ""),
		"ticker.any_of": c.Query("ticker.any_of", ""),
		// ...all other parameters from original implementation...
		"limit": c.Query("limit", ""),
		"sort":  c.Query("sort", ""),
	}

	cacheKey := fmt.Sprintf("short-interest:t=%s:tany=%s:limit=%s:sort=%s",
		extra["ticker"], extra["ticker.any_of"], extra["limit"], extra["sort"])

	return h.cachedJSON(c, cacheKey, func() (interface{}, error) {
		return h.massive.GetShortInterest(extra)
	})
}

func (h *Handler) GetShortVolume(c *fiber.Ctx) error {
	// ...existing code for short volume parameters...
	extra := map[string]string{
		"ticker":        c.Query("ticker", ""),
		"ticker.any_of": c.Query("ticker.any_of", ""),
		// ...all other parameters from original implementation...
		"limit": c.Query("limit", ""),
		"sort":  c.Query("sort", ""),
	}

	cacheKey := fmt.Sprintf("short-volume:t=%s:tany=%s:limit=%s:sort=%s",
		extra["ticker"], extra["ticker.any_of"], extra["limit"], extra["sort"])

	return h.cachedJSON(c, cacheKey, func() (interface{}, error) {
		return h.massive.GetShortVolume(extra)
	})
}
