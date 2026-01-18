package api

import (
	"fmt"
	"strings"

	"github.com/gofiber/fiber/v2"
)

func (h *Handler) GetNews(c *fiber.Ctx) error {
	rawSort := c.Query("sort", "published_utc")
	rawOrder := c.Query("order", "")

	var sortField string
	var order string

	parts := strings.Split(rawSort, ".")
	sortField = parts[0]
	if len(parts) == 2 {
		rawOrder = parts[1]
	}

	allowedSort := map[string]bool{
		"published_utc": true,
		"ticker":        true,
	}
	if !allowedSort[sortField] {
		return c.Status(400).JSON(fiber.Map{"error": "invalid sort field; allowed: published_utc,ticker"})
	}

	switch strings.ToLower(rawOrder) {
	case "", "desc", "descending":
		order = "descending"
	case "asc", "ascending":
		order = "ascending"
	default:
		return c.Status(400).JSON(fiber.Map{"error": "invalid order; use asc, desc, ascending, or descending"})
	}

	extra := map[string]string{
		"ticker":            c.Query("ticker", ""),
		"published_utc":     c.Query("published_utc", ""),
		"ticker.gte":        c.Query("ticker.gte", ""),
		"ticker.gt":         c.Query("ticker.gt", ""),
		"ticker.lte":        c.Query("ticker.lte", ""),
		"ticker.lt":         c.Query("ticker.lt", ""),
		"published_utc.gte": c.Query("published_utc.gte", ""),
		"published_utc.gt":  c.Query("published_utc.gt", ""),
		"published_utc.lte": c.Query("published_utc.lte", ""),
		"published_utc.lt":  c.Query("published_utc.lt", ""),
		"order":             order,
		"limit":             c.Query("limit", ""),
		"sort":              sortField,
	}

	cacheKey := fmt.Sprintf(
		"news:t=%s:pub=%s:sort=%s:order=%s:tgte=%s:tgt=%s:tlte=%s:tlt=%s:pgte=%s:pgt=%s:plte=%s:plt=%s:lim=%s",
		extra["ticker"], extra["published_utc"], extra["sort"], extra["order"],
		extra["ticker.gte"], extra["ticker.gt"], extra["ticker.lte"], extra["ticker.lt"],
		extra["published_utc.gte"], extra["published_utc.gt"], extra["published_utc.lte"], extra["published_utc.lt"],
		extra["limit"],
	)

	return h.cachedJSON(c, cacheKey, func() (interface{}, error) {
		return h.massive.GetNews(extra)
	})
}
