package api

import (
	"fmt"

	"github.com/gofiber/fiber/v2"
)

func (h *Handler) GetSMA(c *fiber.Ctx) error {
	stocksTicker := c.Params("stocksTicker")
	if stocksTicker == "" {
		return c.Status(400).JSON(fiber.Map{"error": "missing stocksTicker"})
	}
	extra := map[string]string{
		"timestamp":         c.Query("timestamp", ""),
		"timespan":          c.Query("timespan", ""),
		"adjusted":          c.Query("adjusted", ""),
		"window":            c.Query("window", ""),
		"series_type":       c.Query("series_type", ""),
		"expand_underlying": c.Query("expand_underlying", ""),
		"order":             c.Query("order", ""),
		"limit":             c.Query("limit", ""),
		"timestamp.gte":     c.Query("timestamp.gte", ""),
		"timestamp.gt":      c.Query("timestamp.gt", ""),
		"timestamp.lte":     c.Query("timestamp.lte", ""),
		"timestamp.lt":      c.Query("timestamp.lt", ""),
	}
	cacheKey := fmt.Sprintf("sma:%s:ts=%s:tsps=%s:adj=%s:w=%s:st=%s:exp=%s:ord=%s:lim=%s:gte=%s:gt=%s:lte=%s:lt=%s",
		stocksTicker, extra["timestamp"], extra["timespan"], extra["adjusted"], extra["window"],
		extra["series_type"], extra["expand_underlying"], extra["order"], extra["limit"],
		extra["timestamp.gte"], extra["timestamp.gt"], extra["timestamp.lte"], extra["timestamp.lt"],
	)
	return h.cachedJSON(c, cacheKey, func() (interface{}, error) {
		return h.massive.GetSMA(stocksTicker, extra)
	})
}

func (h *Handler) GetEMA(c *fiber.Ctx) error {
	stocksTicker := c.Params("stocksTicker")
	if stocksTicker == "" {
		return c.Status(400).JSON(fiber.Map{"error": "missing stocksTicker"})
	}
	extra := map[string]string{
		"timestamp":         c.Query("timestamp", ""),
		"timespan":          c.Query("timespan", ""),
		"adjusted":          c.Query("adjusted", ""),
		"window":            c.Query("window", ""),
		"series_type":       c.Query("series_type", ""),
		"expand_underlying": c.Query("expand_underlying", ""),
		"order":             c.Query("order", ""),
		"limit":             c.Query("limit", ""),
		"timestamp.gte":     c.Query("timestamp.gte", ""),
		"timestamp.gt":      c.Query("timestamp.gt", ""),
		"timestamp.lte":     c.Query("timestamp.lte", ""),
		"timestamp.lt":      c.Query("timestamp.lt", ""),
	}
	cacheKey := fmt.Sprintf("ema:%s:ts=%s:tsps=%s:adj=%s:w=%s:st=%s:exp=%s:ord=%s:lim=%s:gte=%s:gt=%s:lte=%s:lt=%s",
		stocksTicker, extra["timestamp"], extra["timespan"], extra["adjusted"], extra["window"],
		extra["series_type"], extra["expand_underlying"], extra["order"], extra["limit"],
		extra["timestamp.gte"], extra["timestamp.gt"], extra["timestamp.lte"], extra["timestamp.lt"],
	)
	return h.cachedJSON(c, cacheKey, func() (interface{}, error) {
		return h.massive.GetEMA(stocksTicker, extra)
	})
}

func (h *Handler) GetMACD(c *fiber.Ctx) error {
	stocksTicker := c.Params("stocksTicker")
	if stocksTicker == "" {
		return c.Status(400).JSON(fiber.Map{"error": "missing stocksTicker"})
	}
	extra := map[string]string{
		"timestamp":         c.Query("timestamp", ""),
		"timespan":          c.Query("timespan", ""),
		"adjusted":          c.Query("adjusted", ""),
		"short_window":      c.Query("short_window", ""),
		"long_window":       c.Query("long_window", ""),
		"signal_window":     c.Query("signal_window", ""),
		"series_type":       c.Query("series_type", ""),
		"expand_underlying": c.Query("expand_underlying", ""),
		"order":             c.Query("order", ""),
		"limit":             c.Query("limit", ""),
		"timestamp.gte":     c.Query("timestamp.gte", ""),
		"timestamp.gt":      c.Query("timestamp.gt", ""),
		"timestamp.lte":     c.Query("timestamp.lte", ""),
		"timestamp.lt":      c.Query("timestamp.lt", ""),
	}
	cacheKey := fmt.Sprintf("macd:%s:ts=%s:tsps=%s:adj=%s:sw=%s:lw=%s:sig=%s:st=%s:exp=%s:ord=%s:lim=%s:gte=%s:gt=%s:lte=%s:lt=%s",
		stocksTicker, extra["timestamp"], extra["timespan"], extra["adjusted"],
		extra["short_window"], extra["long_window"], extra["signal_window"],
		extra["series_type"], extra["expand_underlying"], extra["order"], extra["limit"],
		extra["timestamp.gte"], extra["timestamp.gt"], extra["timestamp.lte"], extra["timestamp.lt"],
	)
	return h.cachedJSON(c, cacheKey, func() (interface{}, error) {
		return h.massive.GetMACD(stocksTicker, extra)
	})
}

func (h *Handler) GetRSI(c *fiber.Ctx) error {
	stocksTicker := c.Params("stocksTicker")
	if stocksTicker == "" {
		return c.Status(400).JSON(fiber.Map{"error": "missing stocksTicker"})
	}
	extra := map[string]string{
		"timestamp":         c.Query("timestamp", ""),
		"timespan":          c.Query("timespan", ""),
		"adjusted":          c.Query("adjusted", ""),
		"window":            c.Query("window", ""),
		"series_type":       c.Query("series_type", ""),
		"expand_underlying": c.Query("expand_underlying", ""),
		"order":             c.Query("order", ""),
		"limit":             c.Query("limit", ""),
		"timestamp.gte":     c.Query("timestamp.gte", ""),
		"timestamp.gt":      c.Query("timestamp.gt", ""),
		"timestamp.lte":     c.Query("timestamp.lte", ""),
		"timestamp.lt":      c.Query("timestamp.lt", ""),
	}
	cacheKey := fmt.Sprintf("rsi:%s:ts=%s:tsps=%s:adj=%s:w=%s:st=%s:exp=%s:ord=%s:lim=%s:gte=%s:gt=%s:lte=%s:lt=%s",
		stocksTicker, extra["timestamp"], extra["timespan"], extra["adjusted"], extra["window"],
		extra["series_type"], extra["expand_underlying"], extra["order"], extra["limit"],
		extra["timestamp.gte"], extra["timestamp.gt"], extra["timestamp.lte"], extra["timestamp.lt"],
	)
	return h.cachedJSON(c, cacheKey, func() (interface{}, error) {
		return h.massive.GetRSI(stocksTicker, extra)
	})
}
