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

func (h *Handler) GetCustomBars(c *fiber.Ctx) error {
	stocksTicker := c.Params("stocksTicker")
	multiplier := c.Params("multiplier")
	timespan := c.Params("timespan")
	from := c.Params("from")
	to := c.Params("to")

	if stocksTicker == "" || multiplier == "" || timespan == "" || from == "" || to == "" {
		return c.Status(400).JSON(fiber.Map{"error": "missing required path parameter"})
	}

	extra := map[string]string{
		"adjusted": c.Query("adjusted", ""),
		"sort":     c.Query("sort", ""),
		"limit":    c.Query("limit", ""),
	}

	cacheKey := fmt.Sprintf("aggs:%s:%s:%s:%s:%s:adj=%s:sort=%s:limit=%s",
		stocksTicker, multiplier, timespan, from, to,
		extra["adjusted"], extra["sort"], extra["limit"],
	)

	if cached, err := h.cache.Get(cacheKey); err == nil {
		return c.Type("json").SendString(cached)
	}

	data, err := h.massive.GetCustomBars(stocksTicker, multiplier, timespan, from, to, extra)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to marshal response"})
	}
	if err := h.cache.Set(cacheKey, string(jsonData)); err != nil {
		// log cache set error but do not fail the request
		fmt.Println("Redis set error:", err)
	}

	return c.JSON(data)
}

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

		// range helpers
		"timestamp.gte": c.Query("timestamp.gte", ""),
		"timestamp.gt":  c.Query("timestamp.gt", ""),
		"timestamp.lte": c.Query("timestamp.lte", ""),
		"timestamp.lt":  c.Query("timestamp.lt", ""),
	}
	// build a cache key that includes relevant params
	cacheKey := fmt.Sprintf("sma:%s:ts=%s:tsps=%s:adj=%s:w=%s:st=%s:exp=%s:ord=%s:lim=%s:gte=%s:gt=%s:lte=%s:lt=%s",
		stocksTicker,
		extra["timestamp"], extra["timespan"], extra["adjusted"], extra["window"],
		extra["series_type"], extra["expand_underlying"], extra["order"], extra["limit"],
		extra["timestamp.gte"], extra["timestamp.gt"], extra["timestamp.lte"], extra["timestamp.lt"],
	)

	// try cache
	if cached, err := h.cache.Get(cacheKey); err == nil {
		return c.Type("json").SendString(cached)
	}

	// fetch from Massive
	data, err := h.massive.GetSMA(stocksTicker, extra)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	// marshal and cache
	jsonData, err := json.Marshal(data)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to marshal response"})
	}
	if err := h.cache.Set(cacheKey, string(jsonData)); err != nil {
		// log but don't fail the response
		fmt.Println("Redis set error:", err)
	}

	return c.JSON(data)
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

		"timestamp.gte": c.Query("timestamp.gte", ""),
		"timestamp.gt":  c.Query("timestamp.gt", ""),
		"timestamp.lte": c.Query("timestamp.lte", ""),
		"timestamp.lt":  c.Query("timestamp.lt", ""),
	}
	cacheKey := fmt.Sprintf("ema:%s:ts=%s:tsps=%s:adj=%s:w=%s:st=%s:exp=%s:ord=%s:lim=%s:gte=%s:gt=%s:lte=%s:lt=%s",
		stocksTicker,
		extra["timestamp"], extra["timespan"], extra["adjusted"], extra["window"],
		extra["series_type"], extra["expand_underlying"], extra["order"], extra["limit"],
		extra["timestamp.gte"], extra["timestamp.gt"], extra["timestamp.lte"], extra["timestamp.lt"],
	)

	if cached, err := h.cache.Get(cacheKey); err == nil {
		return c.Type("json").SendString(cached)
	}

	data, err := h.massive.GetEMA(stocksTicker, extra)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to marshal response"})
	}
	if err := h.cache.Set(cacheKey, string(jsonData)); err != nil {
		fmt.Println("Redis set error:", err)
	}

	return c.JSON(data)
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

		"timestamp.gte": c.Query("timestamp.gte", ""),
		"timestamp.gt":  c.Query("timestamp.gt", ""),
		"timestamp.lte": c.Query("timestamp.lte", ""),
		"timestamp.lt":  c.Query("timestamp.lt", ""),
	}

	cacheKey := fmt.Sprintf("macd:%s:ts=%s:tsps=%s:adj=%s:sw=%s:lw=%s:sig=%s:st=%s:exp=%s:ord=%s:lim=%s:gte=%s:gt=%s:lte=%s:lt=%s",
		stocksTicker,
		extra["timestamp"], extra["timespan"], extra["adjusted"],
		extra["short_window"], extra["long_window"], extra["signal_window"],
		extra["series_type"], extra["expand_underlying"], extra["order"], extra["limit"],
		extra["timestamp.gte"], extra["timestamp.gt"], extra["timestamp.lte"], extra["timestamp.lt"],
	)

	if cached, err := h.cache.Get(cacheKey); err == nil {
		return c.Type("json").SendString(cached)
	}

	data, err := h.massive.GetMACD(stocksTicker, extra)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to marshal response"})
	}
	if err := h.cache.Set(cacheKey, string(jsonData)); err != nil {
		fmt.Println("Redis set error:", err)
	}

	return c.JSON(data)
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

		"timestamp.gte": c.Query("timestamp.gte", ""),
		"timestamp.gt":  c.Query("timestamp.gt", ""),
		"timestamp.lte": c.Query("timestamp.lte", ""),
		"timestamp.lt":  c.Query("timestamp.lt", ""),
	}

	cacheKey := fmt.Sprintf("rsi:%s:ts=%s:tsps=%s:adj=%s:w=%s:st=%s:exp=%s:ord=%s:lim=%s:gte=%s:gt=%s:lte=%s:lt=%s",
		stocksTicker,
		extra["timestamp"], extra["timespan"], extra["adjusted"], extra["window"],
		extra["series_type"], extra["expand_underlying"], extra["order"], extra["limit"],
		extra["timestamp.gte"], extra["timestamp.gt"], extra["timestamp.lte"], extra["timestamp.lt"],
	)

	if cached, err := h.cache.Get(cacheKey); err == nil {
		return c.Type("json").SendString(cached)
	}

	data, err := h.massive.GetRSI(stocksTicker, extra)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to marshal response"})
	}
	if err := h.cache.Set(cacheKey, string(jsonData)); err != nil {
		fmt.Println("Redis set error:", err)
	}

	return c.JSON(data)
}

func (h *Handler) GetExchanges(c *fiber.Ctx) error {
	extra := map[string]string{
		"asset_class": c.Query("asset_class", ""),
		"locale":      c.Query("locale", ""),
	}

	cacheKey := fmt.Sprintf("exchanges:asset=%s:locale=%s", extra["asset_class"], extra["locale"])

	if cached, err := h.cache.Get(cacheKey); err == nil {
		return c.Type("json").SendString(cached)
	}

	data, err := h.massive.GetExchanges(extra)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to marshal response"})
	}
	if err := h.cache.Set(cacheKey, string(jsonData)); err != nil {
		fmt.Println("Redis set error:", err)
	}

	return c.JSON(data)
}
