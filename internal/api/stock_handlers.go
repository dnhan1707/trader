package api

import (
	"fmt"
	"time"

	"github.com/gofiber/fiber/v2"
)

func (h *Handler) GetTickerDetails(c *fiber.Ctx) error {
	symbol := c.Params("symbol")
	if symbol == "" {
		return c.Status(400).JSON(fiber.Map{"error": "symbol is required"})
	}
	cacheKey := "ticker:" + symbol
	return h.cachedJSON(c, cacheKey, func() (interface{}, error) {
		return h.massive.GetTickerDetails(symbol)
	})
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
	return h.cachedJSON(c, cacheKey, func() (interface{}, error) {
		return h.massive.GetCustomBars(stocksTicker, multiplier, timespan, from, to, extra)
	})
}

func (h *Handler) Get52WeekStats(c *fiber.Ctx) error {
	stocksTicker := c.Params("stocksTicker")
	if stocksTicker == "" {
		return c.Status(400).JSON(fiber.Map{"error": "stocksTicker is required"})
	}

	cacheKey := fmt.Sprintf("52week:%s", stocksTicker)

	return h.cachedJSON(c, cacheKey, func() (interface{}, error) {
		// ...existing code for 52-week stats calculation...
		fromDate := time.Now().AddDate(0, 0, -365).Format("2006-01-02")
		toDate := time.Now().Format("2006-01-02")

		data, err := h.massive.GetCustomBars(stocksTicker, "1", "day", fromDate, toDate, map[string]string{
			"adjusted": "true",
			"sort":     "asc",
			"limit":    "1000",
		})
		if err != nil {
			return nil, fmt.Errorf("failed to fetch 52-week data: %w", err)
		}

		dataMap, ok := interface{}(data).(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("unexpected response format")
		}

		results, ok := dataMap["results"].([]interface{})
		if !ok || len(results) == 0 {
			return nil, fmt.Errorf("no data available for %s", stocksTicker)
		}

		var weekHigh, weekLow float64
		var highDate, lowDate int64

		for i, result := range results {
			bar, ok := result.(map[string]interface{})
			if !ok {
				continue
			}

			high, _ := bar["h"].(float64)
			low, _ := bar["l"].(float64)
			timestamp, _ := bar["t"].(float64)

			if i == 0 {
				weekHigh = high
				weekLow = low
				highDate = int64(timestamp)
				lowDate = int64(timestamp)
			} else {
				if high > weekHigh {
					weekHigh = high
					highDate = int64(timestamp)
				}
				if low < weekLow {
					weekLow = low
					lowDate = int64(timestamp)
				}
			}
		}

		lastBar := results[len(results)-1].(map[string]interface{})
		currentPrice, _ := lastBar["c"].(float64)

		var highPercent, lowPercent float64
		if weekHigh > 0 {
			highPercent = ((currentPrice - weekHigh) / weekHigh) * 100
		}
		if weekLow > 0 {
			lowPercent = ((currentPrice - weekLow) / weekLow) * 100
		}

		return map[string]interface{}{
			"ticker":             stocksTicker,
			"current_price":      currentPrice,
			"week_52_high":       weekHigh,
			"week_52_low":        weekLow,
			"week_52_high_date":  time.Unix(highDate/1000, 0).Format("2006-01-02"),
			"week_52_low_date":   time.Unix(lowDate/1000, 0).Format("2006-01-02"),
			"percent_from_high":  highPercent,
			"percent_from_low":   lowPercent,
			"range_position":     ((currentPrice - weekLow) / (weekHigh - weekLow)) * 100,
			"total_trading_days": len(results),
			"status":             "OK",
		}, nil
	})
}

func (h *Handler) GetTickerSnapshot(c *fiber.Ctx) error {
	stocksTicker := c.Params("stocksTicker")
	if stocksTicker == "" {
		return c.Status(400).JSON(fiber.Map{"error": "stocksTicker is required"})
	}

	cacheKey := fmt.Sprintf("snapshot:ticker:%s", stocksTicker)

	return h.cachedJSON(c, cacheKey, func() (interface{}, error) {
		return h.massive.GetTickerSnapshot(stocksTicker)
	})
}
