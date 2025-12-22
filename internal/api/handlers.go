package api

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/dnhan1707/trader/internal/cache"
	"github.com/dnhan1707/trader/internal/massive"
	"github.com/dnhan1707/trader/internal/services"
	"github.com/gofiber/fiber/v2"
)

type Handler struct {
	cache            *cache.Cache
	massive          *massive.Client
	institutionalSvc *services.InstitutionalOwnershipService
}

func New(c *cache.Cache, m *massive.Client, inst *services.InstitutionalOwnershipService) *Handler {
	return &Handler{
		cache:            c,
		massive:          m,
		institutionalSvc: inst,
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
		// Get 52 weeks (365 days) of daily data
		// Calculate from date (52 weeks ago) to today
		fromDate := time.Now().AddDate(0, 0, -365).Format("2006-01-02")
		toDate := time.Now().Format("2006-01-02")

		// Fetch daily bars for the past year
		data, err := h.massive.GetCustomBars(stocksTicker, "1", "day", fromDate, toDate, map[string]string{
			"adjusted": "true",
			"sort":     "asc",
			"limit":    "1000", // Ensure we get all days
		})
		if err != nil {
			return nil, fmt.Errorf("failed to fetch 52-week data: %w", err)
		}

		// Parse the response
		dataMap, ok := interface{}(data).(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("unexpected response format")
		}

		results, ok := dataMap["results"].([]interface{})
		if !ok || len(results) == 0 {
			return nil, fmt.Errorf("no data available for %s", stocksTicker)
		}

		// Calculate 52-week high and low
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
				// Initialize with first bar
				weekHigh = high
				weekLow = low
				highDate = int64(timestamp)
				lowDate = int64(timestamp)
			} else {
				// Update high
				if high > weekHigh {
					weekHigh = high
					highDate = int64(timestamp)
				}
				// Update low
				if low < weekLow {
					weekLow = low
					lowDate = int64(timestamp)
				}
			}
		}

		// Get current price from most recent bar
		lastBar := results[len(results)-1].(map[string]interface{})
		currentPrice, _ := lastBar["c"].(float64)

		// Calculate percentages from 52-week range
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

func (h *Handler) GetExchanges(c *fiber.Ctx) error {
	extra := map[string]string{
		"asset_class": c.Query("asset_class", ""),
		"locale":      c.Query("locale", ""),
	}
	cacheKey := fmt.Sprintf("exchanges:asset=%s:locale=%s", extra["asset_class"], extra["locale"])
	return h.cachedJSON(c, cacheKey, func() (interface{}, error) {
		return h.massive.GetExchanges(extra)
	})
}

func (h *Handler) GetMarketHolidays(c *fiber.Ctx) error {
	return h.cachedJSON(c, "market:upcoming", func() (interface{}, error) {
		return h.massive.GetMarketHolidays()
	})
}

func (h *Handler) GetMarketStatus(c *fiber.Ctx) error {
	return h.cachedJSON(c, "market:now", func() (interface{}, error) {
		return h.massive.GetMarketStatus()
	})
}

func (h *Handler) GetConditions(c *fiber.Ctx) error {
	extra := map[string]string{
		"asset_class": c.Query("asset_class", ""),
		"data_type":   c.Query("data_type", ""),
		"id":          c.Query("id", ""),
		"sip":         c.Query("sip", ""),
		"order":       c.Query("order", ""),
		"limit":       c.Query("limit", ""),
		"sort":        c.Query("sort", ""),
	}
	cacheKey := fmt.Sprintf("conditions:asset=%s:data=%s:id=%s:sip=%s:order=%s:limit=%s:sort=%s",
		extra["asset_class"], extra["data_type"], extra["id"], extra["sip"],
		extra["order"], extra["limit"], extra["sort"],
	)
	return h.cachedJSON(c, cacheKey, func() (interface{}, error) {
		return h.massive.GetConditions(extra)
	})
}

func (h *Handler) GetIPOs(c *fiber.Ctx) error {
	extra := map[string]string{
		"ticker":           c.Query("ticker", ""),
		"us_code":          c.Query("us_code", ""),
		"isin":             c.Query("isin", ""),
		"listing_date":     c.Query("listing_date", ""),
		"ipo_status":       c.Query("ipo_status", ""),
		"listing_date.gte": c.Query("listing_date.gte", ""),
		"listing_date.gt":  c.Query("listing_date.gt", ""),
		"listing_date.lte": c.Query("listing_date.lte", ""),
		"listing_date.lt":  c.Query("listing_date.lt", ""),
		"order":            c.Query("order", ""),
		"limit":            c.Query("limit", ""),
		"sort":             c.Query("sort", ""),
	}

	cacheKey := fmt.Sprintf(
		"ipos:ticker=%s:us=%s:isin=%s:ld=%s:status=%s:gte=%s:gt=%s:lte=%s:lt=%s:order=%s:limit=%s:sort=%s",
		extra["ticker"], extra["us_code"], extra["isin"], extra["listing_date"], extra["ipo_status"],
		extra["listing_date.gte"], extra["listing_date.gt"], extra["listing_date.lte"], extra["listing_date.lt"],
		extra["order"], extra["limit"], extra["sort"],
	)

	return h.cachedJSON(c, cacheKey, func() (interface{}, error) {
		return h.massive.GetIPOs(extra)
	})
}

func (h *Handler) GetDividends(c *fiber.Ctx) error {
	extra := map[string]string{
		"ticker":               c.Query("ticker", ""),
		"ex_dividend_date":     c.Query("ex_dividend_date", ""),
		"record_date":          c.Query("record_date", ""),
		"declaration_date":     c.Query("declaration_date", ""),
		"pay_date":             c.Query("pay_date", ""),
		"frequency":            c.Query("frequency", ""),
		"cash_amount":          c.Query("cash_amount", ""),
		"dividend_type":        c.Query("dividend_type", ""),
		"ticker.gte":           c.Query("ticker.gte", ""),
		"ticker.gt":            c.Query("ticker.gt", ""),
		"ticker.lte":           c.Query("ticker.lte", ""),
		"ticker.lt":            c.Query("ticker.lt", ""),
		"ex_dividend_date.gte": c.Query("ex_dividend_date.gte", ""),
		"ex_dividend_date.gt":  c.Query("ex_dividend_date.gt", ""),
		"ex_dividend_date.lte": c.Query("ex_dividend_date.lte", ""),
		"ex_dividend_date.lt":  c.Query("ex_dividend_date.lt", ""),
		"record_date.gte":      c.Query("record_date.gte", ""),
		"record_date.gt":       c.Query("record_date.gt", ""),
		"record_date.lte":      c.Query("record_date.lte", ""),
		"record_date.lt":       c.Query("record_date.lt", ""),
		"declaration_date.gte": c.Query("declaration_date.gte", ""),
		"declaration_date.gt":  c.Query("declaration_date.gt", ""),
		"declaration_date.lte": c.Query("declaration_date.lte", ""),
		"declaration_date.lt":  c.Query("declaration_date.lt", ""),
		"pay_date.gte":         c.Query("pay_date.gte", ""),
		"pay_date.gt":          c.Query("pay_date.gt", ""),
		"pay_date.lte":         c.Query("pay_date.lte", ""),
		"pay_date.lt":          c.Query("pay_date.lt", ""),
		"cash_amount.gte":      c.Query("cash_amount.gte", ""),
		"cash_amount.gt":       c.Query("cash_amount.gt", ""),
		"cash_amount.lte":      c.Query("cash_amount.lte", ""),
		"cash_amount.lt":       c.Query("cash_amount.lt", ""),
		"order":                c.Query("order", ""),
		"limit":                c.Query("limit", ""),
		"sort":                 c.Query("sort", ""),
	}

	cacheKey := fmt.Sprintf(
		"dividends:t=%s:ex=%s:rec=%s:dec=%s:pay=%s:f=%s:amt=%s:type=%s:tgte=%s:tgt=%s:tlte=%s:tlt=%s:exgte=%s:exgt=%s:exlte=%s:exlt=%s:rgte=%s:rgt=%s:rlte=%s:rlt=%s:dgte=%s:dgt=%s:dlte=%s:dlt=%s:pgte=%s:pgt=%s:plte=%s:plt=%s:amgte=%s:amgt=%s:amlte=%s:amlt=%s:o=%s:l=%s:s=%s",
		extra["ticker"], extra["ex_dividend_date"], extra["record_date"], extra["declaration_date"], extra["pay_date"],
		extra["frequency"], extra["cash_amount"], extra["dividend_type"],
		extra["ticker.gte"], extra["ticker.gt"], extra["ticker.lte"], extra["ticker.lt"],
		extra["ex_dividend_date.gte"], extra["ex_dividend_date.gt"], extra["ex_dividend_date.lte"], extra["ex_dividend_date.lt"],
		extra["record_date.gte"], extra["record_date.gt"], extra["record_date.lte"], extra["record_date.lt"],
		extra["declaration_date.gte"], extra["declaration_date.gt"], extra["declaration_date.lte"], extra["declaration_date.lt"],
		extra["pay_date.gte"], extra["pay_date.gt"], extra["pay_date.lte"], extra["pay_date.lt"],
		extra["cash_amount.gte"], extra["cash_amount.gt"], extra["cash_amount.lte"], extra["cash_amount.lt"],
		extra["order"], extra["limit"], extra["sort"],
	)

	return h.cachedJSON(c, cacheKey, func() (interface{}, error) {
		return h.massive.GetDividends(extra)
	})
}

func (h *Handler) GetShortInterest(c *fiber.Ctx) error {
	extra := map[string]string{
		"ticker":        c.Query("ticker", ""),
		"ticker.any_of": c.Query("ticker.any_of", ""),
		"ticker.gt":     c.Query("ticker.gt", ""),
		"ticker.gte":    c.Query("ticker.gte", ""),
		"ticker.lt":     c.Query("ticker.lt", ""),
		"ticker.lte":    c.Query("ticker.lte", ""),

		"days_to_cover":        c.Query("days_to_cover", ""),
		"days_to_cover.any_of": c.Query("days_to_cover.any_of", ""),
		"days_to_cover.gt":     c.Query("days_to_cover.gt", ""),
		"days_to_cover.gte":    c.Query("days_to_cover.gte", ""),
		"days_to_cover.lt":     c.Query("days_to_cover.lt", ""),
		"days_to_cover.lte":    c.Query("days_to_cover.lte", ""),

		"settlement_date":        c.Query("settlement_date", ""),
		"settlement_date.any_of": c.Query("settlement_date.any_of", ""),
		"settlement_date.gt":     c.Query("settlement_date.gt", ""),
		"settlement_date.gte":    c.Query("settlement_date.gte", ""),
		"settlement_date.lt":     c.Query("settlement_date.lt", ""),
		"settlement_date.lte":    c.Query("settlement_date.lte", ""),

		"avg_daily_volume":        c.Query("avg_daily_volume", ""),
		"avg_daily_volume.any_of": c.Query("avg_daily_volume.any_of", ""),
		"avg_daily_volume.gt":     c.Query("avg_daily_volume.gt", ""),
		"avg_daily_volume.gte":    c.Query("avg_daily_volume.gte", ""),
		"avg_daily_volume.lt":     c.Query("avg_daily_volume.lt", ""),
		"avg_daily_volume.lte":    c.Query("avg_daily_volume.lte", ""),

		"limit": c.Query("limit", ""),
		"sort":  c.Query("sort", ""),
	}

	cacheKey := fmt.Sprintf(
		"short-interest:t=%s:tany=%s:tgt=%s:tgte=%s:tlt=%s:tlte=%s:dtc=%s:dtcany=%s:dtcgt=%s:dtcgte=%s:dtclt=%s:dtclte=%s:set=%s:setany=%s:setgt=%s:setgte=%s:setlt=%s:setlte=%s:adv=%s:advany=%s:advgt=%s:advgte=%s:advlt=%s:advlte=%s:limit=%s:sort=%s",
		extra["ticker"], extra["ticker.any_of"], extra["ticker.gt"], extra["ticker.gte"], extra["ticker.lt"], extra["ticker.lte"],
		extra["days_to_cover"], extra["days_to_cover.any_of"], extra["days_to_cover.gt"], extra["days_to_cover.gte"], extra["days_to_cover.lt"], extra["days_to_cover.lte"],
		extra["settlement_date"], extra["settlement_date.any_of"], extra["settlement_date.gt"], extra["settlement_date.gte"], extra["settlement_date.lt"], extra["settlement_date.lte"],
		extra["avg_daily_volume"], extra["avg_daily_volume.any_of"], extra["avg_daily_volume.gt"], extra["avg_daily_volume.gte"], extra["avg_daily_volume.lt"], extra["avg_daily_volume.lte"],
		extra["limit"], extra["sort"],
	)

	return h.cachedJSON(c, cacheKey, func() (interface{}, error) {
		return h.massive.GetShortInterest(extra)
	})
}

func (h *Handler) GetShortVolume(c *fiber.Ctx) error {
	extra := map[string]string{
		"ticker":        c.Query("ticker", ""),
		"ticker.any_of": c.Query("ticker.any_of", ""),
		"ticker.gt":     c.Query("ticker.gt", ""),
		"ticker.gte":    c.Query("ticker.gte", ""),
		"ticker.lt":     c.Query("ticker.lt", ""),
		"ticker.lte":    c.Query("ticker.lte", ""),

		"date":        c.Query("date", ""),
		"date.any_of": c.Query("date.any_of", ""),
		"date.gt":     c.Query("date.gt", ""),
		"date.gte":    c.Query("date.gte", ""),
		"date.lt":     c.Query("date.lt", ""),
		"date.lte":    c.Query("date.lte", ""),

		"short_volume_ratio":        c.Query("short_volume_ratio", ""),
		"short_volume_ratio.any_of": c.Query("short_volume_ratio.any_of", ""),
		"short_volume_ratio.gt":     c.Query("short_volume_ratio.gt", ""),
		"short_volume_ratio.gte":    c.Query("short_volume_ratio.gte", ""),
		"short_volume_ratio.lt":     c.Query("short_volume_ratio.lt", ""),
		"short_volume_ratio.lte":    c.Query("short_volume_ratio.lte", ""),

		"total_volume":        c.Query("total_volume", ""),
		"total_volume.any_of": c.Query("total_volume.any_of", ""),
		"total_volume.gt":     c.Query("total_volume.gt", ""),
		"total_volume.gte":    c.Query("total_volume.gte", ""),
		"total_volume.lt":     c.Query("total_volume.lt", ""),
		"total_volume.lte":    c.Query("total_volume.lte", ""),

		"limit": c.Query("limit", ""),
		"sort":  c.Query("sort", ""),
	}

	cacheKey := fmt.Sprintf(
		"short-volume:t=%s:tany=%s:tgt=%s:tgte=%s:tlt=%s:tlte=%s:d=%s:dany=%s:dgt=%s:dgte=%s:dlt=%s:dlte=%s:svr=%s:svrany=%s:svrgt=%s:svrgte=%s:svrlt=%s:svrlte=%s:tot=%s:totany=%s:totgt=%s:totgte=%s:totlt=%s:totlte=%s:limit=%s:sort=%s",
		extra["ticker"], extra["ticker.any_of"], extra["ticker.gt"], extra["ticker.gte"], extra["ticker.lt"], extra["ticker.lte"],
		extra["date"], extra["date.any_of"], extra["date.gt"], extra["date.gte"], extra["date.lt"], extra["date.lte"],
		extra["short_volume_ratio"], extra["short_volume_ratio.any_of"], extra["short_volume_ratio.gt"], extra["short_volume_ratio.gte"], extra["short_volume_ratio.lt"], extra["short_volume_ratio.lte"],
		extra["total_volume"], extra["total_volume.any_of"], extra["total_volume.gt"], extra["total_volume.gte"], extra["total_volume.lt"], extra["total_volume.lte"],
		extra["limit"], extra["sort"],
	)

	return h.cachedJSON(c, cacheKey, func() (interface{}, error) {
		return h.massive.GetShortVolume(extra)
	})
}

func (h *Handler) GetNews(c *fiber.Ctx) error {
	// Accept either:
	//   ?sort=published_utc&order=descending
	// or combined:
	//   ?sort=published_utc.desc (order param optional)
	//   ?sort=ticker.asc
	rawSort := c.Query("sort", "published_utc")
	rawOrder := c.Query("order", "")

	var sortField string
	var order string

	parts := strings.Split(rawSort, ".")
	sortField = parts[0]
	if len(parts) == 2 {
		rawOrder = parts[1] // override separate order if combined form used
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

func (h *Handler) GetRatios(c *fiber.Ctx) error {
	extra := map[string]string{
		"ticker":        c.Query("ticker", ""),
		"ticker.any_of": c.Query("ticker.any_of", ""),
		"ticker.gt":     c.Query("ticker.gt", ""),
		"ticker.gte":    c.Query("ticker.gte", ""),
		"ticker.lt":     c.Query("ticker.lt", ""),
		"ticker.lte":    c.Query("ticker.lte", ""),

		"cik":        c.Query("cik", ""),
		"cik.any_of": c.Query("cik.any_of", ""),
		"cik.gt":     c.Query("cik.gt", ""),
		"cik.gte":    c.Query("cik.gte", ""),
		"cik.lt":     c.Query("cik.lt", ""),
		"cik.lte":    c.Query("cik.lte", ""),

		"price":     c.Query("price", ""),
		"price.gt":  c.Query("price.gt", ""),
		"price.gte": c.Query("price.gte", ""),
		"price.lt":  c.Query("price.lt", ""),
		"price.lte": c.Query("price.lte", ""),

		"average_volume":     c.Query("average_volume", ""),
		"average_volume.gt":  c.Query("average_volume.gt", ""),
		"average_volume.gte": c.Query("average_volume.gte", ""),
		"average_volume.lt":  c.Query("average_volume.lt", ""),
		"average_volume.lte": c.Query("average_volume.lte", ""),

		"market_cap":     c.Query("market_cap", ""),
		"market_cap.gt":  c.Query("market_cap.gt", ""),
		"market_cap.gte": c.Query("market_cap.gte", ""),
		"market_cap.lt":  c.Query("market_cap.lt", ""),
		"market_cap.lte": c.Query("market_cap.lte", ""),

		"earnings_per_share":     c.Query("earnings_per_share", ""),
		"earnings_per_share.gt":  c.Query("earnings_per_share.gt", ""),
		"earnings_per_share.gte": c.Query("earnings_per_share.gte", ""),
		"earnings_per_share.lt":  c.Query("earnings_per_share.lt", ""),
		"earnings_per_share.lte": c.Query("earnings_per_share.lte", ""),

		"price_to_earnings":     c.Query("price_to_earnings", ""),
		"price_to_earnings.gt":  c.Query("price_to_earnings.gt", ""),
		"price_to_earnings.gte": c.Query("price_to_earnings.gte", ""),
		"price_to_earnings.lt":  c.Query("price_to_earnings.lt", ""),
		"price_to_earnings.lte": c.Query("price_to_earnings.lte", ""),

		"price_to_book":     c.Query("price_to_book", ""),
		"price_to_book.gt":  c.Query("price_to_book.gt", ""),
		"price_to_book.gte": c.Query("price_to_book.gte", ""),
		"price_to_book.lt":  c.Query("price_to_book.lt", ""),
		"price_to_book.lte": c.Query("price_to_book.lte", ""),

		"price_to_sales":     c.Query("price_to_sales", ""),
		"price_to_sales.gt":  c.Query("price_to_sales.gt", ""),
		"price_to_sales.gte": c.Query("price_to_sales.gte", ""),
		"price_to_sales.lt":  c.Query("price_to_sales.lt", ""),
		"price_to_sales.lte": c.Query("price_to_sales.lte", ""),

		"price_to_cash_flow":     c.Query("price_to_cash_flow", ""),
		"price_to_cash_flow.gt":  c.Query("price_to_cash_flow.gt", ""),
		"price_to_cash_flow.gte": c.Query("price_to_cash_flow.gte", ""),
		"price_to_cash_flow.lt":  c.Query("price_to_cash_flow.lt", ""),
		"price_to_cash_flow.lte": c.Query("price_to_cash_flow.lte", ""),

		"price_to_free_cash_flow":     c.Query("price_to_free_cash_flow", ""),
		"price_to_free_cash_flow.gt":  c.Query("price_to_free_cash_flow.gt", ""),
		"price_to_free_cash_flow.gte": c.Query("price_to_free_cash_flow.gte", ""),
		"price_to_free_cash_flow.lt":  c.Query("price_to_free_cash_flow.lt", ""),
		"price_to_free_cash_flow.lte": c.Query("price_to_free_cash_flow.lte", ""),

		"dividend_yield":     c.Query("dividend_yield", ""),
		"dividend_yield.gt":  c.Query("dividend_yield.gt", ""),
		"dividend_yield.gte": c.Query("dividend_yield.gte", ""),
		"dividend_yield.lt":  c.Query("dividend_yield.lt", ""),
		"dividend_yield.lte": c.Query("dividend_yield.lte", ""),

		"return_on_assets":     c.Query("return_on_assets", ""),
		"return_on_assets.gt":  c.Query("return_on_assets.gt", ""),
		"return_on_assets.gte": c.Query("return_on_assets.gte", ""),
		"return_on_assets.lt":  c.Query("return_on_assets.lt", ""),
		"return_on_assets.lte": c.Query("return_on_assets.lte", ""),

		"return_on_equity":     c.Query("return_on_equity", ""),
		"return_on_equity.gt":  c.Query("return_on_equity.gt", ""),
		"return_on_equity.gte": c.Query("return_on_equity.gte", ""),
		"return_on_equity.lt":  c.Query("return_on_equity.lt", ""),
		"return_on_equity.lte": c.Query("return_on_equity.lte", ""),

		"debt_to_equity":     c.Query("debt_to_equity", ""),
		"debt_to_equity.gt":  c.Query("debt_to_equity.gt", ""),
		"debt_to_equity.gte": c.Query("debt_to_equity.gte", ""),
		"debt_to_equity.lt":  c.Query("debt_to_equity.lt", ""),
		"debt_to_equity.lte": c.Query("debt_to_equity.lte", ""),

		"current":     c.Query("current", ""),
		"current.gt":  c.Query("current.gt", ""),
		"current.gte": c.Query("current.gte", ""),
		"current.lt":  c.Query("current.lt", ""),
		"current.lte": c.Query("current.lte", ""),

		"quick":     c.Query("quick", ""),
		"quick.gt":  c.Query("quick.gt", ""),
		"quick.gte": c.Query("quick.gte", ""),
		"quick.lt":  c.Query("quick.lt", ""),
		"quick.lte": c.Query("quick.lte", ""),

		"cash":     c.Query("cash", ""),
		"cash.gt":  c.Query("cash.gt", ""),
		"cash.gte": c.Query("cash.gte", ""),
		"cash.lt":  c.Query("cash.lt", ""),
		"cash.lte": c.Query("cash.lte", ""),

		"ev_to_sales":     c.Query("ev_to_sales", ""),
		"ev_to_sales.gt":  c.Query("ev_to_sales.gt", ""),
		"ev_to_sales.gte": c.Query("ev_to_sales.gte", ""),
		"ev_to_sales.lt":  c.Query("ev_to_sales.lt", ""),
		"ev_to_sales.lte": c.Query("ev_to_sales.lte", ""),

		"ev_to_ebitda":     c.Query("ev_to_ebitda", ""),
		"ev_to_ebitda.gt":  c.Query("ev_to_ebitda.gt", ""),
		"ev_to_ebitda.gte": c.Query("ev_to_ebitda.gte", ""),
		"ev_to_ebitda.lt":  c.Query("ev_to_ebitda.lt", ""),
		"ev_to_ebitda.lte": c.Query("ev_to_ebitda.lte", ""),

		"enterprise_value":     c.Query("enterprise_value", ""),
		"enterprise_value.gt":  c.Query("enterprise_value.gt", ""),
		"enterprise_value.gte": c.Query("enterprise_value.gte", ""),
		"enterprise_value.lt":  c.Query("enterprise_value.lt", ""),
		"enterprise_value.lte": c.Query("enterprise_value.lte", ""),

		"free_cash_flow":     c.Query("free_cash_flow", ""),
		"free_cash_flow.gt":  c.Query("free_cash_flow.gt", ""),
		"free_cash_flow.gte": c.Query("free_cash_flow.gte", ""),
		"free_cash_flow.lt":  c.Query("free_cash_flow.lt", ""),
		"free_cash_flow.lte": c.Query("free_cash_flow.lte", ""),

		"limit": c.Query("limit", ""),
		"sort":  c.Query("sort", ""),
	}

	cacheKey := fmt.Sprintf(
		"ratios:t=%s:tany=%s:tgt=%s:tgte=%s:tlt=%s:tlte=%s:cik=%s:cikany=%s:cikgt=%s:cikgte=%s:ciklt=%s:ciklte=%s:p=%s:pgt=%s:pgte=%s:plt=%s:plte=%s:av=%s:avgt=%s:avgte=%s:avlt=%s:avlte=%s:mc=%s:mcgt=%s:mcgte=%s:mclt=%s:mclte=%s:eps=%s:epsgt=%s:epsgte=%s:epslt=%s:epslte=%s:pe=%s:pegt=%s:pegte=%s:pelt=%s:pelte=%s:pb=%s:pbgt=%s:pbgte=%s:pblt=%s:pblte=%s:ps=%s:psgt=%s:psgte=%s:pslt=%s:pslte=%s:pcf=%s:pcfgt=%s:pcfgte=%s:pcflt=%s:pcflte=%s:pfcf=%s:pfcfgt=%s:pfcfgte=%s:pfcflt=%s:pfcflte=%s:dy=%s:dygt=%s:dygte=%s:dylt=%s:dylte=%s:roa=%s:roagt=%s:roagte=%s:roalt=%s:roalte=%s:roe=%s:roegt=%s:roegte=%s:roelt=%s:roelte=%s:de=%s:degt=%s:degte=%s:delt=%s:delte=%s:cur=%s:curgt=%s:curgte=%s:curlt=%s:curlte=%s:q=%s:qgt=%s:qgte=%s:qlt=%s:qlte=%s:c=%s:cgt=%s:cgte=%s:clt=%s:clte=%s:evs=%s:evsgt=%s:evsgte=%s:evslt=%s:evslte=%s:eve=%s:evegt=%s:evegte=%s:evelt=%s:evelte=%s:ev=%s:evgt=%s:evgte=%s:evlt=%s:evlte=%s:fcf=%s:fcfgt=%s:fcfgte=%s:fcflt=%s:fcflte=%s:lim=%s:sort=%s",
		extra["ticker"], extra["ticker.any_of"], extra["ticker.gt"], extra["ticker.gte"], extra["ticker.lt"], extra["ticker.lte"],
		extra["cik"], extra["cik.any_of"], extra["cik.gt"], extra["cik.gte"], extra["cik.lt"], extra["cik.lte"],
		extra["price"], extra["price.gt"], extra["price.gte"], extra["price.lt"], extra["price.lte"],
		extra["average_volume"], extra["average_volume.gt"], extra["average_volume.gte"], extra["average_volume.lt"], extra["average_volume.lte"],
		extra["market_cap"], extra["market_cap.gt"], extra["market_cap.gte"], extra["market_cap.lt"], extra["market_cap.lte"],
		extra["earnings_per_share"], extra["earnings_per_share.gt"], extra["earnings_per_share.gte"], extra["earnings_per_share.lt"], extra["earnings_per_share.lte"],
		extra["price_to_earnings"], extra["price_to_earnings.gt"], extra["price_to_earnings.gte"], extra["price_to_earnings.lt"], extra["price_to_earnings.lte"],
		extra["price_to_book"], extra["price_to_book.gt"], extra["price_to_book.gte"], extra["price_to_book.lt"], extra["price_to_book.lte"],
		extra["price_to_sales"], extra["price_to_sales.gt"], extra["price_to_sales.gte"], extra["price_to_sales.lt"], extra["price_to_sales.lte"],
		extra["price_to_cash_flow"], extra["price_to_cash_flow.gt"], extra["price_to_cash_flow.gte"], extra["price_to_cash_flow.lt"], extra["price_to_cash_flow.lte"],
		extra["price_to_free_cash_flow"], extra["price_to_free_cash_flow.gt"], extra["price_to_free_cash_flow.gte"], extra["price_to_free_cash_flow.lt"], extra["price_to_free_cash_flow.lte"],
		extra["dividend_yield"], extra["dividend_yield.gt"], extra["dividend_yield.gte"], extra["dividend_yield.lt"], extra["dividend_yield.lte"],
		extra["return_on_assets"], extra["return_on_assets.gt"], extra["return_on_assets.gte"], extra["return_on_assets.lt"], extra["return_on_assets.lte"],
		extra["return_on_equity"], extra["return_on_equity.gt"], extra["return_on_equity.gte"], extra["return_on_equity.lt"], extra["return_on_equity.lte"],
		extra["debt_to_equity"], extra["debt_to_equity.gt"], extra["debt_to_equity.gte"], extra["debt_to_equity.lt"], extra["debt_to_equity.lte"],
		extra["current"], extra["current.gt"], extra["current.gte"], extra["current.lt"], extra["current.lte"],
		extra["quick"], extra["quick.gt"], extra["quick.gte"], extra["quick.lt"], extra["quick.lte"],
		extra["cash"], extra["cash.gt"], extra["cash.gte"], extra["cash.lt"], extra["cash.lte"],
		extra["ev_to_sales"], extra["ev_to_sales.gt"], extra["ev_to_sales.gte"], extra["ev_to_sales.lt"], extra["ev_to_sales.lte"],
		extra["ev_to_ebitda"], extra["ev_to_ebitda.gt"], extra["ev_to_ebitda.gte"], extra["ev_to_ebitda.lt"], extra["ev_to_ebitda.lte"],
		extra["enterprise_value"], extra["enterprise_value.gt"], extra["enterprise_value.gte"], extra["enterprise_value.lt"], extra["enterprise_value.lte"],
		extra["free_cash_flow"], extra["free_cash_flow.gt"], extra["free_cash_flow.gte"], extra["free_cash_flow.lt"], extra["free_cash_flow.lte"],
		extra["limit"], extra["sort"],
	)

	return h.cachedJSON(c, cacheKey, func() (interface{}, error) {
		return h.massive.GetRatios(extra)
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

func (h *Handler) GetTopOwners(c *fiber.Ctx) error {
	ticker := c.Query("ticker")
	year := c.Query("year")
	quarter := c.Query("quarter")
	nStr := c.Query("n", "10")

	if ticker == "" {
		return c.Status(400).JSON(fiber.Map{"error": "ticker query parameter is required"})
	}
	if year == "" {
		return c.Status(400).JSON(fiber.Map{"error": "year query parameter is required"})
	}
	if quarter == "" {
		return c.Status(400).JSON(fiber.Map{"error": "quarter query parameter is required"})
	}

	n := 10
	if _, err := fmt.Sscanf(nStr, "%d", &n); err != nil || n <= 0 {
		return c.Status(400).JSON(fiber.Map{"error": "n must be a positive integer"})
	}

	cacheKey := fmt.Sprintf("top-owners:%s:%s:%s:%d", ticker, year, quarter, n)

	return h.cachedJSON(c, cacheKey, func() (interface{}, error) {
		return h.institutionalSvc.TopNSharesOwner(c.Context(), ticker, year, quarter, n)
	})
}

func (h *Handler) GetIncomeStatements(c *fiber.Ctx) error {
	extra := map[string]string{
		"cik":        c.Query("cik", ""),
		"cik.any_of": c.Query("cik.any_of", ""),
		"cik.gt":     c.Query("cik.gt", ""),
		"cik.gte":    c.Query("cik.gte", ""),
		"cik.lt":     c.Query("cik.lt", ""),
		"cik.lte":    c.Query("cik.lte", ""),

		"tickers":        c.Query("tickers", ""),
		"tickers.all_of": c.Query("tickers.all_of", ""),
		"tickers.any_of": c.Query("tickers.any_of", ""),

		"period_end":     c.Query("period_end", ""),
		"period_end.gt":  c.Query("period_end.gt", ""),
		"period_end.gte": c.Query("period_end.gte", ""),
		"period_end.lt":  c.Query("period_end.lt", ""),
		"period_end.lte": c.Query("period_end.lte", ""),

		"filing_date":     c.Query("filing_date", ""),
		"filing_date.gt":  c.Query("filing_date.gt", ""),
		"filing_date.gte": c.Query("filing_date.gte", ""),
		"filing_date.lt":  c.Query("filing_date.lt", ""),
		"filing_date.lte": c.Query("filing_date.lte", ""),

		"fiscal_year":     c.Query("fiscal_year", ""),
		"fiscal_year.gt":  c.Query("fiscal_year.gt", ""),
		"fiscal_year.gte": c.Query("fiscal_year.gte", ""),
		"fiscal_year.lt":  c.Query("fiscal_year.lt", ""),
		"fiscal_year.lte": c.Query("fiscal_year.lte", ""),

		"fiscal_quarter":     c.Query("fiscal_quarter", ""),
		"fiscal_quarter.gt":  c.Query("fiscal_quarter.gt", ""),
		"fiscal_quarter.gte": c.Query("fiscal_quarter.gte", ""),
		"fiscal_quarter.lt":  c.Query("fiscal_quarter.lt", ""),
		"fiscal_quarter.lte": c.Query("fiscal_quarter.lte", ""),

		"timeframe":        c.Query("timeframe", ""),
		"timeframe.any_of": c.Query("timeframe.any_of", ""),
		"timeframe.gt":     c.Query("timeframe.gt", ""),
		"timeframe.gte":    c.Query("timeframe.gte", ""),
		"timeframe.lt":     c.Query("timeframe.lt", ""),
		"timeframe.lte":    c.Query("timeframe.lte", ""),

		"limit": c.Query("limit", ""),
		"sort":  c.Query("sort", ""),
	}

	cacheKey := fmt.Sprintf(
		"income-statements:cik=%s:cikany=%s:cikgt=%s:cikgte=%s:ciklt=%s:ciklte=%s:t=%s:tall=%s:tany=%s:pe=%s:pegt=%s:pegte=%s:pelt=%s:pelte=%s:fd=%s:fdgt=%s:fdgte=%s:fdlt=%s:fdlte=%s:fy=%s:fygt=%s:fygte=%s:fylt=%s:fylte=%s:fq=%s:fqgt=%s:fqgte=%s:fqlt=%s:fqlte=%s:tf=%s:tfany=%s:tfgt=%s:tfgte=%s:tflt=%s:tflte=%s:limit=%s:sort=%s",
		extra["cik"], extra["cik.any_of"], extra["cik.gt"], extra["cik.gte"], extra["cik.lt"], extra["cik.lte"],
		extra["tickers"], extra["tickers.all_of"], extra["tickers.any_of"],
		extra["period_end"], extra["period_end.gt"], extra["period_end.gte"], extra["period_end.lt"], extra["period_end.lte"],
		extra["filing_date"], extra["filing_date.gt"], extra["filing_date.gte"], extra["filing_date.lt"], extra["filing_date.lte"],
		extra["fiscal_year"], extra["fiscal_year.gt"], extra["fiscal_year.gte"], extra["fiscal_year.lt"], extra["fiscal_year.lte"],
		extra["fiscal_quarter"], extra["fiscal_quarter.gt"], extra["fiscal_quarter.gte"], extra["fiscal_quarter.lt"], extra["fiscal_quarter.lte"],
		extra["timeframe"], extra["timeframe.any_of"], extra["timeframe.gt"], extra["timeframe.gte"], extra["timeframe.lt"], extra["timeframe.lte"],
		extra["limit"], extra["sort"],
	)

	return h.cachedJSON(c, cacheKey, func() (interface{}, error) {
		return h.massive.GetIncomeStatements(extra)
	})
}
