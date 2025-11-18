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
		"order":             c.Query("order", ""),
		"limit":             c.Query("limit", ""),
		"sort":              c.Query("sort", ""),
	}

	cacheKey := fmt.Sprintf(
		"news:t=%s:pub=%s:tgte=%s:tgt=%s:tlte=%s:tlt=%s:pgte=%s:pgt=%s:plte=%s:plt=%s:ord=%s:lim=%s:sort=%s",
		extra["ticker"], extra["published_utc"],
		extra["ticker.gte"], extra["ticker.gt"], extra["ticker.lte"], extra["ticker.lt"],
		extra["published_utc.gte"], extra["published_utc.gt"], extra["published_utc.lte"], extra["published_utc.lt"],
		extra["order"], extra["limit"], extra["sort"],
	)

	return h.cachedJSON(c, cacheKey, func() (interface{}, error) {
		return h.massive.GetNews(extra)
	})
}
