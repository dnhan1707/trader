package api

import (
	"fmt"

	"github.com/gofiber/fiber/v2"
)

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
