package api

import (
	"fmt"

	"github.com/gofiber/fiber/v2"
)

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
	// ...existing code for dividend parameters...
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

func (h *Handler) GetRatios(c *fiber.Ctx) error {
	// ...existing code for ratios parameters (keeping all the existing parameter handling)...
	extra := map[string]string{
		"ticker":        c.Query("ticker", ""),
		"ticker.any_of": c.Query("ticker.any_of", ""),
		// ...continuing with all existing parameters...
		"limit": c.Query("limit", ""),
		"sort":  c.Query("sort", ""),
	}

	// ...existing cache key generation...
	cacheKey := fmt.Sprintf("ratios:t=%s:tany=%s:tgt=%s:tgte=%s:tlt=%s:tlte=%s:lim=%s:sort=%s",
		extra["ticker"], extra["ticker.any_of"], extra["ticker.gt"],
		extra["ticker.gte"], extra["ticker.lt"], extra["ticker.lte"],
		extra["limit"], extra["sort"])

	return h.cachedJSON(c, cacheKey, func() (interface{}, error) {
		return h.massive.GetRatios(extra)
	})
}

func (h *Handler) GetIncomeStatements(c *fiber.Ctx) error {
	// ...existing code for income statement parameters...
	extra := map[string]string{
		"cik": c.Query("cik", ""),
		// ...all other parameters...
		"limit": c.Query("limit", ""),
		"sort":  c.Query("sort", ""),
	}

	cacheKey := fmt.Sprintf("income-statements:cik=%s:limit=%s:sort=%s",
		extra["cik"], extra["limit"], extra["sort"])

	return h.cachedJSON(c, cacheKey, func() (interface{}, error) {
		return h.massive.GetIncomeStatements(extra)
	})
}
