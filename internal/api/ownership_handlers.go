package api

import (
	"fmt"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/log"
)

func (h *Handler) GetTopOwners(c *fiber.Ctx) error {
	ticker := c.Query("ticker")
	companyName := c.Query("companyName")
	limit := c.QueryInt("limit")

	if ticker == "" {
		return c.Status(400).JSON(fiber.Map{"error": "ticker query parameter is required"})
	}
	if companyName == "" {
		return c.Status(400).JSON(fiber.Map{"error": "companyName query parameter is required"})
	}
	if limit <= 0 {
		return c.Status(400).JSON(fiber.Map{"error": "limit query parameter cannot be 0 or less"})
	}

	companyName = strings.ReplaceAll(companyName, "+", " ")

	log.Debug(fmt.Sprintf("TopOwner - Company name query = %s", companyName))
	cacheKey := fmt.Sprintf("top-owners:%s:%s", ticker, companyName)

	return h.cachedJSON(c, cacheKey, func() (interface{}, error) {
		return h.institutionalSvc.GetTopOwnersByNameWithTicker(companyName, ticker, limit)
	})
}

func (h *Handler) GetTopOwnersByCusip(c *fiber.Ctx) error {
	ticker := c.Query("ticker")
	limit := c.QueryInt("limit")

	if ticker == "" {
		return c.Status(400).JSON(fiber.Map{"error": "ticker query parameter is required"})
	}
	if limit <= 0 {
		return c.Status(400).JSON(fiber.Map{"error": "limit query parameter cannot be 0 or less"})
	}

	log.Debug(fmt.Sprintf("TopOwnersByCusip - Ticker query = %s", ticker))
	cacheKey := fmt.Sprintf("top-owners-cusip:%s:%d", ticker, limit)

	return h.cachedJSON(c, cacheKey, func() (interface{}, error) {
		return h.institutionalSvc.GetTopOwnersByCusip(ticker, limit)
	})
}

func (h *Handler) GetTopInsiders(c *fiber.Ctx) error {
	ticker := c.Query("ticker")
	startYear := c.QueryInt("startYear")
	limit := c.QueryInt("limit")

	if ticker == "" {
		return c.Status(400).JSON(fiber.Map{"error": "ticker query parameter is required"})
	}
	if startYear <= 0 {
		startYear = 2020
	}
	if limit <= 0 {
		limit = 10
	}

	log.Debug(fmt.Sprintf("TopInsiders - Ticker = %s, StartYear = %d", ticker, startYear))
	cacheKey := fmt.Sprintf("top-insiders:%s:%d:%d", ticker, startYear, limit)

	return h.cachedJSON(c, cacheKey, func() (interface{}, error) {
		return h.insiderSvc.GetTopInsidersFiltered(ticker, startYear, limit)
	})
}
