package main

import (
	"log"

	"github.com/dnhan1707/trader/internal/api"
	"github.com/dnhan1707/trader/internal/cache"
	"github.com/dnhan1707/trader/internal/config"
	"github.com/dnhan1707/trader/internal/massive"
	"github.com/gofiber/fiber/v2"
)

func main() {
	cfg := config.Load()

	app := fiber.New()

	cacheClient := cache.New(cfg.RedisAddr, cfg.RedisPass, cfg.RedisDB, cfg.CacheTTL)
	defer cacheClient.Close()

	massive_client := massive.New(cfg.MassiveBase, cfg.MassiveKey)
	handler := api.New(cacheClient, massive_client)

	app.Get("/health", func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	app.Get("/api/tickers/:symbol", handler.GetTickerDetails)
	app.Get("/api/aggs/ticker/:stocksTicker/range/:multiplier/:timespan/:from/:to", handler.GetCustomBars)

	log.Fatal(app.Listen(":" + cfg.Port))
}
