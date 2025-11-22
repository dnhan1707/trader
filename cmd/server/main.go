package main

import (
	"log"

	"github.com/dnhan1707/trader/internal/api"
	"github.com/dnhan1707/trader/internal/cache"
	"github.com/dnhan1707/trader/internal/config"
	"github.com/dnhan1707/trader/internal/massive"
	"github.com/dnhan1707/trader/internal/ws"
	"github.com/gofiber/fiber/v2"
)

func main() {
	cfg := config.Load()

	cacheClient := cache.New(cfg.RedisAddr, cfg.RedisPass, cfg.RedisDB, cfg.CacheTTL)
	defer cacheClient.Close()

	massiveClient := massive.New(cfg.MassiveBase, cfg.MassiveKey)
	handler := api.New(cacheClient, massiveClient)

	// Websocket initialization
	hub := ws.NewHub()

	// current having 2 channels
	stockSubChan := make(chan string)
	indexSubChan := make(chan string)
	go hub.Run()
	go massive.ListenStocks(cfg.MassiveKey, hub, stockSubChan)
	go massive.ListenIndices(cfg.MassiveKey, hub, indexSubChan)

	app := fiber.New()

	app.Get("/health", func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	app.Get("/api/tickers/:symbol", handler.GetTickerDetails)
	// app.Get("/api/aggs/ticker/:stocksTicker/range/:multiplier/:timespan/:from/:to", handler.GetCustomBars)
	app.Get("/api/indicators/sma/:stocksTicker", handler.GetSMA)
	app.Get("/api/indicators/ema/:stocksTicker", handler.GetEMA)
	app.Get("/api/indicators/macd/:stocksTicker", handler.GetMACD)
	app.Get("/api/indicators/rsi/:stocksTicker", handler.GetRSI)
	app.Get("/api/exchanges", handler.GetExchanges)
	app.Get("/api/market/upcoming", handler.GetMarketHolidays)
	app.Get("/api/market/now", handler.GetMarketStatus)
	app.Get("/api/market/condition", handler.GetConditions)
	app.Get("/api/ipos", handler.GetIPOs)
	app.Get("/api/dividends", handler.GetDividends)
	app.Get("/api/stocks/short-interest", handler.GetShortInterest)
	app.Get("/api/stocks/short-volume", handler.GetShortVolume)
	app.Get("/api/news", handler.GetNews)
	app.Get("/api/stocks/ratios", handler.GetRatios)
	app.Get("/api/snapshot/stocks/tickers/:stocksTicker", handler.GetTickerSnapshot)
	app.Get("/api/stocks/:stocksTicker/52week", handler.Get52WeekStats)

	// WebSocket route
	app.Get("/ws", ws.NewHandler(hub, stockSubChan, indexSubChan))

	log.Fatal(app.Listen(":" + cfg.Port))
}
