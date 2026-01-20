package main

import (
	"database/sql"
	"log"

	"github.com/dnhan1707/trader/internal/api"
	"github.com/dnhan1707/trader/internal/auth"
	"github.com/dnhan1707/trader/internal/cache"
	"github.com/dnhan1707/trader/internal/config"
	"github.com/dnhan1707/trader/internal/eodhd"
	"github.com/dnhan1707/trader/internal/massive"
	"github.com/dnhan1707/trader/internal/services"
	"github.com/dnhan1707/trader/internal/ws"
	"github.com/gofiber/fiber/v2"
	_ "github.com/lib/pq"
)

func main() {
	cfg := config.Load()

	// Redis Cache
	cacheClient := cache.New(cfg.RedisAddr, cfg.RedisPass, cfg.RedisDB, cfg.CacheTTL)
	defer cacheClient.Close()

	// Postgres Connection
	dsn := "postgres://trader_app:trader_app_123@localhost:5434/13f_filings?sslmode=disable"
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatal("open db:", err)
	}
	defer db.Close()

	// Handlers Setup
	massiveClient := massive.New(cfg.MassiveBase, cfg.MassiveKey)
	eodhClient := eodhd.New(cfg.EODHD_BASE, cfg.EODHD_API_KEY)
	instSvc := services.NewInstitutionalOwnershipService(db, massiveClient, eodhClient)
	insiderSvc := services.NewInsiderOwnershipService(db, massiveClient)
	authService := services.NewAuthService(db)
	authHandler := api.NewAuthHandler(authService, cfg.JwtSecret, cfg.JwtExpiresIn)

	handler := api.New(cacheClient, massiveClient, instSvc, insiderSvc)

	// Websocket initialization
	hub := ws.NewHub()

	// current having 2 channels
	stockSubChan := make(chan string)
	indexSubChan := make(chan string)
	go hub.Run()
	go massive.ListenStocks(cfg.MassiveKey, hub, stockSubChan)
	go massive.ListenIndices(cfg.MassiveKey, hub, indexSubChan)

	// Fiber App
	app := fiber.New()

	app.Get("/health", func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	// Public auth routes
	app.Post("/api/auth/signup", authHandler.SignUp)
	app.Post("/api/auth/login", authHandler.Login)
	app.Post("/api/auth/logout", authHandler.Logout)

	// Protect all other /api routes
	apiGroup := app.Group("/api", auth.Middleware(cfg.JwtSecret))

	apiGroup.Get("/tickers/:symbol", handler.GetTickerDetails)
	// app.Get("/api/aggs/ticker/:stocksTicker/range/:multiplier/:timespan/:from/:to", handler.GetCustomBars)
	apiGroup.Get("/indicators/sma/:stocksTicker", handler.GetSMA)
	apiGroup.Get("/indicators/ema/:stocksTicker", handler.GetEMA)
	apiGroup.Get("/indicators/macd/:stocksTicker", handler.GetMACD)
	apiGroup.Get("/indicators/rsi/:stocksTicker", handler.GetRSI)
	apiGroup.Get("/exchanges", handler.GetExchanges)
	apiGroup.Get("/market/upcoming", handler.GetMarketHolidays)
	apiGroup.Get("/market/now", handler.GetMarketStatus)
	apiGroup.Get("/market/condition", handler.GetConditions)
	apiGroup.Get("/ipos", handler.GetIPOs)
	apiGroup.Get("/dividends", handler.GetDividends)
	apiGroup.Get("/stocks/short-interest", handler.GetShortInterest)
	apiGroup.Get("/stocks/short-volume", handler.GetShortVolume)
	apiGroup.Get("/news", handler.GetNews)
	apiGroup.Get("/stocks/ratios", handler.GetRatios)
	apiGroup.Get("/snapshot/stocks/tickers/:stocksTicker", handler.GetTickerSnapshot)
	apiGroup.Get("/stocks/:stocksTicker/52week", handler.Get52WeekStats)
	apiGroup.Get("/stocks/financials/income-statements", handler.GetIncomeStatements)
	apiGroup.Get("/stocks/ownership", handler.GetTopOwners)
	apiGroup.Get("/stocks/ownership/cusip", handler.GetTopOwnersByCusip)
	apiGroup.Get("/stocks/insiders", handler.GetTopInsiders)

	// WebSocket route
	app.Get("/ws", ws.NewHandler(hub, stockSubChan, indexSubChan))

	log.Fatal(app.Listen(":" + cfg.Port))
}
