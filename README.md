# Trader API

Lightweight Go/Fiber API that proxies selected Massive.com endpoints with Redis caching. Designed to be frontend-friendly and easy to extend.

- Server: Go + Fiber
- Cache: Redis (TTL-based)
- HTTP client: centralized Massive client with URL/query helpers
- Testing: httptest + miniredis (no real network needed)

## Demo (frontend is at: https://github.com/dnhan1707/trader-view)
<img width="2549" height="1215" alt="traderview" src="https://github.com/user-attachments/assets/e36f2efb-b0f8-46fb-a629-eb9ec19a9f29" />


## Quickstart (Windows/PowerShell)

1) Requirements
- Go 1.21+
- Redis (local or Docker)
- Massive.com API key

2) Start Redis (Docker)
- docker run --name trader-redis -p 6379:6379 -d redis:7

3) Environment variables
Set these before running (adjust as needed):
- $env:PORT="8080"
- $env:MASSIVE_BASE="https://api.massive.com"
- $env:MASSIVE_KEY="<your-api-key>"
- $env:REDIS_ADDR="localhost:6379"
- $env:REDIS_PASS=""
- $env:REDIS_DB="0"
- $env:CACHE_TTL="300"    # seconds

4) Run the server
- go run ./cmd/server

Open http://localhost:8080/health

## Wire routes (cmd/server/main.go)

Ensure your main registers the routes you need:
````go
app.Get("/health", func(c *fiber.Ctx) error { return c.SendString("ok") })

app.Get("/api/tickers/:symbol", handler.GetTickerDetails)

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
