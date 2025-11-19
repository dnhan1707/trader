package intergration

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dnhan1707/trader/internal/api"
	"github.com/dnhan1707/trader/internal/massive"
	"github.com/dnhan1707/trader/tests/testutil"
	"github.com/gofiber/fiber/v2"
)

func TestIntegration_NewsCaching(t *testing.T) {
	var newsHits int
	up := testutil.NewUpstreamJSON(func(r *http.Request) any {
		if r.URL.Path == "/v2/reference/news" {
			newsHits++
			return map[string]any{"status": "OK", "count": newsHits}
		}
		return map[string]any{"status": "UNKNOWN"}
	})
	defer up.Close()

	mc, err := testutil.NewMiniCache(60)
	if err != nil {
		t.Fatalf("mini cache error: %v", err)
	}
	defer mc.Close()

	client := massive.New(up.URL, "test-key")
	h := api.New(mc.Cache, client)
	app := fiber.New()
	app.Get("/api/news", h.GetNews)

	// First request should hit upstream (newsHits becomes 1)
	req1 := httptest.NewRequest(http.MethodGet, "/api/news?ticker=AAPL&limit=1&sort=published_utc.desc", nil)
	resp1, err := app.Test(req1)
	if err != nil {
		t.Fatalf("first request error: %v", err)
	}
	var body1 map[string]any
	_ = json.NewDecoder(resp1.Body).Decode(&body1)
	if body1["count"].(float64) != 1 {
		t.Fatalf("expected count=1 got %v", body1["count"])
	}

	// Allow async cache set goroutine to complete
	time.Sleep(50 * time.Millisecond)

	// Second request should serve from cache; upstream not incremented
	req2 := httptest.NewRequest(http.MethodGet, "/api/news?ticker=AAPL&limit=1&sort=published_utc.desc", nil)
	resp2, err := app.Test(req2)
	if err != nil {
		t.Fatalf("second request error: %v", err)
	}
	var body2 map[string]any
	_ = json.NewDecoder(resp2.Body).Decode(&body2)
	if body2["count"].(float64) != 1 {
		t.Fatalf("expected cached count=1 got %v", body2["count"])
	}
	if newsHits != 1 {
		t.Fatalf("expected upstream hits=1 got %d (cache miss?)", newsHits)
	}
}

func TestIntegration_TickerDetails(t *testing.T) {
	var tickerHits int
	up := testutil.NewUpstreamJSON(func(r *http.Request) any {
		if r.URL.Path == "/v3/reference/tickers/AAPL" {
			tickerHits++
			return map[string]any{"status": "OK", "ticker": "AAPL", "hits": tickerHits}
		}
		return map[string]any{"status": "UNKNOWN"}
	})
	defer up.Close()

	mc, err := testutil.NewMiniCache(60)
	if err != nil {
		t.Fatalf("mini cache error: %v", err)
	}
	defer mc.Close()

	client := massive.New(up.URL, "test-key")
	h := api.New(mc.Cache, client)
	app := fiber.New()
	app.Get("/api/tickers/:symbol", h.GetTickerDetails)

	req1 := httptest.NewRequest(http.MethodGet, "/api/tickers/AAPL", nil)
	resp1, err := app.Test(req1)
	if err != nil {
		t.Fatalf("first ticker request error: %v", err)
	}
	var body1 map[string]any
	_ = json.NewDecoder(resp1.Body).Decode(&body1)
	if body1["ticker"] != "AAPL" || body1["hits"].(float64) != 1 {
		t.Fatalf("unexpected body1: %v", body1)
	}

	time.Sleep(50 * time.Millisecond)

	req2 := httptest.NewRequest(http.MethodGet, "/api/tickers/AAPL", nil)
	resp2, err := app.Test(req2)
	if err != nil {
		t.Fatalf("second ticker request error: %v", err)
	}
	var body2 map[string]any
	_ = json.NewDecoder(resp2.Body).Decode(&body2)
	if body2["hits"].(float64) != 1 {
		t.Fatalf("expected cached hits=1 got %v", body2["hits"])
	}
	if tickerHits != 1 {
		t.Fatalf("expected upstream hits=1 got %d (cache miss?)", tickerHits)
	}
}

func TestIntegration_CustomBars(t *testing.T) {
	var hits int
	up := testutil.NewUpstreamJSON(func(r *http.Request) any {
		if r.URL.Path == "/v2/aggs/ticker/AAPL/range/1/day/2024-01-01/2024-01-02" {
			hits++
			return map[string]any{"status": "OK", "hits": hits, "ticker": "AAPL"}
		}
		return map[string]any{"status": "UNKNOWN"}
	})
	defer up.Close()
	mc, _ := testutil.NewMiniCache(60)
	defer mc.Close()
	client := massive.New(up.URL, "k")
	h := api.New(mc.Cache, client)
	app := fiber.New()
	app.Get("/api/aggs/ticker/:stocksTicker/range/:multiplier/:timespan/:from/:to", h.GetCustomBars)
	req := httptest.NewRequest(http.MethodGet, "/api/aggs/ticker/AAPL/range/1/day/2024-01-01/2024-01-02?adjusted=true&limit=10", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body["ticker"] != "AAPL" || body["hits"].(float64) != 1 {
		t.Fatalf("unexpected body: %v", body)
	}
}

func TestIntegration_SMA(t *testing.T) {
	up := testutil.NewUpstreamJSON(func(r *http.Request) any {
		if r.URL.Path == "/v1/indicators/sma/AAPL" {
			return map[string]any{"status": "OK", "window": r.URL.Query().Get("window")}
		}
		return map[string]any{"status": "UNKNOWN"}
	})
	defer up.Close()
	mc, _ := testutil.NewMiniCache(60)
	defer mc.Close()
	client := massive.New(up.URL, "k")
	h := api.New(mc.Cache, client)
	app := fiber.New()
	app.Get("/api/indicators/sma/:stocksTicker", h.GetSMA)
	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/api/indicators/sma/AAPL?window=5", nil))
	if err != nil {
		t.Fatalf("sma req err: %v", err)
	}
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body["window"] != "5" {
		t.Fatalf("expected window=5 got %v", body)
	}
}

func TestIntegration_EMA(t *testing.T) {
	up := testutil.NewUpstreamJSON(func(r *http.Request) any {
		if r.URL.Path == "/v1/indicators/ema/AAPL" {
			return map[string]any{"status": "OK", "window": r.URL.Query().Get("window")}
		}
		return map[string]any{"status": "UNKNOWN"}
	})
	defer up.Close()
	mc, _ := testutil.NewMiniCache(60)
	defer mc.Close()
	client := massive.New(up.URL, "k")
	h := api.New(mc.Cache, client)
	app := fiber.New()
	app.Get("/api/indicators/ema/:stocksTicker", h.GetEMA)
	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/api/indicators/ema/AAPL?window=12", nil))
	if err != nil {
		t.Fatalf("ema req err: %v", err)
	}
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body["window"] != "12" {
		t.Fatalf("expected window=12 got %v", body)
	}
}

func TestIntegration_MACD(t *testing.T) {
	up := testutil.NewUpstreamJSON(func(r *http.Request) any {
		if r.URL.Path == "/v1/indicators/macd/AAPL" {
			return map[string]any{"status": "OK", "short_window": r.URL.Query().Get("short_window")}
		}
		return map[string]any{"status": "UNKNOWN"}
	})
	defer up.Close()
	mc, _ := testutil.NewMiniCache(60)
	defer mc.Close()
	client := massive.New(up.URL, "k")
	h := api.New(mc.Cache, client)
	app := fiber.New()
	app.Get("/api/indicators/macd/:stocksTicker", h.GetMACD)
	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/api/indicators/macd/AAPL?short_window=3&long_window=10", nil))
	if err != nil {
		t.Fatalf("macd req err: %v", err)
	}
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body["short_window"] != "3" {
		t.Fatalf("expected short_window=3 got %v", body)
	}
}

func TestIntegration_RSI(t *testing.T) {
	up := testutil.NewUpstreamJSON(func(r *http.Request) any {
		if r.URL.Path == "/v1/indicators/rsi/AAPL" {
			return map[string]any{"status": "OK", "window": r.URL.Query().Get("window")}
		}
		return map[string]any{"status": "UNKNOWN"}
	})
	defer up.Close()
	mc, _ := testutil.NewMiniCache(60)
	defer mc.Close()
	client := massive.New(up.URL, "k")
	h := api.New(mc.Cache, client)
	app := fiber.New()
	app.Get("/api/indicators/rsi/:stocksTicker", h.GetRSI)
	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/api/indicators/rsi/AAPL?window=14", nil))
	if err != nil {
		t.Fatalf("rsi req err: %v", err)
	}
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body["window"] != "14" {
		t.Fatalf("expected window=14 got %v", body)
	}
}

func TestIntegration_Exchanges(t *testing.T) {
	up := testutil.NewUpstreamJSON(func(r *http.Request) any {
		if r.URL.Path == "/v3/reference/exchanges" {
			return map[string]any{"status": "OK", "locale": r.URL.Query().Get("locale")}
		}
		return map[string]any{"status": "UNKNOWN"}
	})
	defer up.Close()
	mc, _ := testutil.NewMiniCache(60)
	defer mc.Close()
	client := massive.New(up.URL, "k")
	h := api.New(mc.Cache, client)
	app := fiber.New()
	app.Get("/api/exchanges", h.GetExchanges)
	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/api/exchanges?locale=US", nil))
	if err != nil {
		t.Fatalf("exchanges req err: %v", err)
	}
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body["locale"] != "US" {
		t.Fatalf("expected locale=US got %v", body)
	}
}

func TestIntegration_MarketHolidays(t *testing.T) {
	up := testutil.NewUpstreamJSON(func(r *http.Request) any {
		if r.URL.Path == "/v1/marketstatus/upcoming" {
			return map[string]any{"status": "OK", "kind": "upcoming"}
		}
		return map[string]any{"status": "UNKNOWN"}
	})
	defer up.Close()
	mc, _ := testutil.NewMiniCache(60)
	defer mc.Close()
	client := massive.New(up.URL, "k")
	h := api.New(mc.Cache, client)
	app := fiber.New()
	app.Get("/api/market/upcoming", h.GetMarketHolidays)
	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/api/market/upcoming", nil))
	if err != nil {
		t.Fatalf("market upcoming err: %v", err)
	}
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body["kind"] != "upcoming" {
		t.Fatalf("unexpected body: %v", body)
	}
}

func TestIntegration_MarketStatus(t *testing.T) {
	up := testutil.NewUpstreamJSON(func(r *http.Request) any {
		if r.URL.Path == "/v1/marketstatus/now" {
			return map[string]any{"status": "OK", "kind": "now"}
		}
		return map[string]any{"status": "UNKNOWN"}
	})
	defer up.Close()
	mc, _ := testutil.NewMiniCache(60)
	defer mc.Close()
	client := massive.New(up.URL, "k")
	h := api.New(mc.Cache, client)
	app := fiber.New()
	app.Get("/api/market/now", h.GetMarketStatus)
	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/api/market/now", nil))
	if err != nil {
		t.Fatalf("market now err: %v", err)
	}
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body["kind"] != "now" {
		t.Fatalf("unexpected body: %v", body)
	}
}

func TestIntegration_Conditions(t *testing.T) {
	up := testutil.NewUpstreamJSON(func(r *http.Request) any {
		if r.URL.Path == "/v3/reference/conditions" {
			return map[string]any{"status": "OK", "asset_class": r.URL.Query().Get("asset_class")}
		}
		return map[string]any{"status": "UNKNOWN"}
	})
	defer up.Close()
	mc, _ := testutil.NewMiniCache(60)
	defer mc.Close()
	client := massive.New(up.URL, "k")
	h := api.New(mc.Cache, client)
	app := fiber.New()
	app.Get("/api/market/condition", h.GetConditions)
	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/api/market/condition?asset_class=stocks", nil))
	if err != nil {
		t.Fatalf("conditions err: %v", err)
	}
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body["asset_class"] != "stocks" {
		t.Fatalf("unexpected body: %v", body)
	}
}

func TestIntegration_IPOs(t *testing.T) {
	up := testutil.NewUpstreamJSON(func(r *http.Request) any {
		if r.URL.Path == "/v3/reference/ipos" {
			return map[string]any{"status": "OK", "limit": r.URL.Query().Get("limit")}
		}
		return map[string]any{"status": "UNKNOWN"}
	})
	defer up.Close()
	mc, _ := testutil.NewMiniCache(60)
	defer mc.Close()
	client := massive.New(up.URL, "k")
	h := api.New(mc.Cache, client)
	app := fiber.New()
	app.Get("/api/ipos", h.GetIPOs)
	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/api/ipos?limit=2", nil))
	if err != nil {
		t.Fatalf("ipos err: %v", err)
	}
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body["limit"] != "2" {
		t.Fatalf("unexpected body: %v", body)
	}
}

func TestIntegration_Dividends(t *testing.T) {
	up := testutil.NewUpstreamJSON(func(r *http.Request) any {
		if r.URL.Path == "/v3/reference/dividends" {
			return map[string]any{"status": "OK", "ticker": r.URL.Query().Get("ticker")}
		}
		return map[string]any{"status": "UNKNOWN"}
	})
	defer up.Close()
	mc, _ := testutil.NewMiniCache(60)
	defer mc.Close()
	client := massive.New(up.URL, "k")
	h := api.New(mc.Cache, client)
	app := fiber.New()
	app.Get("/api/dividends", h.GetDividends)
	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/api/dividends?ticker=MSFT", nil))
	if err != nil {
		t.Fatalf("dividends err: %v", err)
	}
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body["ticker"] != "MSFT" {
		t.Fatalf("unexpected body: %v", body)
	}
}

func TestIntegration_ShortInterest(t *testing.T) {
	up := testutil.NewUpstreamJSON(func(r *http.Request) any {
		if r.URL.Path == "/stocks/v1/short-interest" {
			return map[string]any{"status": "OK", "ticker": r.URL.Query().Get("ticker")}
		}
		return map[string]any{"status": "UNKNOWN"}
	})
	defer up.Close()
	mc, _ := testutil.NewMiniCache(60)
	defer mc.Close()
	client := massive.New(up.URL, "k")
	h := api.New(mc.Cache, client)
	app := fiber.New()
	app.Get("/api/stocks/short-interest", h.GetShortInterest)
	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/api/stocks/short-interest?ticker=NVDA", nil))
	if err != nil {
		t.Fatalf("short-interest err: %v", err)
	}
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body["ticker"] != "NVDA" {
		t.Fatalf("unexpected body: %v", body)
	}
}

func TestIntegration_ShortVolume(t *testing.T) {
	up := testutil.NewUpstreamJSON(func(r *http.Request) any {
		if r.URL.Path == "/stocks/v1/short-volume" {
			return map[string]any{"status": "OK", "ticker": r.URL.Query().Get("ticker")}
		}
		return map[string]any{"status": "UNKNOWN"}
	})
	defer up.Close()
	mc, _ := testutil.NewMiniCache(60)
	defer mc.Close()
	client := massive.New(up.URL, "k")
	h := api.New(mc.Cache, client)
	app := fiber.New()
	app.Get("/api/stocks/short-volume", h.GetShortVolume)
	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/api/stocks/short-volume?ticker=TSLA", nil))
	if err != nil {
		t.Fatalf("short-volume err: %v", err)
	}
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body["ticker"] != "TSLA" {
		t.Fatalf("unexpected body: %v", body)
	}
}
