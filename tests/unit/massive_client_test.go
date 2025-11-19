package unit

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dnhan1707/trader/internal/massive"
)

func TestClient_GetNews_URLAndParse(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v2/reference/news" {
			t.Fatalf("path=%s", r.URL.Path)
		}
		q := r.URL.Query()
		if q.Get("apiKey") != "test-key" {
			t.Fatalf("missing apiKey query")
		}
		if q.Get("ticker") != "AAPL" {
			t.Fatalf("ticker query mismatch: %s", q.Get("ticker"))
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "OK",
			"count":  1,
			"results": []any{
				map[string]any{"id": "n1", "title": "ok"},
			},
		})
	}))
	defer up.Close()
	c := massive.New(up.URL, "test-key")
	out, err := c.GetNews(map[string]string{"ticker": "AAPL", "limit": "1"})
	if err != nil {
		t.Fatalf("GetNews error: %v", err)
	}
	if out["status"] != "OK" {
		t.Fatalf("status=%v", out["status"])
	}
}

func TestClient_GetShortVolume(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/stocks/v1/short-volume" {
			t.Fatalf("path=%s", r.URL.Path)
		}
		q := r.URL.Query()
		if q.Get("ticker") != "TSLA" {
			t.Fatalf("expected ticker=TSLA got %s", q.Get("ticker"))
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":  "OK",
			"results": []any{map[string]any{"ticker": "TSLA", "short_volume": 12345}},
		})
	}))
	defer up.Close()
	c := massive.New(up.URL, "k")
	out, err := c.GetShortVolume(map[string]string{"ticker": "TSLA"})
	if err != nil {
		t.Fatalf("GetShortVolume error: %v", err)
	}
	if out["status"] != "OK" {
		t.Fatalf("status=%v", out["status"])
	}
}

func TestClient_GetTickerDetails(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/v3/reference/tickers/") {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "OK", "ticker": "AAPL"})
	}))
	defer up.Close()
	c := massive.New(up.URL, "k")
	out, err := c.GetTickerDetails("AAPL")
	if err != nil {
		t.Fatalf("GetTickerDetails error: %v", err)
	}
	if out["ticker"] != "AAPL" {
		t.Fatalf("ticker mismatch: %v", out["ticker"])
	}
}

func TestClient_ErrorStatus(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"bad"}`))
	}))
	defer up.Close()
	c := massive.New(up.URL, "k")
	_, err := c.GetNews(map[string]string{})
	if err == nil {
		t.Fatalf("expected error for status >=400")
	}
	if !strings.Contains(err.Error(), "massive error") {
		t.Fatalf("unexpected error text: %v", err)
	}
}

func TestClient_GetShortInterest(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/stocks/v1/short-interest" {
			t.Fatalf("path=%s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "OK", "results": []any{map[string]any{"ticker": "A", "short_interest": 999}}})
	}))
	defer up.Close()
	c := massive.New(up.URL, "k")
	out, err := c.GetShortInterest(map[string]string{"ticker": "A"})
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if out["status"] != "OK" {
		t.Fatalf("status mismatch: %v", out["status"])
	}
}

// Ensure buildURL attaches apiKey and filters empty values
func TestClient_SMA_QueryComposition(t *testing.T) {
	var captured string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/indicators/sma/AAPL" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		captured = r.URL.RawQuery
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "OK"})
	}))
	defer up.Close()
	c := massive.New(up.URL, "sekret")
	_, err := c.GetSMA("AAPL", map[string]string{"window": "10", "limit": "", "adjusted": "false"})
	if err != nil {
		t.Fatalf("GetSMA error: %v", err)
	}
	if !strings.Contains(captured, "apiKey=sekret") {
		t.Fatalf("apiKey missing in query: %s", captured)
	}
	if strings.Contains(captured, "limit=") {
		t.Fatalf("empty param appeared: %s", captured)
	}
	if !strings.Contains(captured, "window=10") || !strings.Contains(captured, "adjusted=false") {
		t.Fatalf("expected params missing: %s", captured)
	}
}

// Guard: fetchRaw propagates JSON unmarshal errors
func TestClient_fetchRaw_BadJSON(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("{not-json"))
	}))
	defer up.Close()
	c := massive.New(up.URL, "k")
	_, err := c.GetNews(nil)
	if err == nil {
		t.Fatalf("expected JSON error")
	}
	var syntaxErr *json.SyntaxError
	if !errors.As(err, &syntaxErr) {
		t.Fatalf("expected SyntaxError, got %v", err)
	}
}
