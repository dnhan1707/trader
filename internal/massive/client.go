package massive

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Client struct {
	baseURL string
	apiKey  string
	http    *http.Client
}

func New(baseURL, apiKey string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		http: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (c *Client) fetchRaw(fullURL string) (map[string]interface{}, error) {
	req, _ := http.NewRequestWithContext(context.Background(), "GET", fullURL, nil)
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("massive error: %s", string(body))
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) fetchAny(fullURL string) (interface{}, error) {
	req, _ := http.NewRequestWithContext(context.Background(), "GET", fullURL, nil)
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("massive error: %s", string(body))
	}

	var result interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// buildURL attaches apiKey and extra query params to a base path
func (c *Client) buildURL(path string, extra map[string]string) string {
	u := c.baseURL + path
	q := url.Values{}
	if c.apiKey != "" {
		q.Set("apiKey", c.apiKey)
	}
	for k, v := range extra {
		if v != "" {
			q.Set(k, v)
		}
	}
	if enc := q.Encode(); enc != "" {
		u += "?" + enc
	}
	return u
}

func (c *Client) GetTickerDetails(symbol string) (map[string]interface{}, error) {
	full := c.buildURL(fmt.Sprintf("/v3/reference/tickers/%s", symbol), nil)
	return c.fetchRaw(full)
}

func (c *Client) GetCustomBars(stocksTicker, multiplier, timespan, from, to string, extra map[string]string) (map[string]interface{}, error) {
	full := c.buildURL(
		fmt.Sprintf("/v2/aggs/ticker/%s/range/%s/%s/%s/%s", stocksTicker, multiplier, timespan, from, to),
		extra,
	)
	return c.fetchRaw(full)
}

func (c *Client) GetSMA(stocksTicker string, extra map[string]string) (map[string]interface{}, error) {
	full := c.buildURL(fmt.Sprintf("/v1/indicators/sma/%s", stocksTicker), extra)
	return c.fetchRaw(full)
}

func (c *Client) GetEMA(stocksTicker string, extra map[string]string) (map[string]interface{}, error) {
	full := c.buildURL(fmt.Sprintf("/v1/indicators/ema/%s", stocksTicker), extra)
	return c.fetchRaw(full)
}

func (c *Client) GetMACD(stocksTicker string, extra map[string]string) (map[string]interface{}, error) {
	full := c.buildURL(fmt.Sprintf("/v1/indicators/macd/%s", stocksTicker), extra)
	return c.fetchRaw(full)
}

func (c *Client) GetRSI(stocksTicker string, extra map[string]string) (map[string]interface{}, error) {
	full := c.buildURL(fmt.Sprintf("/v1/indicators/rsi/%s", stocksTicker), extra)
	return c.fetchRaw(full)
}

func (c *Client) GetExchanges(extra map[string]string) (map[string]interface{}, error) {
	full := c.buildURL("/v3/reference/exchanges", extra)
	return c.fetchRaw(full)
}

func (c *Client) GetMarketHolidays() (interface{}, error) {
	full := c.buildURL("/v1/marketstatus/upcoming", nil)
	return c.fetchAny(full)
}

func (c *Client) GetMarketStatus() (map[string]interface{}, error) {
	full := c.buildURL("/v1/marketstatus/now", nil)
	return c.fetchRaw(full)
}

func (c *Client) GetConditions(extra map[string]string) (map[string]interface{}, error) {
	full := c.buildURL("/v3/reference/conditions", extra)
	return c.fetchRaw(full)
}

func (c *Client) GetIPOs(extra map[string]string) (map[string]interface{}, error) {
	// Reference endpoints live under v3
	full := c.buildURL("/vX/reference/ipos", extra)
	return c.fetchRaw(full)
}

func (c *Client) GetDividends(extra map[string]string) (map[string]interface{}, error) {
	full := c.buildURL("/v3/reference/dividends", extra)
	return c.fetchRaw(full)
}

func (c *Client) GetShortInterest(extra map[string]string) (map[string]interface{}, error) {
	full := c.buildURL("/stocks/v1/short-interest", extra)
	return c.fetchRaw(full)
}

func (c *Client) GetShortVolume(extra map[string]string) (map[string]interface{}, error) {
	full := c.buildURL("/stocks/v1/short-volume", extra)
	return c.fetchRaw(full)
}

func (c *Client) GetNews(extra map[string]string) (map[string]interface{}, error) {
	full := c.buildURL("/v2/reference/news", extra)
	return c.fetchRaw(full)
}
