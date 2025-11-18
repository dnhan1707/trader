package massive

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

type Client struct {
	baseURL string
	apiKey  string
	http    *http.Client
}

func New(baseURL, apiKey string) *Client {
	return &Client{
		baseURL: baseURL,
		apiKey:  apiKey,
		http: &http.Client{
			//This ensures that any HTTP request made using this client will automatically fail and return an error
			// if the server does not respond or the connection takes longer than 10 seconds.
			// This prevents requests from hanging indefinitely.
			Timeout: 10 * time.Second,
		},
	}
}

// func (c *Client) fetch(path string) (map[string]interface{}, error) {
// 	req, _ := http.NewRequestWithContext(context.Background(), "GET", fmt.Sprintf("%s%s", c.baseURL, path), nil)
// 	req.Header.Set("Authorization", "Bearer "+c.apiKey)
// 	resp, err := c.http.Do(req)
// 	if err != nil {
// 		return nil, err
// 	}

// 	defer resp.Body.Close()
// 	body, _ := io.ReadAll(resp.Body)
// 	if resp.StatusCode >= 400 {
// 		return nil, fmt.Errorf("massive error: %s", string(body))
// 	}
// 	var result map[string]interface{}
// 	if err := json.Unmarshal(body, &result); err != nil {
// 		return nil, err
// 	}
// 	return result, nil
// }

func (c *Client) fetchRaw(fullURL string) (map[string]interface{}, error) {
	req, _ := http.NewRequestWithContext(context.Background(), "GET", fullURL, nil)
	// Prefer Authorization header; some Massive deployments also accept apiKey query param
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

func (c *Client) GetTickerDetails(symbol string) (map[string]interface{}, error) {
	path := fmt.Sprintf("%s/v3/reference/tickers/%s?apiKey=%s", c.baseURL, symbol, c.apiKey)
	return c.fetchRaw(path)
}

func (c *Client) GetCustomBars(stocksTicker, multiplier, timespan, from, to string, extra map[string]string) (map[string]interface{}, error) {
	path := fmt.Sprintf("%s/v2/aggs/ticker/%s/range/%s/%s/%s/%s", c.baseURL, stocksTicker, multiplier, timespan, from, to)

	values := url.Values{}
	values.Set("apiKey", c.apiKey)
	for k, v := range extra {
		if v != "" {
			values.Set(k, v)
		}
	}

	fullPath := path + "?" + values.Encode()
	return c.fetchRaw(fullPath)
}

func (c *Client) GetSMA(stocksTicker string, extra map[string]string) (map[string]interface{}, error) {
	path := fmt.Sprintf("%s/v1/indicators/sma/%s", c.baseURL, stocksTicker)

	values := url.Values{}
	// include apiKey in query as some endpoints expect it
	values.Set("apiKey", c.apiKey)
	for k, v := range extra {
		if v != "" {
			values.Set(k, v)
		}
	}

	fullPath := path
	if enc := values.Encode(); enc != "" {
		fullPath = path + "?" + enc
	}
	return c.fetchRaw(fullPath)
}

func (c *Client) GetEMA(stocksTicker string, extra map[string]string) (map[string]interface{}, error) {
	path := fmt.Sprintf("%s/v1/indicators/ema/%s", c.baseURL, stocksTicker)

	values := url.Values{}
	values.Set("apiKey", c.apiKey)
	for k, v := range extra {
		if v != "" {
			values.Set(k, v)
		}
	}

	fullPath := path
	if enc := values.Encode(); enc != "" {
		fullPath = path + "?" + enc
	}
	return c.fetchRaw(fullPath)
}

func (c *Client) GetMACD(stocksTicker string, extra map[string]string) (map[string]interface{}, error) {
	path := fmt.Sprintf("%s/v1/indicators/macd/%s", c.baseURL, stocksTicker)

	values := url.Values{}
	values.Set("apiKey", c.apiKey)
	for k, v := range extra {
		if v != "" {
			values.Set(k, v)
		}
	}

	fullPath := path
	if enc := values.Encode(); enc != "" {
		fullPath = path + "?" + enc
	}
	return c.fetchRaw(fullPath)
}

func (c *Client) GetRSI(stocksTicker string, extra map[string]string) (map[string]interface{}, error) {
	path := fmt.Sprintf("%s/v1/indicators/rsi/%s", c.baseURL, stocksTicker)

	values := url.Values{}
	values.Set("apiKey", c.apiKey)
	for k, v := range extra {
		if v != "" {
			values.Set(k, v)
		}
	}

	fullPath := path
	if enc := values.Encode(); enc != "" {
		fullPath = path + "?" + enc
	}
	return c.fetchRaw(fullPath)
}

func (c *Client) GetExchanges(extra map[string]string) (map[string]interface{}, error) {
	path := fmt.Sprintf("%s/v3/reference/exchanges", c.baseURL)

	values := url.Values{}
	values.Set("apiKey", c.apiKey)
	for k, v := range extra {
		if v != "" {
			values.Set(k, v)
		}
	}

	fullPath := path
	if enc := values.Encode(); enc != "" {
		fullPath = path + "?" + enc
	}
	return c.fetchRaw(fullPath)
}
