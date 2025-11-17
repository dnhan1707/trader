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

func (c *Client) fetchRaw(path string) (map[string]interface{}, error) {
	fullURL := fmt.Sprintf("%s%s", c.baseURL, path)

	req, err := http.NewRequestWithContext(context.Background(), "GET", fullURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

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
	path := fmt.Sprintf("/reference/tickers/%s?apiKey=%s", symbol, c.apiKey)
	return c.fetchRaw(path)
}

func (c *Client) GetCustomBars(stocksTicker, multiplier, timespan, from, to string, extra map[string]string) (map[string]interface{}, error) {
	path := fmt.Sprintf("/v2/aggs/ticker/%s/range/%s/%s/%s/%s", stocksTicker, multiplier, timespan, from, to)

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
