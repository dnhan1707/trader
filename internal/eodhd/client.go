package eodhd

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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

func (c *Client) GetCusipByTicker(ticker string) (string, error) {
	fullURL := fmt.Sprintf("%s/id-mapping?filter[symbol]=%s.US&page[limit]=1&page[offset]=0&api_token=%s&fmt=json", c.baseURL, ticker, c.apiKey)
	req, _ := http.NewRequest("GET", fullURL, nil)

	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("EODHD error: %s", string(body))
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", err
	}

	// Extract data array
	data, ok := result["data"].([]interface{})
	if !ok {
		return "", fmt.Errorf("invalid data format in response")
	}

	if len(data) == 0 {
		return "", fmt.Errorf("no data found for ticker %s", ticker)
	}

	// Extract first item
	firstItem, ok := data[0].(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("invalid item format in response")
	}

	// Extract CUSIP
	cusip, ok := firstItem["cusip"].(string)
	if !ok {
		return "", fmt.Errorf("cusip not found or invalid format for ticker %s", ticker)
	}

	return cusip, nil
}
