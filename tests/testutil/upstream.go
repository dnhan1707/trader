package testutil

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
)

func NewUpstreamJSON(handler func(r *http.Request) any) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(handler(r))
	}))
}
