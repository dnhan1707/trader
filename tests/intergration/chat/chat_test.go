package chat

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dnhan1707/trader/internal/api"
	"github.com/dnhan1707/trader/internal/auth"
	"github.com/dnhan1707/trader/internal/services"
	"github.com/gofiber/fiber/v2"
	_ "github.com/lib/pq"
)

const (
	testDSN     = "postgres://trader_app:trader_app_123@localhost:5434/13f_filings?sslmode=disable"
	testSecret  = "test-secret"
	testExpires = "1h"
)

func setupTestApp(t *testing.T) (*fiber.App, *sql.DB) {
	t.Helper()

	db, err := sql.Open("postgres", testDSN)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	authService := services.NewAuthService(db)
	dmService := services.NewDMService(db)
	authHandler := api.NewAuthHandler(authService, testSecret, testExpires)
	dmHandler := api.NewDMHandler(dmService)

	app := fiber.New()

	// public auth routes
	app.Post("/api/auth/signup", authHandler.SignUp)
	app.Post("/api/auth/login", authHandler.Login)

	// protected routes
	apiGroup := app.Group("/api", auth.Middleware(testSecret))

	chatGroup := apiGroup.Group("/chat")
	chatGroup.Post("/dm/thread", dmHandler.CreateThread)
	chatGroup.Post("/dm/threads/:threadId/messages", dmHandler.SendMessage)
	chatGroup.Get("/dm/threads/:threadId/messages", dmHandler.ListMessages)
	chatGroup.Get("/dm/threads", dmHandler.ListThreads)
	chatGroup.Post("/dm/threads/:threadId/read", dmHandler.MarkThreadRead)

	return app, db
}

func doRequestJSON(t *testing.T, app *fiber.App, method, path, token string, body any, expectedStatus int, out any) {
	t.Helper()

	var buf *bytes.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		buf = bytes.NewReader(b)
	} else {
		buf = bytes.NewReader(nil)
	}

	req := httptest.NewRequest(method, path, buf)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != expectedStatus {
		var dbg any
		_ = json.NewDecoder(resp.Body).Decode(&dbg)
		t.Fatalf("expected status %d, got %d, body=%v", expectedStatus, resp.StatusCode, dbg)
	}

	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			t.Fatalf("decode response: %v", err)
		}
	}
}

func TestDMChatWorkflow(t *testing.T) {
	app, db := setupTestApp(t)
	defer db.Close()

	suffix := time.Now().UnixNano()
	usernameA := fmt.Sprintf("dm_test_userA_%d", suffix)
	usernameB := fmt.Sprintf("dm_test_userB_%d", suffix)

	const passwordA = "passwordA123!"
	const passwordB = "passwordB123!"

	type userPayload struct {
		ID       string `json:"id"`
		Username string `json:"username"`
	}
	type signupResp struct {
		User userPayload `json:"user"`
	}
	type loginResp struct {
		AccessToken string      `json:"accessToken"`
		User        userPayload `json:"user"`
	}

	// signup user A
	var sA signupResp
	doRequestJSON(t, app, http.MethodPost, "/api/auth/signup", "",
		map[string]string{"username": usernameA, "password": passwordA},
		http.StatusCreated, &sA)

	// signup user B
	var sB signupResp
	doRequestJSON(t, app, http.MethodPost, "/api/auth/signup", "",
		map[string]string{"username": usernameB, "password": passwordB},
		http.StatusCreated, &sB)

	userAID := sA.User.ID
	userBID := sB.User.ID

	// cleanup users + chat data at end
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM dm_thread_reads WHERE user_id IN ($1, $2)`, userAID, userBID)
		_, _ = db.Exec(`DELETE FROM dm_messages WHERE thread_id IN (
            SELECT id FROM dm_threads WHERE user1_id = $1 OR user2_id = $1 OR user1_id = $2 OR user2_id = $2
        )`, userAID, userBID)
		_, _ = db.Exec(`DELETE FROM dm_threads WHERE user1_id = $1 OR user2_id = $1 OR user1_id = $2 OR user2_id = $2`, userAID, userBID)
		_, _ = db.Exec(`DELETE FROM users WHERE id IN ($1, $2)`, userAID, userBID)
	})

	// login user A
	var lA loginResp
	doRequestJSON(t, app, http.MethodPost, "/api/auth/login", "",
		map[string]string{"username": usernameA, "password": passwordA},
		http.StatusOK, &lA)

	// login user B
	var lB loginResp
	doRequestJSON(t, app, http.MethodPost, "/api/auth/login", "",
		map[string]string{"username": usernameB, "password": passwordB},
		http.StatusOK, &lB)

	tokenA := lA.AccessToken
	tokenB := lB.AccessToken

	// create DM thread as A with B
	var threadResp map[string]any
	doRequestJSON(t, app, http.MethodPost, "/api/chat/dm/thread", tokenA,
		map[string]string{"otherUserId": userBID},
		http.StatusOK, &threadResp)

	threadID, ok := threadResp["ID"].(string)
	if !ok || threadID == "" {
		t.Fatalf("expected thread ID in response, got: %#v", threadResp)
	}

	// send message as A
	var msgResp map[string]any
	doRequestJSON(t, app, http.MethodPost, "/api/chat/dm/threads/"+threadID+"/messages", tokenA,
		map[string]string{"content": "hello from A"},
		http.StatusOK, &msgResp)

	if msgResp["Content"] == "" {
		t.Fatalf("expected message content in response, got: %#v", msgResp)
	}

	// list messages as B
	req := httptest.NewRequest(http.MethodGet, "/api/chat/dm/threads/"+threadID+"/messages?limit=10", nil)
	req.Header.Set("Authorization", "Bearer "+tokenB)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("list messages app.Test: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var dbg any
		_ = json.NewDecoder(resp.Body).Decode(&dbg)
		t.Fatalf("expected 200 from list messages, got %d, body=%v", resp.StatusCode, dbg)
	}

	var msgs []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&msgs); err != nil {
		t.Fatalf("decode messages: %v", err)
	}
	if len(msgs) == 0 {
		t.Fatalf("expected at least one message, got 0")
	}

	// list threads as B
	req2 := httptest.NewRequest(http.MethodGet, "/api/chat/dm/threads", nil)
	req2.Header.Set("Authorization", "Bearer "+tokenB)
	resp2, err := app.Test(req2)
	if err != nil {
		t.Fatalf("list threads app.Test: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		var dbg any
		_ = json.NewDecoder(resp2.Body).Decode(&dbg)
		t.Fatalf("expected 200 from list threads, got %d, body=%v", resp2.StatusCode, dbg)
	}

	var threads []map[string]any
	if err := json.NewDecoder(resp2.Body).Decode(&threads); err != nil {
		t.Fatalf("decode threads: %v", err)
	}
	if len(threads) == 0 {
		t.Fatalf("expected at least one thread for user B, got 0")
	}
}

// Tests unread count + mark-read behavior.
func TestDMUnreadCounts(t *testing.T) {
	app, db := setupTestApp(t)
	defer db.Close()

	suffix := time.Now().UnixNano()
	usernameA := fmt.Sprintf("dm_unread_userA_%d", suffix)
	usernameB := fmt.Sprintf("dm_unread_userB_%d", suffix)

	const passwordA = "passwordA123!"
	const passwordB = "passwordB123!"

	type userPayload struct {
		ID       string `json:"id"`
		Username string `json:"username"`
	}
	type signupResp struct {
		User userPayload `json:"user"`
	}
	type loginResp struct {
		AccessToken string      `json:"accessToken"`
		User        userPayload `json:"user"`
	}

	// signup A & B
	var sA signupResp
	doRequestJSON(t, app, http.MethodPost, "/api/auth/signup", "",
		map[string]string{"username": usernameA, "password": passwordA},
		http.StatusCreated, &sA)

	var sB signupResp
	doRequestJSON(t, app, http.MethodPost, "/api/auth/signup", "",
		map[string]string{"username": usernameB, "password": passwordB},
		http.StatusCreated, &sB)

	userAID := sA.User.ID
	userBID := sB.User.ID

	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM dm_thread_reads WHERE user_id IN ($1, $2)`, userAID, userBID)
		_, _ = db.Exec(`DELETE FROM dm_messages WHERE thread_id IN (
            SELECT id FROM dm_threads WHERE user1_id = $1 OR user2_id = $1 OR user1_id = $2 OR user2_id = $2
        )`, userAID, userBID)
		_, _ = db.Exec(`DELETE FROM dm_threads WHERE user1_id = $1 OR user2_id = $1 OR user1_id = $2 OR user2_id = $2`, userAID, userBID)
		_, _ = db.Exec(`DELETE FROM users WHERE id IN ($1, $2)`, userAID, userBID)
	})

	// login
	var lA loginResp
	doRequestJSON(t, app, http.MethodPost, "/api/auth/login", "",
		map[string]string{"username": usernameA, "password": passwordA},
		http.StatusOK, &lA)

	var lB loginResp
	doRequestJSON(t, app, http.MethodPost, "/api/auth/login", "",
		map[string]string{"username": usernameB, "password": passwordB},
		http.StatusOK, &lB)

	tokenA := lA.AccessToken
	tokenB := lB.AccessToken

	// create DM thread A-B
	var threadResp map[string]any
	doRequestJSON(t, app, http.MethodPost, "/api/chat/dm/thread", tokenA,
		map[string]string{"otherUserId": userBID},
		http.StatusOK, &threadResp)

	threadID, ok := threadResp["ID"].(string)
	if !ok || threadID == "" {
		t.Fatalf("expected thread ID in response, got: %#v", threadResp)
	}

	// send 2 messages from A to B
	doRequestJSON(t, app, http.MethodPost, "/api/chat/dm/threads/"+threadID+"/messages", tokenA,
		map[string]string{"content": "msg1 from A"},
		http.StatusOK, nil)
	doRequestJSON(t, app, http.MethodPost, "/api/chat/dm/threads/"+threadID+"/messages", tokenA,
		map[string]string{"content": "msg2 from A"},
		http.StatusOK, nil)

	// list threads as B, check unreadCount > 0
	req := httptest.NewRequest(http.MethodGet, "/api/chat/dm/threads", nil)
	req.Header.Set("Authorization", "Bearer "+tokenB)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("list threads app.Test: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var dbg any
		_ = json.NewDecoder(resp.Body).Decode(&dbg)
		t.Fatalf("expected 200 from list threads, got %d, body=%v", resp.StatusCode, dbg)
	}

	var threads []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&threads); err != nil {
		t.Fatalf("decode threads: %v", err)
	}
	if len(threads) == 0 {
		t.Fatalf("expected at least one thread for user B, got 0")
	}

	var found map[string]any
	for _, th := range threads {
		if th["ThreadID"] == threadID {
			found = th
			break
		}
	}
	if found == nil {
		t.Fatalf("expected thread %s in list for user B, got: %#v", threadID, threads)
	}

	unread, ok := found["UnreadCount"].(float64) // JSON numbers decode to float64
	if !ok {
		t.Fatalf("expected UnreadCount field, got: %#v", found)
	}
	if unread <= 0 {
		t.Fatalf("expected UnreadCount > 0, got %v", unread)
	}

	// mark thread as read for B
	doRequestJSON(t, app, http.MethodPost, "/api/chat/dm/threads/"+threadID+"/read", tokenB,
		nil, http.StatusNoContent, nil)

	// list threads again, unreadCount should be 0
	req2 := httptest.NewRequest(http.MethodGet, "/api/chat/dm/threads", nil)
	req2.Header.Set("Authorization", "Bearer "+tokenB)
	resp2, err := app.Test(req2)
	if err != nil {
		t.Fatalf("list threads after read app.Test: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		var dbg any
		_ = json.NewDecoder(resp2.Body).Decode(&dbg)
		t.Fatalf("expected 200 from list threads after read, got %d, body=%v", resp2.StatusCode, dbg)
	}

	var threads2 []map[string]any
	if err := json.NewDecoder(resp2.Body).Decode(&threads2); err != nil {
		t.Fatalf("decode threads2: %v", err)
	}
	var found2 map[string]any
	for _, th := range threads2 {
		if th["ThreadID"] == threadID {
			found2 = th
			break
		}
	}
	if found2 == nil {
		t.Fatalf("expected thread %s in list for user B (second call), got: %#v", threadID, threads2)
	}

	unread2, ok := found2["UnreadCount"].(float64)
	if !ok {
		t.Fatalf("expected UnreadCount in second response, got: %#v", found2)
	}
	if unread2 != 0 {
		t.Fatalf("expected UnreadCount == 0 after mark-read, got %v", unread2)
	}
}
