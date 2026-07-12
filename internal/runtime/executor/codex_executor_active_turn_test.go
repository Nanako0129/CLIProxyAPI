package executor

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	_ "github.com/router-for-me/CLIProxyAPI/v7/internal/translator"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/tidwall/gjson"
)

func activeTurnClaudeRequest(promptID string) (cliproxyexecutor.Request, cliproxyexecutor.Options) {
	payload := []byte(`{
		"model":"gpt-5.6-sol",
		"metadata":{"user_id":"{\"device_id\":\"device-a\",\"session_id\":\"session-a\"}"},
		"messages":[{"role":"user","content":[{"type":"text","text":"work"}]}]
	}`)
	return cliproxyexecutor.Request{Model: "gpt-5.6-sol", Payload: payload}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString("claude"),
		Metadata: map[string]any{
			cliproxyexecutor.CodexActiveTurnBridgeMetadataKey: true,
		},
		Headers: http.Header{
			calicoActiveTurnHeader:     []string{calicoActiveTurnVersion},
			calicoPromptIDHeader:       []string{promptID},
			"X-Claude-Code-Session-Id": []string{"session-a"},
			"X-Claude-Code-Agent-Id":   []string{"agent-a"},
		},
	}
}

func TestCodexExecutorClaudeActiveTurnReplaysServerState(t *testing.T) {
	executor := NewCodexExecutor(nil)
	auth := &cliproxyauth.Auth{ID: "credential-a"}
	req, opts := activeTurnClaudeRequest("11111111-1111-4111-8111-111111111111")
	body := []byte(`{"model":"gpt-5.6-sol","input":[]}`)

	firstBody, first, releaseFirst, err := executor.prepareClaudeActiveTurn(context.Background(), opts.SourceFormat, auth, req, opts, body)
	if err != nil {
		t.Fatalf("prepare first: %v", err)
	}
	if first == nil || releaseFirst == nil {
		t.Fatal("active turn was not enabled")
	}
	firstTurnID := first.turn.TurnID()
	metadataRaw := gjson.GetBytes(firstBody, "client_metadata.x-codex-turn-metadata").String()
	if got := gjson.Get(metadataRaw, "turn_id").String(); got != firstTurnID {
		t.Fatalf("turn metadata turn_id = %q, want %q", got, firstTurnID)
	}
	if got := gjson.Get(metadataRaw, "request_kind").String(); got != "turn" {
		t.Fatalf("request_kind = %q, want turn", got)
	}
	for _, path := range []string{"client_metadata.x-codex-installation-id", "client_metadata.session_id", "client_metadata.thread_id", "client_metadata.x-codex-window-id", "client_metadata.turn_id", "prompt_cache_key"} {
		if got := gjson.GetBytes(firstBody, path).String(); got == "" {
			t.Fatalf("%s is empty", path)
		}
	}

	firstHTTP, err := http.NewRequest(http.MethodPost, "https://example.com/responses", nil)
	if err != nil {
		t.Fatalf("new first request: %v", err)
	}
	applyClaudeActiveTurnHeaders(firstHTTP, firstBody, first)
	if got := firstHTTP.Header.Get(codexTurnStateHeader); got != "" {
		t.Fatalf("initial turn state = %q, want empty", got)
	}
	captureClaudeActiveTurnState(http.Header{codexTurnStateHeader: []string{"server-state-a"}}, first)
	releaseFirst()

	secondBody, second, releaseSecond, err := executor.prepareClaudeActiveTurn(context.Background(), opts.SourceFormat, auth, req, opts, body)
	if err != nil {
		t.Fatalf("prepare second: %v", err)
	}
	defer releaseSecond()
	if second.turn.TurnID() != firstTurnID {
		t.Fatalf("continuation turn id = %q, want %q", second.turn.TurnID(), firstTurnID)
	}
	secondHTTP, err := http.NewRequest(http.MethodPost, "https://example.com/responses", nil)
	if err != nil {
		t.Fatalf("new second request: %v", err)
	}
	applyClaudeActiveTurnHeaders(secondHTTP, secondBody, second)
	if got := secondHTTP.Header.Get(codexTurnStateHeader); got != "server-state-a" {
		t.Fatalf("continuation turn state = %q, want server-state-a", got)
	}
	if secondHTTP.Header.Get("Session-Id") == "" || secondHTTP.Header.Get("Thread-Id") == "" {
		t.Fatal("native Codex identity headers are incomplete")
	}
}

func TestCodexExecutorClaudeActiveTurnSeparatesNextPrompt(t *testing.T) {
	executor := NewCodexExecutor(nil)
	auth := &cliproxyauth.Auth{ID: "credential-a"}
	body := []byte(`{"model":"gpt-5.6-sol","input":[]}`)
	firstReq, firstOpts := activeTurnClaudeRequest("11111111-1111-4111-8111-111111111111")
	_, first, releaseFirst, err := executor.prepareClaudeActiveTurn(context.Background(), firstOpts.SourceFormat, auth, firstReq, firstOpts, body)
	if err != nil {
		t.Fatalf("prepare first: %v", err)
	}
	releaseFirst()

	secondReq, secondOpts := activeTurnClaudeRequest("22222222-2222-4222-8222-222222222222")
	_, second, releaseSecond, err := executor.prepareClaudeActiveTurn(context.Background(), secondOpts.SourceFormat, auth, secondReq, secondOpts, body)
	if err != nil {
		t.Fatalf("prepare second: %v", err)
	}
	defer releaseSecond()
	if first.turn.TurnID() == second.turn.TurnID() {
		t.Fatal("new prompt reused the previous Codex turn")
	}
}

func TestCodexExecutorClaudeActiveTurnKeepsIdentityAcrossModelFallback(t *testing.T) {
	executor := NewCodexExecutor(nil)
	auth := &cliproxyauth.Auth{ID: "credential-a"}
	body := []byte(`{"model":"gpt-5.6-sol","input":[]}`)
	req, opts := activeTurnClaudeRequest("11111111-1111-4111-8111-111111111111")
	firstBody, first, releaseFirst, err := executor.prepareClaudeActiveTurn(context.Background(), opts.SourceFormat, auth, req, opts, body)
	if err != nil {
		t.Fatalf("prepare first: %v", err)
	}
	firstCache := gjson.GetBytes(firstBody, "prompt_cache_key").String()
	releaseFirst()

	req.Model = "gpt-5.6-terra"
	secondBody, second, releaseSecond, err := executor.prepareClaudeActiveTurn(context.Background(), opts.SourceFormat, auth, req, opts, body)
	if err != nil {
		t.Fatalf("prepare fallback: %v", err)
	}
	defer releaseSecond()
	if got := gjson.GetBytes(secondBody, "prompt_cache_key").String(); got != firstCache {
		t.Fatalf("fallback prompt_cache_key = %q, want %q", got, firstCache)
	}
	if second.turn.TurnID() != first.turn.TurnID() {
		t.Fatal("model fallback changed active turn identity")
	}
}

func TestCodexExecutorClaudeActiveTurnFailsClosedWithoutVersionedPrompt(t *testing.T) {
	executor := NewCodexExecutor(nil)
	auth := &cliproxyauth.Auth{ID: "credential-a"}
	req, opts := activeTurnClaudeRequest("not-a-uuid")
	body := []byte(`{"model":"gpt-5.6-sol","input":[]}`)

	gotBody, active, release, err := executor.prepareClaudeActiveTurn(context.Background(), opts.SourceFormat, auth, req, opts, body)
	if err != nil {
		t.Fatalf("prepare invalid prompt: %v", err)
	}
	if active != nil || release != nil {
		t.Fatal("invalid Calico prompt id enabled active-turn state")
	}
	if string(gotBody) != string(body) {
		t.Fatalf("inactive body changed: %s", string(gotBody))
	}
}

func TestCodexExecutorExecuteReplaysActiveTurnAcrossClaudeRequests(t *testing.T) {
	var mu sync.Mutex
	var requestCount int
	var firstTurnID string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, errRead := io.ReadAll(r.Body)
		if errRead != nil {
			t.Errorf("read upstream request: %v", errRead)
			return
		}
		metadataRaw := gjson.GetBytes(body, "client_metadata.x-codex-turn-metadata").String()
		turnID := gjson.Get(metadataRaw, "turn_id").String()

		mu.Lock()
		requestCount++
		current := requestCount
		if current == 1 {
			firstTurnID = turnID
		} else if turnID != firstTurnID {
			t.Errorf("request %d turn_id = %q, want %q", current, turnID, firstTurnID)
		}
		mu.Unlock()

		if current == 1 && r.Header.Get(codexTurnStateHeader) != "" {
			t.Errorf("initial request unexpectedly sent turn state")
		}
		if current == 2 && r.Header.Get(codexTurnStateHeader) != "server-state-a" {
			t.Errorf("continuation turn state = %q, want server-state-a", r.Header.Get(codexTurnStateHeader))
		}
		if r.Header.Get("Thread-Id") == "" || r.Header.Get("Session-Id") == "" {
			t.Errorf("request %d missing native identity headers", current)
		}

		w.Header().Set("Content-Type", "text/event-stream")
		if current == 1 {
			w.Header().Set(codexTurnStateHeader, "server-state-a")
		}
		_, _ = w.Write([]byte(`data: {"type":"response.completed","response":{"id":"resp_1","object":"response","created_at":1775555723,"status":"completed","model":"gpt-5.6-sol","output":[{"id":"msg_1","type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":"ok","annotations":[]}]}],"usage":{"input_tokens":8,"output_tokens":1,"total_tokens":9}}}` + "\n\n"))
	}))
	defer server.Close()

	executor := NewCodexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{ID: "credential-a", Attributes: map[string]string{
		"base_url": server.URL,
		"api_key":  "test",
	}}
	req, opts := activeTurnClaudeRequest("11111111-1111-4111-8111-111111111111")
	for i := 0; i < 2; i++ {
		if _, err := executor.Execute(context.Background(), auth, req, opts); err != nil {
			t.Fatalf("Execute request %d: %v", i+1, err)
		}
	}
	if requestCount != 2 {
		t.Fatalf("request count = %d, want 2", requestCount)
	}
}

func TestCodexExecutorHardUsageLimitTerminatesActiveTurn(t *testing.T) {
	var mu sync.Mutex
	var requestCount int
	var firstTurnID string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, errRead := io.ReadAll(r.Body)
		if errRead != nil {
			t.Errorf("read upstream request: %v", errRead)
			return
		}
		metadataRaw := gjson.GetBytes(body, "client_metadata.x-codex-turn-metadata").String()
		turnID := gjson.Get(metadataRaw, "turn_id").String()

		mu.Lock()
		requestCount++
		current := requestCount
		if current == 1 {
			firstTurnID = turnID
		}
		mu.Unlock()

		switch current {
		case 1:
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set(codexTurnStateHeader, "server-state-a")
			_, _ = w.Write([]byte(`data: {"type":"response.completed","response":{"id":"resp_1","status":"completed","model":"gpt-5.6-sol","output":[]}}` + "\n\n"))
		case 2:
			if got := r.Header.Get(codexTurnStateHeader); got != "server-state-a" {
				t.Errorf("second request turn state = %q, want server-state-a", got)
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":{"type":"usage_limit_reached","message":"limit"}}`))
		case 3:
			if got := r.Header.Get(codexTurnStateHeader); got != "" {
				t.Errorf("new request replayed terminated state %q", got)
			}
			if turnID == firstTurnID {
				t.Errorf("new request reused terminated turn id %q", turnID)
			}
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte(`data: {"type":"response.completed","response":{"id":"resp_3","status":"completed","model":"gpt-5.6-sol","output":[]}}` + "\n\n"))
		}
	}))
	defer server.Close()

	executor := NewCodexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{ID: "credential-a", Attributes: map[string]string{
		"base_url": server.URL,
		"api_key":  "test",
	}}
	req, opts := activeTurnClaudeRequest("11111111-1111-4111-8111-111111111111")
	if _, err := executor.Execute(context.Background(), auth, req, opts); err != nil {
		t.Fatalf("first Execute: %v", err)
	}
	if _, err := executor.Execute(context.Background(), auth, req, opts); err == nil {
		t.Fatal("hard usage limit unexpectedly succeeded")
	}
	if _, err := executor.Execute(context.Background(), auth, req, opts); err != nil {
		t.Fatalf("third Execute: %v", err)
	}
}
