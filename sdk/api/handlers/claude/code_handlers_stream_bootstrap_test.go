package claude

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
)

type delayedClaudeStreamExecutor struct {
	release <-chan struct{}
}

func (e *delayedClaudeStreamExecutor) Identifier() string { return "codex" }

func (e *delayedClaudeStreamExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *delayedClaudeStreamExecutor) ExecuteStream(ctx context.Context, _ *coreauth.Auth, _ coreexecutor.Request, _ coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	chunks := make(chan coreexecutor.StreamChunk)
	go func() {
		defer close(chunks)
		select {
		case <-ctx.Done():
		case <-e.release:
			select {
			case <-ctx.Done():
			case chunks <- coreexecutor.StreamChunk{Payload: []byte("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")}:
			}
		}
	}()
	return &coreexecutor.StreamResult{Chunks: chunks}, nil
}

func (e *delayedClaudeStreamExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *delayedClaudeStreamExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *delayedClaudeStreamExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

type stalledClaudeStreamExecutor struct {
	canceled   chan struct{}
	cancelOnce sync.Once
}

func (e *stalledClaudeStreamExecutor) Identifier() string { return "codex" }

func (e *stalledClaudeStreamExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *stalledClaudeStreamExecutor) ExecuteStream(ctx context.Context, _ *coreauth.Auth, _ coreexecutor.Request, _ coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	chunks := make(chan coreexecutor.StreamChunk)
	go func() {
		defer close(chunks)
		select {
		case <-ctx.Done():
			e.cancelOnce.Do(func() { close(e.canceled) })
			return
		case chunks <- coreexecutor.StreamChunk{Payload: []byte("event: message_start\ndata: {\"type\":\"message_start\"}\n\n")}:
		}
		<-ctx.Done()
		e.cancelOnce.Do(func() { close(e.canceled) })
	}()
	return &coreexecutor.StreamResult{Chunks: chunks}, nil
}

func (e *stalledClaudeStreamExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *stalledClaudeStreamExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e *stalledClaudeStreamExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func TestClaudeStreamingSendsConfiguredKeepAliveBeforeFirstChunk(t *testing.T) {
	gin.SetMode(gin.TestMode)
	releaseChan := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseChan) }) }
	defer release()

	executor := &delayedClaudeStreamExecutor{release: releaseChan}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	authID := t.Name() + "-auth"
	auth := &coreauth.Auth{ID: authID, Provider: executor.Identifier(), Status: coreauth.StatusActive}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("register auth: %v", err)
	}
	modelID := t.Name() + "-model"
	registry.GetGlobalRegistry().RegisterClient(authID, auth.Provider, []*registry.ModelInfo{{ID: modelID}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })

	cfg := &sdkconfig.SDKConfig{Streaming: sdkconfig.StreamingConfig{KeepAliveSeconds: 1}}
	handler := NewClaudeCodeAPIHandler(handlers.NewBaseAPIHandlers(cfg, manager))
	router := gin.New()
	router.POST("/v1/messages", handler.ClaudeMessages)
	server := httptest.NewServer(router)
	defer server.Close()

	req, err := http.NewRequest(http.MethodPost, server.URL+"/v1/messages", strings.NewReader(`{"model":"`+modelID+`","stream":true,"max_tokens":16,"messages":[{"role":"user","content":"hello"}]}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	client := server.Client()
	client.Timeout = 2500 * time.Millisecond

	started := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("stream did not start before the first upstream chunk: %v", err)
	}
	if elapsed := time.Since(started); elapsed >= 2*time.Second {
		t.Fatalf("stream headers took %s, want less than 2s", elapsed)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(got, "text/event-stream") {
		t.Fatalf("Content-Type = %q, want text/event-stream", got)
	}

	release()
	body, err := io.ReadAll(resp.Body)
	if errClose := resp.Body.Close(); err == nil && errClose != nil {
		err = errClose
	}
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	if !strings.HasPrefix(string(body), ": keep-alive\n\n") {
		t.Fatalf("body = %q, want pre-first-chunk keep-alive", body)
	}
}

func TestClaudeStreamingBootstrapTimeoutReturnsGatewayTimeout(t *testing.T) {
	gin.SetMode(gin.TestMode)
	releaseChan := make(chan struct{})
	defer close(releaseChan)

	executor := &delayedClaudeStreamExecutor{release: releaseChan}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	authID := t.Name() + "-auth"
	auth := &coreauth.Auth{ID: authID, Provider: executor.Identifier(), Status: coreauth.StatusActive}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("register auth: %v", err)
	}
	modelID := t.Name() + "-model"
	registry.GetGlobalRegistry().RegisterClient(authID, auth.Provider, []*registry.ModelInfo{{ID: modelID}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })

	cfg := &sdkconfig.SDKConfig{Streaming: sdkconfig.StreamingConfig{BootstrapTimeoutSeconds: 1}}
	handler := NewClaudeCodeAPIHandler(handlers.NewBaseAPIHandlers(cfg, manager))
	router := gin.New()
	router.POST("/v1/messages", handler.ClaudeMessages)
	server := httptest.NewServer(router)
	defer server.Close()

	req, err := http.NewRequest(http.MethodPost, server.URL+"/v1/messages", strings.NewReader(`{"model":"`+modelID+`","stream":true,"max_tokens":16,"messages":[{"role":"user","content":"hello"}]}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	client := server.Client()
	client.Timeout = 2500 * time.Millisecond

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request did not return after bootstrap timeout: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	if errClose := resp.Body.Close(); err == nil && errClose != nil {
		err = errClose
	}
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	if resp.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want %d; body=%s", resp.StatusCode, http.StatusGatewayTimeout, body)
	}
	if !strings.Contains(string(body), `"type":"timeout_error"`) {
		t.Fatalf("body = %s, want Claude timeout error", body)
	}
}

func TestClaudeStreamingIdleTimeoutTerminatesCommittedStream(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &stalledClaudeStreamExecutor{canceled: make(chan struct{})}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	authID := t.Name() + "-auth"
	auth := &coreauth.Auth{ID: authID, Provider: executor.Identifier(), Status: coreauth.StatusActive}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("register auth: %v", err)
	}
	modelID := t.Name() + "-model"
	registry.GetGlobalRegistry().RegisterClient(authID, auth.Provider, []*registry.ModelInfo{{ID: modelID}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })

	cfg := &sdkconfig.SDKConfig{Streaming: sdkconfig.StreamingConfig{IdleTimeoutSeconds: 1}}
	handler := NewClaudeCodeAPIHandler(handlers.NewBaseAPIHandlers(cfg, manager))
	router := gin.New()
	router.POST("/v1/messages", handler.ClaudeMessages)
	server := httptest.NewServer(router)
	defer server.Close()

	req, err := http.NewRequest(http.MethodPost, server.URL+"/v1/messages", strings.NewReader(`{"model":"`+modelID+`","stream":true,"max_tokens":16,"messages":[{"role":"user","content":"hello"}]}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	client := server.Client()
	client.Timeout = 2500 * time.Millisecond

	started := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request did not commit first stream chunk: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	if errClose := resp.Body.Close(); err == nil && errClose != nil {
		err = errClose
	}
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	if elapsed := time.Since(started); elapsed >= 2*time.Second {
		t.Fatalf("idle timeout took %s, want less than 2s", elapsed)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want committed 200; body=%s", resp.StatusCode, body)
	}
	if count := strings.Count(string(body), "event: error"); count != 1 {
		t.Fatalf("error event count = %d, want 1; body=%s", count, body)
	}
	if !strings.Contains(string(body), `"type":"timeout_error"`) {
		t.Fatalf("body = %s, want Claude timeout error", body)
	}
	if strings.Contains(string(body), "message_stop") {
		t.Fatalf("body = %s, must not synthesize message_stop", body)
	}
	select {
	case <-executor.canceled:
	case <-time.After(time.Second):
		t.Fatal("idle-timeout stream context was not canceled")
	}
}
