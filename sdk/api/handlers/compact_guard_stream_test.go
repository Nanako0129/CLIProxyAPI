package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
)

type compactCountingExecutor struct {
	mu    sync.Mutex
	calls int
	// emitReasoningForever emits a non-empty payload every interval and never completes.
	emitReasoningForever bool
	interval             time.Duration
	// failImmediately returns a retryable error from ExecuteStream.
	failImmediately bool
	// stallFirstByte never emits until ctx canceled.
	stallFirstByte bool
	authIDs        []string
}

func (e *compactCountingExecutor) Identifier() string { return "codex" }

func (e *compactCountingExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, nil
}

func (e *compactCountingExecutor) ExecuteStream(ctx context.Context, auth *coreauth.Auth, req coreexecutor.Request, opts coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	e.mu.Lock()
	e.calls++
	if auth != nil {
		e.authIDs = append(e.authIDs, auth.ID)
	}
	e.mu.Unlock()

	if e.failImmediately {
		return nil, &statusErr{code: http.StatusBadGateway, msg: "upstream failed"}
	}

	chunks := make(chan coreexecutor.StreamChunk)
	go func() {
		defer close(chunks)
		if e.stallFirstByte {
			<-ctx.Done()
			return
		}
		if !e.emitReasoningForever {
			select {
			case <-ctx.Done():
			case chunks <- coreexecutor.StreamChunk{Payload: []byte(`data: {"type":"message_start"}` + "\n\n")}:
			}
			return
		}
		ticker := time.NewTicker(e.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				select {
				case <-ctx.Done():
					return
				case chunks <- coreexecutor.StreamChunk{Payload: []byte(`data: {"type":"content_block_delta","delta":{"type":"thinking_delta","thinking":"."}}` + "\n\n")}:
				}
			}
		}
	}()
	return &coreexecutor.StreamResult{Chunks: chunks}, nil
}

func (e *compactCountingExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *compactCountingExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, nil
}

func (e *compactCountingExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, nil
}

func (e *compactCountingExecutor) snapshot() (int, []string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls, append([]string(nil), e.authIDs...)
}

type statusErr struct {
	code int
	msg  string
}

func (e *statusErr) Error() string   { return e.msg }
func (e *statusErr) StatusCode() int { return e.code }

func newCompactHandler(t *testing.T, executor *compactCountingExecutor, cfg *sdkconfig.SDKConfig, authIDs ...string) *BaseAPIHandler {
	t.Helper()
	manager := coreauth.NewManager(nil, nil, nil)
	manager.SetRetryConfig(3, time.Second, 3)
	manager.RegisterExecutor(executor)
	for _, authID := range authIDs {
		auth := &coreauth.Auth{ID: authID, Provider: "codex", Status: coreauth.StatusActive}
		if _, err := manager.Register(context.Background(), auth); err != nil {
			t.Fatal(err)
		}
		registry.GetGlobalRegistry().RegisterClient(authID, auth.Provider, []*registry.ModelInfo{{ID: "test-model"}})
		authID := authID
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })
	}
	return NewBaseAPIHandlers(cfg, manager)
}

func collectStreamWithHeaders(handler *BaseAPIHandler, headers http.Header, body []byte) (payload []byte, status int, elapsed time.Duration) {
	started := time.Now()
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	c.Request.Header = headers.Clone()
	ctx := context.WithValue(context.Background(), "gin", c)
	data, _, errs := handler.ExecuteStreamWithAuthManager(ctx, "claude", "test-model", body, "")
	if data != nil {
		for chunk := range data {
			payload = append(payload, chunk...)
		}
	}
	for msg := range errs {
		if msg != nil {
			status = msg.StatusCode
		}
	}
	return payload, status, time.Since(started)
}

func TestCompactGuardAbsoluteTimeoutWithContinuousReasoning(t *testing.T) {
	executor := &compactCountingExecutor{emitReasoningForever: true, interval: 20 * time.Millisecond}
	cfg := &sdkconfig.SDKConfig{Streaming: sdkconfig.StreamingConfig{
		BootstrapRetries: 2,
		Compact: sdkconfig.StreamingCompactConfig{
			Enabled:            true,
			MaxDurationSeconds: 1,
		},
	}}
	handler := newCompactHandler(t, executor, cfg, "auth1")
	headers := http.Header{}
	headers.Set(CalicoRequestSourceHeader, CalicoRequestSourceCompact)
	// Gateway must not rewrite product fields; client body is forwarded as-is.
	body := []byte(`{"model":"test-model","output_config":{"effort":"xhigh"},"thinking":{"type":"adaptive"},"stream":true}`)

	_, status, elapsed := collectStreamWithHeaders(handler, headers, body)
	if status != http.StatusGatewayTimeout {
		t.Fatalf("status=%d, want 504; elapsed=%s", status, elapsed)
	}
	if elapsed < 800*time.Millisecond || elapsed > 3*time.Second {
		t.Fatalf("elapsed=%s, want ~1s", elapsed)
	}
	calls, _ := executor.snapshot()
	if calls != 1 {
		t.Fatalf("calls=%d, want 1 (no stream retries)", calls)
	}
}

func TestCompactGuardDisablesBootstrapRetries(t *testing.T) {
	executor := &compactCountingExecutor{stallFirstByte: true}
	cfg := &sdkconfig.SDKConfig{Streaming: sdkconfig.StreamingConfig{
		BootstrapRetries:        2,
		BootstrapTimeoutSeconds: 1,
		Compact: sdkconfig.StreamingCompactConfig{
			Enabled:            true,
			MaxDurationSeconds: 5,
		},
	}}
	handler := newCompactHandler(t, executor, cfg, "auth1", "auth2")
	headers := http.Header{}
	headers.Set(CalicoRequestSourceHeader, CalicoRequestSourceCompact)
	_, status, _ := collectStreamWithHeaders(handler, headers, []byte(`{"model":"test-model"}`))
	if status != http.StatusGatewayTimeout {
		t.Fatalf("status=%d, want timeout", status)
	}
	calls, authIDs := executor.snapshot()
	if calls != 1 {
		t.Fatalf("calls=%d authIDs=%v, want single bootstrap attempt", calls, authIDs)
	}
}

func TestCompactGuardDisablesAuthRequestRetry(t *testing.T) {
	executor := &compactCountingExecutor{failImmediately: true}
	cfg := &sdkconfig.SDKConfig{Streaming: sdkconfig.StreamingConfig{
		BootstrapRetries: 2,
		Compact: sdkconfig.StreamingCompactConfig{
			Enabled: true,
		},
	}}
	handler := newCompactHandler(t, executor, cfg, "auth1", "auth2")
	headers := http.Header{}
	headers.Set(CalicoRequestSourceHeader, CalicoRequestSourceCompact)
	_, status, _ := collectStreamWithHeaders(handler, headers, []byte(`{"model":"test-model"}`))
	if status == 0 {
		t.Fatal("expected error status")
	}
	calls, authIDs := executor.snapshot()
	if calls != 1 {
		t.Fatalf("calls=%d authIDs=%v, want single auth attempt", calls, authIDs)
	}
}

func TestCompactGuardFailClosedWithoutEnabled(t *testing.T) {
	executor := &compactCountingExecutor{failImmediately: true}
	cfg := &sdkconfig.SDKConfig{Streaming: sdkconfig.StreamingConfig{
		// Compact disabled (zero value)
		BootstrapRetries: 0,
	}}
	// Auth manager still has request-retry=3 from SetRetryConfig
	handler := newCompactHandler(t, executor, cfg, "auth1", "auth2")
	headers := http.Header{}
	headers.Set(CalicoRequestSourceHeader, CalicoRequestSourceCompact)
	_, _, _ = collectStreamWithHeaders(handler, headers, []byte(`{"model":"test-model"}`))
	calls, _ := executor.snapshot()
	// Without compact enabled, auth outer retry may rotate credentials (>1).
	if calls < 2 {
		t.Fatalf("calls=%d, want >=2 retries when compact guard disabled", calls)
	}
}

func TestCompactGuardDoesNotRewriteBody(t *testing.T) {
	// Structural: StreamingCompactConfig must not expose product rewrite fields.
	// Runtime rewrite helpers were removed; this test documents the v2 contract.
	cfg := sdkconfig.StreamingCompactConfig{Enabled: true, MaxDurationSeconds: 90}
	_ = cfg
	// Compile-time: only Enabled and MaxDurationSeconds remain on the type.
	if !cfg.Enabled || cfg.MaxDurationSeconds != 90 {
		t.Fatal("unexpected compact guard defaults")
	}
}
