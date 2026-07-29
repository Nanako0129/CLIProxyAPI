package handlers

import (
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	coreexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
	"golang.org/x/net/context"
)

const maxStreamingBootstrapTimeout = 10 * time.Minute

// StreamingBootstrapTimeout returns the configured maximum wait for the first
// upstream stream payload. Zero disables the timeout.
func StreamingBootstrapTimeout(cfg *config.SDKConfig) time.Duration {
	if cfg == nil || cfg.Streaming.BootstrapTimeoutSeconds <= 0 {
		return 0
	}
	seconds := cfg.Streaming.BootstrapTimeoutSeconds
	if seconds > int(maxStreamingBootstrapTimeout/time.Second) {
		return maxStreamingBootstrapTimeout
	}
	return time.Duration(seconds) * time.Second
}

type streamBootstrapTimeoutError struct {
	timeout time.Duration
}

func (e *streamBootstrapTimeoutError) Error() string {
	return fmt.Sprintf("upstream stream produced no payload within %s", e.timeout)
}

func (e *streamBootstrapTimeoutError) Unwrap() error   { return context.DeadlineExceeded }
func (e *streamBootstrapTimeoutError) StatusCode() int { return http.StatusGatewayTimeout }

// streamBootstrapAttempt uses cancellation rather than a context deadline so
// the bootstrap timer can be removed after the first payload without imposing
// a deadline on the remainder of a healthy stream.
type streamBootstrapAttempt struct {
	ctx       context.Context
	cancel    context.CancelFunc
	timer     *time.Timer
	timerDone chan struct{}
	timedOut  atomic.Bool
}

func newStreamBootstrapAttempt(parent context.Context, timeout time.Duration) *streamBootstrapAttempt {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	attempt := &streamBootstrapAttempt{
		ctx:       ctx,
		cancel:    cancel,
		timerDone: make(chan struct{}),
	}
	if timeout <= 0 {
		return attempt
	}
	attempt.timer = time.AfterFunc(timeout, func() {
		attempt.timedOut.Store(true)
		attempt.cancel()
		close(attempt.timerDone)
	})
	return attempt
}

// disarm stops the bootstrap timer and reports whether it already fired.
func (a *streamBootstrapAttempt) disarm() bool {
	if a == nil || a.timer == nil {
		return false
	}
	if !a.timer.Stop() {
		<-a.timerDone
	}
	return a.timedOut.Load()
}

func (h *BaseAPIHandler) executeStreamBootstrapAttempt(ctx context.Context, providers []string, req coreexecutor.Request, opts coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	bootstrapTimeout := StreamingBootstrapTimeout(h.Cfg)
	if bootstrapTimeout <= 0 {
		return h.AuthManager.ExecuteStream(ctx, providers, req, opts)
	}

	attempt := newStreamBootstrapAttempt(ctx, bootstrapTimeout)
	result, err := h.AuthManager.ExecuteStream(attempt.ctx, providers, req, opts)
	if attempt.disarm() {
		attempt.cancel()
		return nil, &streamBootstrapTimeoutError{timeout: bootstrapTimeout}
	}
	if err != nil {
		attempt.cancel()
		return nil, err
	}
	return releaseStreamBootstrapAttempt(result, attempt.ctx, attempt.cancel), nil
}

func releaseStreamBootstrapAttempt(result *coreexecutor.StreamResult, ctx context.Context, cancel context.CancelFunc) *coreexecutor.StreamResult {
	if result == nil || result.Chunks == nil {
		cancel()
		return result
	}
	remaining := result.Chunks
	out := make(chan coreexecutor.StreamChunk)
	result.Chunks = out
	go func() {
		defer close(out)
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				return
			case chunk, ok := <-remaining:
				if !ok {
					return
				}
				select {
				case <-ctx.Done():
					return
				case out <- chunk:
				}
				if chunk.Err != nil {
					return
				}
			}
		}
	}()
	return result
}

func (h *BaseAPIHandler) executeInitialStreamWithBootstrapTimeout(ctx context.Context, providers []string, req coreexecutor.Request, opts coreexecutor.Options, maxRetries int) (*coreexecutor.StreamResult, int, error) {
	retriesUsed := 0
	for {
		result, err := h.executeStreamBootstrapAttempt(ctx, providers, req, opts)
		if err == nil {
			return result, retriesUsed, nil
		}
		var timeoutErr *streamBootstrapTimeoutError
		if !errors.As(err, &timeoutErr) || retriesUsed >= maxRetries {
			return nil, retriesUsed, err
		}
		retriesUsed++
	}
}
