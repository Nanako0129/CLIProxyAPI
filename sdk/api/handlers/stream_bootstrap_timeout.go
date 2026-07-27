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

// StreamingIdleTimeout returns the configured maximum gap between upstream
// stream payloads. Zero disables the timeout.
func StreamingIdleTimeout(cfg *config.SDKConfig) time.Duration {
	if cfg == nil || cfg.Streaming.IdleTimeoutSeconds <= 0 {
		return 0
	}
	seconds := cfg.Streaming.IdleTimeoutSeconds
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

type streamIdleTimeoutError struct {
	timeout time.Duration
}

func (e *streamIdleTimeoutError) Error() string {
	return fmt.Sprintf("upstream stream produced no payload for %s", e.timeout)
}

func (e *streamIdleTimeoutError) Unwrap() error   { return context.DeadlineExceeded }
func (e *streamIdleTimeoutError) StatusCode() int { return http.StatusGatewayTimeout }

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
	idleTimeout := StreamingIdleTimeout(h.Cfg)
	if bootstrapTimeout <= 0 && idleTimeout <= 0 {
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
	return releaseStreamBootstrapAttempt(result, attempt.ctx, attempt.cancel, idleTimeout), nil
}

func receiveStreamChunkWithIdleTimeout(ctx context.Context, chunks <-chan coreexecutor.StreamChunk, timeout time.Duration) (coreexecutor.StreamChunk, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return coreexecutor.StreamChunk{}, false, err
	}
	if timeout <= 0 {
		select {
		case <-ctx.Done():
			return coreexecutor.StreamChunk{}, false, ctx.Err()
		case chunk, ok := <-chunks:
			return chunk, ok, nil
		}
	}

	timer := time.NewTimer(timeout)
	defer func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()

	select {
	case <-ctx.Done():
		return coreexecutor.StreamChunk{}, false, ctx.Err()
	case chunk, ok := <-chunks:
		return chunk, ok, nil
	case <-timer.C:
		// Linearize cancellation at the timer wake, then prefer an upstream
		// payload or clean close that is already observable at that instant.
		if err := ctx.Err(); err != nil {
			return coreexecutor.StreamChunk{}, false, err
		}
		select {
		case chunk, ok := <-chunks:
			return chunk, ok, nil
		default:
			return coreexecutor.StreamChunk{}, false, &streamIdleTimeoutError{timeout: timeout}
		}
	}
}

func releaseStreamBootstrapAttempt(result *coreexecutor.StreamResult, ctx context.Context, cancel context.CancelFunc, idleTimeout time.Duration) *coreexecutor.StreamResult {
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
			chunk, ok, err := receiveStreamChunkWithIdleTimeout(ctx, remaining, idleTimeout)
			if err != nil {
				var timeoutErr *streamIdleTimeoutError
				if !errors.As(err, &timeoutErr) {
					return
				}
				// Cancellation observed before the unbuffered send wins. Once the
				// send completes, the timeout is the single terminal result.
				if ctx.Err() != nil {
					return
				}
				select {
				case <-ctx.Done():
					return
				case out <- coreexecutor.StreamChunk{Err: timeoutErr}:
				}
				return
			}
			if !ok {
				return
			}
			// The idle timer is no longer armed while downstream delivery is
			// blocked, so client backpressure cannot look like upstream idleness.
			if ctx.Err() != nil {
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
