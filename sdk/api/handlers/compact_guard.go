package handlers

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	coreexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
	"golang.org/x/net/context"
)

// CalicoRequestSourceHeader is emitted by calico when Claude query source is "compact".
const CalicoRequestSourceHeader = "X-Calico-Request-Source"

// CalicoRequestSourceCompact is the only header value that arms compact guards.
const CalicoRequestSourceCompact = "compact"

const maxStreamingCompactDuration = 10 * time.Minute

// streamCompactTimeoutError is returned when a compact absolute wall-clock fires.
type streamCompactTimeoutError struct {
	timeout time.Duration
}

func (e *streamCompactTimeoutError) Error() string {
	return fmt.Sprintf("compact request exceeded absolute duration %s", e.timeout)
}

func (e *streamCompactTimeoutError) Unwrap() error   { return context.DeadlineExceeded }
func (e *streamCompactTimeoutError) StatusCode() int { return http.StatusGatewayTimeout }

// CompactGuardActive reports whether compact class guards apply (timeout + no-retry).
// Fail-closed: requires both config enabled and the compact request header.
// Product policy (model/effort/thinking) is never rewritten here.
func CompactGuardActive(cfg *config.SDKConfig, headers http.Header) bool {
	if cfg == nil || !cfg.Streaming.Compact.Enabled {
		return false
	}
	return RequestSourceIsCompact(headers)
}

// RequestSourceIsCompact reports the calico compact marker only.
func RequestSourceIsCompact(headers http.Header) bool {
	if headers == nil {
		return false
	}
	value := strings.TrimSpace(headers.Get(CalicoRequestSourceHeader))
	return strings.EqualFold(value, CalicoRequestSourceCompact)
}

// StreamingCompactMaxDuration returns the absolute compact wall-clock when armed.
// Zero means do not arm a timer (guard may still disable retries).
func StreamingCompactMaxDuration(cfg *config.SDKConfig) time.Duration {
	if cfg == nil || !cfg.Streaming.Compact.Enabled {
		return 0
	}
	seconds := cfg.Streaming.Compact.MaxDurationSeconds
	if seconds <= 0 {
		return 0
	}
	if seconds > int(maxStreamingCompactDuration/time.Second) {
		return maxStreamingCompactDuration
	}
	return time.Duration(seconds) * time.Second
}

// compactTimeoutState tracks whether the compact absolute timer itself fired,
// distinct from parent-context deadlines.
type compactTimeoutState struct {
	duration time.Duration
	fired    chan struct{}
	once     sync.Once
}

func (s *compactTimeoutState) markFired() {
	if s == nil {
		return
	}
	s.once.Do(func() { close(s.fired) })
}

func (s *compactTimeoutState) errorIfFired() error {
	if s == nil || s.duration <= 0 {
		return nil
	}
	select {
	case <-s.fired:
		return &streamCompactTimeoutError{timeout: s.duration}
	default:
		return nil
	}
}

// WithCompactAbsoluteTimeout cancels ctx after duration using an independent timer.
// Parent deadlines cancel the context without being reported as compact timeouts.
func WithCompactAbsoluteTimeout(parent context.Context, duration time.Duration) (context.Context, context.CancelFunc, *compactTimeoutState) {
	if parent == nil {
		parent = context.Background()
	}
	if duration <= 0 {
		ctx, cancel := context.WithCancel(parent)
		return ctx, cancel, nil
	}
	state := &compactTimeoutState{
		duration: duration,
		fired:    make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(parent)
	timer := time.AfterFunc(duration, func() {
		state.markFired()
		cancel()
	})
	stop := func() {
		timer.Stop()
		cancel()
	}
	return ctx, stop, state
}

// CompactTimeoutErrorIfDeadline returns a streamCompactTimeoutError only when
// the compact absolute timer fired (not when a parent deadline expires).
func CompactTimeoutErrorIfDeadline(state *compactTimeoutState) error {
	if state == nil {
		return nil
	}
	return state.errorIfFired()
}

// releaseCompactAbsoluteTimeout keeps the absolute deadline armed while chunks
// are consumed and surfaces a gateway-timeout error when the compact timer fires.
func releaseCompactAbsoluteTimeout(result *coreexecutor.StreamResult, cancel context.CancelFunc, ctx context.Context, state *compactTimeoutState) *coreexecutor.StreamResult {
	if result == nil || result.Chunks == nil {
		if cancel != nil {
			cancel()
		}
		return result
	}
	remaining := result.Chunks
	// Buffer one chunk so a terminal compact timeout error is not dropped when
	// the consumer is briefly busy forwarding a previous payload.
	out := make(chan coreexecutor.StreamChunk, 1)
	result.Chunks = out
	go func() {
		defer close(out)
		if cancel != nil {
			defer cancel()
		}
		sendTimeout := func() {
			if err := CompactTimeoutErrorIfDeadline(state); err != nil {
				// Blocking send: consumer must observe the terminal error.
				// If they already abandoned the stream, this returns when out closes
				// is not possible before close; use select with ctx only for parent cancel
				// without compact fire — still try once.
				select {
				case out <- coreexecutor.StreamChunk{Err: err}:
				case <-time.After(5 * time.Second):
					// Best-effort: avoid leaking this goroutine forever if nobody reads.
				}
			}
		}
		for {
			select {
			case <-ctx.Done():
				sendTimeout()
				return
			case chunk, ok := <-remaining:
				if !ok {
					return
				}
				select {
				case <-ctx.Done():
					sendTimeout()
					return
				case out <- chunk:
				}
			}
		}
	}()
	return result
}

// armCompactStreamGuard prepares compact no-retry metadata and optional absolute
// wall-clock for a stream attempt. Returns the context to use for execution.
func armCompactStreamGuard(cfg *config.SDKConfig, headers http.Header, parent context.Context, meta map[string]any) (context.Context, context.CancelFunc, *compactTimeoutState, map[string]any) {
	if !CompactGuardActive(cfg, headers) {
		return parent, nil, nil, meta
	}
	if meta == nil {
		meta = make(map[string]any)
	}
	meta[coreexecutor.DisableStreamRetriesMetadataKey] = true
	duration := StreamingCompactMaxDuration(cfg)
	if duration <= 0 {
		return parent, nil, nil, meta
	}
	ctx, cancel, state := WithCompactAbsoluteTimeout(parent, duration)
	return ctx, cancel, state, meta
}
