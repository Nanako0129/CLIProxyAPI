package handlers

import (
	"fmt"
	"net/http"
	"strings"
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

// WithCompactAbsoluteTimeout wraps ctx with an absolute deadline when duration > 0.
// The timer does not reset on payload activity.
func WithCompactAbsoluteTimeout(parent context.Context, duration time.Duration) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	if duration <= 0 {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, duration)
}

// CompactTimeoutErrorIfDeadline returns a streamCompactTimeoutError when ctx ended
// because of the compact absolute deadline (DeadlineExceeded only).
func CompactTimeoutErrorIfDeadline(ctx context.Context, duration time.Duration) error {
	if duration <= 0 || ctx == nil {
		return nil
	}
	if ctx.Err() == context.DeadlineExceeded {
		return &streamCompactTimeoutError{timeout: duration}
	}
	return nil
}

// releaseCompactAbsoluteTimeout keeps the absolute deadline armed while chunks
// are consumed and surfaces a gateway-timeout error when the deadline fires.
func releaseCompactAbsoluteTimeout(result *coreexecutor.StreamResult, cancel context.CancelFunc, ctx context.Context, duration time.Duration) *coreexecutor.StreamResult {
	if result == nil || result.Chunks == nil {
		if cancel != nil {
			cancel()
		}
		return result
	}
	remaining := result.Chunks
	out := make(chan coreexecutor.StreamChunk)
	result.Chunks = out
	go func() {
		defer close(out)
		if cancel != nil {
			defer cancel()
		}
		for {
			select {
			case <-ctx.Done():
				if err := CompactTimeoutErrorIfDeadline(ctx, duration); err != nil {
					select {
					case out <- coreexecutor.StreamChunk{Err: err}:
					default:
					}
				}
				return
			case chunk, ok := <-remaining:
				if !ok {
					return
				}
				select {
				case <-ctx.Done():
					if err := CompactTimeoutErrorIfDeadline(ctx, duration); err != nil {
						select {
						case out <- coreexecutor.StreamChunk{Err: err}:
						default:
						}
					}
					return
				case out <- chunk:
				}
			}
		}
	}()
	return result
}
