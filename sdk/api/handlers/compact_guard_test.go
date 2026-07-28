package handlers

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
	"golang.org/x/net/context"
)

func TestCompactGuardActiveFailClosed(t *testing.T) {
	headers := http.Header{}
	headers.Set(CalicoRequestSourceHeader, CalicoRequestSourceCompact)

	if CompactGuardActive(nil, headers) {
		t.Fatal("nil cfg must not activate")
	}
	if CompactGuardActive(&config.SDKConfig{}, headers) {
		t.Fatal("disabled compact must not activate")
	}
	if CompactGuardActive(&config.SDKConfig{Streaming: config.StreamingConfig{Compact: config.StreamingCompactConfig{Enabled: true}}}, nil) {
		t.Fatal("missing header must not activate")
	}
	cfg := &config.SDKConfig{Streaming: config.StreamingConfig{Compact: config.StreamingCompactConfig{Enabled: true}}}
	if !CompactGuardActive(cfg, headers) {
		t.Fatal("enabled + compact header must activate")
	}
}

func TestStreamingCompactMaxDurationBounds(t *testing.T) {
	if got := StreamingCompactMaxDuration(nil); got != 0 {
		t.Fatalf("nil cfg duration = %v", got)
	}
	cfg := &config.SDKConfig{Streaming: config.StreamingConfig{Compact: config.StreamingCompactConfig{
		Enabled:            true,
		MaxDurationSeconds: 90,
	}}}
	if got := StreamingCompactMaxDuration(cfg); got != 90*time.Second {
		t.Fatalf("duration = %v, want 90s", got)
	}
	cfg.Streaming.Compact.MaxDurationSeconds = 9999
	if got := StreamingCompactMaxDuration(cfg); got != 10*time.Minute {
		t.Fatalf("capped duration = %v, want 10m", got)
	}
}

func TestCompactAbsoluteTimeoutFires(t *testing.T) {
	ctx, cancel, state := WithCompactAbsoluteTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("absolute timeout did not fire")
	}
	err := CompactTimeoutErrorIfDeadline(state)
	if err == nil {
		t.Fatal("expected compact timeout error")
	}
	var timeoutErr *streamCompactTimeoutError
	if !errorsAsCompact(err, &timeoutErr) {
		t.Fatalf("err type = %T, want streamCompactTimeoutError", err)
	}
	if !strings.Contains(err.Error(), "compact request exceeded") {
		t.Fatalf("error message = %q", err.Error())
	}
}

func TestCompactTimeoutIgnoresParentDeadline(t *testing.T) {
	parent, parentCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer parentCancel()
	ctx, cancel, state := WithCompactAbsoluteTimeout(parent, 5*time.Second)
	defer cancel()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("parent deadline did not cancel child")
	}
	if err := CompactTimeoutErrorIfDeadline(state); err != nil {
		t.Fatalf("parent deadline must not report compact timeout: %v", err)
	}
}

func errorsAsCompact(err error, target **streamCompactTimeoutError) bool {
	if err == nil {
		return false
	}
	e, ok := err.(*streamCompactTimeoutError)
	if !ok {
		return false
	}
	*target = e
	return true
}
