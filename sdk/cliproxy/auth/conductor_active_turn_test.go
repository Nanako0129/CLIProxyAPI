package auth

import (
	"context"
	"testing"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

func TestManagerCodexActiveTurnBridgeReadyRequiresBoundedTopology(t *testing.T) {
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	cfg := &internalconfig.Config{}
	cfg.Codex.ActiveTurnBridge = true
	manager.SetConfig(cfg)

	if manager.CodexActiveTurnBridgeReady() {
		t.Fatal("bridge ready without a Codex credential")
	}
	if _, err := manager.Register(context.Background(), &Auth{
		ID:       "codex-a",
		Provider: "codex",
		Metadata: map[string]any{"disable_cooling": true},
	}); err != nil {
		t.Fatalf("register codex-a: %v", err)
	}
	if !manager.CodexActiveTurnBridgeReady() {
		t.Fatal("single non-cooling Codex credential should be ready")
	}

	if _, err := manager.Register(context.Background(), &Auth{
		ID:       "codex-b",
		Provider: "codex",
		Metadata: map[string]any{"disable_cooling": true},
	}); err != nil {
		t.Fatalf("register codex-b: %v", err)
	}
	if manager.CodexActiveTurnBridgeReady() {
		t.Fatal("multi-credential topology must fail closed")
	}
}

func TestManagerCodexActiveTurnBridgeReadyRequiresCoolingDisabled(t *testing.T) {
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	cfg := &internalconfig.Config{}
	cfg.Codex.ActiveTurnBridge = true
	manager.SetConfig(cfg)
	if _, err := manager.Register(context.Background(), &Auth{ID: "codex-a", Provider: "codex"}); err != nil {
		t.Fatalf("register codex-a: %v", err)
	}
	if manager.CodexActiveTurnBridgeReady() {
		t.Fatal("cooling-enabled credential must fail closed")
	}

	cfg.DisableCooling = true
	manager.SetConfig(cfg)
	if !manager.CodexActiveTurnBridgeReady() {
		t.Fatal("global disable-cooling should satisfy readiness")
	}

	cfg.Home.Enabled = true
	manager.SetConfig(cfg)
	if manager.CodexActiveTurnBridgeReady() {
		t.Fatal("Home topology must fail closed")
	}
}
