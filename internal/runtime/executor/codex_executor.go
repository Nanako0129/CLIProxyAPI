package executor

import (
	"sync"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
)

// CodexExecutor handles Codex requests and retains bounded active-turn state
// for versioned Calico Claude bridge requests.
// If api_key is unavailable on auth, it falls back to legacy via ClientAdapter.
type CodexExecutor struct {
	cfg             *config.Config
	activeTurnsOnce sync.Once
	activeTurns     *helps.CodexActiveTurnStore
}

func NewCodexExecutor(cfg *config.Config) *CodexExecutor { return &CodexExecutor{cfg: cfg} }

func (e *CodexExecutor) Identifier() string { return "codex" }
