package auth

import internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"

// CodexActiveTurnBridgeReady reports whether this process is in the bounded
// topology supported by the Claude-to-Codex active-turn bridge. Version 1 is
// deliberately limited to one local Codex credential with quota cooling
// disabled; multi-credential failover and Home routing cannot preserve a
// server-issued turn token on the same account.
func (m *Manager) CodexActiveTurnBridgeReady() bool {
	if m == nil {
		return false
	}
	cfg, _ := m.runtimeConfig.Load().(*internalconfig.Config)
	if cfg == nil || !cfg.Codex.ActiveTurnBridge || cfg.Home.Enabled {
		return false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	var codexAuth *Auth
	for _, auth := range m.auths {
		if auth == nil || auth.Disabled || auth.Status == StatusDisabled || executorKeyFromAuth(auth) != "codex" {
			continue
		}
		if codexAuth != nil {
			return false
		}
		codexAuth = auth
	}
	return codexAuth != nil && quotaCooldownDisabledForAuthWithConfig(codexAuth, cfg)
}
