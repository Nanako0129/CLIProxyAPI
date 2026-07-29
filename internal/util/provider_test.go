package util

import "testing"

func TestMaskSensitiveHeaderValueMasksCodexTurnState(t *testing.T) {
	raw := "opaque-backend-turn-state"
	masked := MaskSensitiveHeaderValue("X-Codex-Turn-State", raw)
	if masked != "[REDACTED]" {
		t.Fatalf("masked turn state = %q, want [REDACTED]", masked)
	}
}
