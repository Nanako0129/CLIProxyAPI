package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const (
	calicoPromptIDHeader      = "X-Calico-Prompt-Id"
	calicoActiveTurnHeader    = "X-Calico-Active-Turn-Version"
	calicoActiveTurnVersion   = "1"
	codexTurnStateHeader      = "X-Codex-Turn-State"
	codexActiveTurnTTL        = 24 * time.Hour
	codexActiveTurnMaxEntries = 4096
)

type codexClaudeActiveTurn struct {
	turn        *helps.CodexActiveTurn
	store       *helps.CodexActiveTurnStore
	key         helps.CodexActiveTurnKey
	promptCache string
}

func (e *CodexExecutor) codexActiveTurnStore() *helps.CodexActiveTurnStore {
	e.activeTurnsOnce.Do(func() {
		e.activeTurns = helps.NewCodexActiveTurnStore(codexActiveTurnTTL, codexActiveTurnMaxEntries)
	})
	return e.activeTurns
}

func requestHeader(ctx context.Context, headers http.Header, name string) string {
	if headers != nil {
		if value := strings.TrimSpace(headers.Get(name)); value != "" {
			return value
		}
	}
	if ctx != nil {
		if ginCtx, ok := ctx.Value("gin").(*gin.Context); ok && ginCtx != nil && ginCtx.Request != nil {
			return strings.TrimSpace(ginCtx.Request.Header.Get(name))
		}
	}
	return ""
}

func (e *CodexExecutor) prepareClaudeActiveTurn(ctx context.Context, from sdktranslator.Format, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options, body []byte) ([]byte, *codexClaudeActiveTurn, func(), error) {
	if !sourceFormatEqual(from, sdktranslator.FormatClaude) || auth == nil || strings.TrimSpace(auth.ID) == "" {
		return body, nil, nil, nil
	}
	ready, _ := opts.Metadata[cliproxyexecutor.CodexActiveTurnBridgeMetadataKey].(bool)
	if !ready {
		return body, nil, nil, nil
	}
	if requestHeader(ctx, opts.Headers, calicoActiveTurnHeader) != calicoActiveTurnVersion {
		return body, nil, nil, nil
	}
	promptID := requestHeader(ctx, opts.Headers, calicoPromptIDHeader)
	if _, errParse := uuid.Parse(promptID); errParse != nil {
		return body, nil, nil, nil
	}
	sessionID := helps.ExtractClaudeCodeSessionID(ctx, req.Payload, opts.Headers)
	if sessionID == "" {
		return body, nil, nil, nil
	}
	agentID := requestHeader(ctx, opts.Headers, "X-Claude-Code-Agent-Id")
	if agentID == "" {
		agentID = "main"
	}

	cache, ok, errCache := helps.ClaudeCodePromptCache(ctx, req.Model, req.Payload, opts.Headers)
	if errCache != nil {
		return body, nil, nil, errCache
	}
	if !ok || strings.TrimSpace(cache.ID) == "" {
		return body, nil, nil, nil
	}

	key := helps.CodexActiveTurnKey{
		CredentialID: auth.ID,
		SessionID:    sessionID,
		AgentID:      agentID,
		PromptID:     promptID,
	}
	store := e.codexActiveTurnStore()
	turn, release, errBegin := store.Begin(ctx, key, cache.ID)
	if errBegin != nil {
		return body, nil, nil, errBegin
	}
	promptCacheID := turn.PromptCacheID()

	turnMetadata, errJSON := json.Marshal(map[string]any{
		"installation_id":         turn.InstallationID(),
		"session_id":              promptCacheID,
		"thread_id":               promptCacheID,
		"turn_id":                 turn.TurnID(),
		"window_id":               promptCacheID + ":0",
		"prompt_cache_key":        promptCacheID,
		"request_kind":            "turn",
		"turn_started_at_unix_ms": turn.StartedAtUnixMilli(),
	})
	if errJSON != nil {
		release()
		return body, nil, nil, fmt.Errorf("marshal codex active turn metadata: %w", errJSON)
	}
	body, _ = sjson.SetBytes(body, "client_metadata.x-codex-turn-metadata", string(turnMetadata))
	body, _ = sjson.SetBytes(body, "client_metadata.x-codex-installation-id", turn.InstallationID())
	body, _ = sjson.SetBytes(body, "client_metadata.session_id", promptCacheID)
	body, _ = sjson.SetBytes(body, "client_metadata.thread_id", promptCacheID)
	body, _ = sjson.SetBytes(body, "client_metadata.x-codex-window-id", promptCacheID+":0")
	body, _ = sjson.SetBytes(body, "client_metadata.turn_id", turn.TurnID())
	body, _ = sjson.SetBytes(body, "prompt_cache_key", promptCacheID)

	return body, &codexClaudeActiveTurn{turn: turn, store: store, key: key, promptCache: promptCacheID}, release, nil
}

func applyClaudeActiveTurnHeaders(req *http.Request, upstreamBody []byte, active *codexClaudeActiveTurn) {
	if req == nil || active == nil || active.turn == nil {
		return
	}
	turnMetadata := strings.TrimSpace(gjson.GetBytes(upstreamBody, "client_metadata.x-codex-turn-metadata").String())
	if turnMetadata != "" {
		req.Header.Set("X-Codex-Turn-Metadata", turnMetadata)
	}
	promptCache := strings.TrimSpace(gjson.GetBytes(upstreamBody, "prompt_cache_key").String())
	if promptCache == "" {
		promptCache = active.promptCache
	}
	req.Header.Set("Session_id", promptCache)
	req.Header.Set("Session-Id", promptCache)
	req.Header.Set("Thread-Id", promptCache)
	req.Header.Set("X-Client-Request-Id", promptCache)
	req.Header.Set("X-Codex-Window-Id", promptCache+":0")
	if turnState := active.turn.TurnState(); turnState != "" {
		req.Header.Set(codexTurnStateHeader, turnState)
		log.WithFields(log.Fields{
			"turn_id":      active.turn.TurnID(),
			"state_sha256": active.turn.TurnStateFingerprint(),
		}).Debug("codex active turn: replay state")
	}
}

func terminateClaudeActiveTurn(active *codexClaudeActiveTurn) {
	if active == nil || active.store == nil || active.turn == nil {
		return
	}
	active.store.Terminate(active.key, active.turn)
}

func captureClaudeActiveTurnState(headers http.Header, active *codexClaudeActiveTurn) {
	if active == nil || active.turn == nil {
		return
	}
	before := active.turn.TurnState()
	active.turn.CaptureTurnState(headers.Get(codexTurnStateHeader))
	if before == "" && active.turn.TurnState() != "" {
		log.WithFields(log.Fields{
			"turn_id":      active.turn.TurnID(),
			"state_sha256": active.turn.TurnStateFingerprint(),
		}).Debug("codex active turn: captured state")
	}
}
