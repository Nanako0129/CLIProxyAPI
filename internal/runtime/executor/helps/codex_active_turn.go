package helps

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// CodexActiveTurnKey isolates backend turn state by credential and the
// Claude-side session, agent, and prompt identities that produced it.
type CodexActiveTurnKey struct {
	CredentialID string
	SessionID    string
	AgentID      string
	PromptID     string
}

func (k CodexActiveTurnKey) valid() bool {
	return strings.TrimSpace(k.CredentialID) != "" &&
		strings.TrimSpace(k.SessionID) != "" &&
		strings.TrimSpace(k.PromptID) != ""
}

// CodexActiveTurn holds opaque backend state for one active Codex turn.
// The raw backend token must never be logged.
type CodexActiveTurn struct {
	turnID         string
	startedAt      int64
	promptCacheID  string
	installationID string

	mu         sync.RWMutex
	turnState  string
	lastUsed   time.Time
	activeRefs int
	terminated bool
	gate       chan struct{}
}

func newCodexActiveTurn(now time.Time, key CodexActiveTurnKey, promptCacheID string) *CodexActiveTurn {
	turn := &CodexActiveTurn{
		turnID:         uuid.NewString(),
		startedAt:      now.UnixMilli(),
		promptCacheID:  promptCacheID,
		installationID: uuid.NewSHA1(uuid.NameSpaceOID, []byte("cli-proxy-api:codex-installation:"+key.CredentialID)).String(),
		lastUsed:       now,
		gate:           make(chan struct{}, 1),
	}
	turn.gate <- struct{}{}
	return turn
}

// TurnID returns the stable client-generated identity for this turn.
func (t *CodexActiveTurn) TurnID() string { return t.turnID }

// StartedAtUnixMilli returns the stable start time for this turn.
func (t *CodexActiveTurn) StartedAtUnixMilli() int64 { return t.startedAt }

// PromptCacheID returns the stable session/thread/cache identity selected by
// the first request in this turn.
func (t *CodexActiveTurn) PromptCacheID() string { return t.promptCacheID }

// InstallationID returns a stable, credential-scoped installation identity.
func (t *CodexActiveTurn) InstallationID() string { return t.installationID }

// TurnState returns the first server-issued sticky-routing token captured for
// this turn. An empty value means the first upstream response has not supplied
// one yet.
func (t *CodexActiveTurn) TurnState() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.turnState
}

// TurnStateFingerprint returns a non-reversible, truncated correlation value
// suitable for debug logs. It never returns the opaque state itself.
func (t *CodexActiveTurn) TurnStateFingerprint() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.turnState == "" {
		return ""
	}
	digest := sha256.Sum256([]byte(t.turnState))
	return fmt.Sprintf("%x", digest[:6])
}

// CaptureTurnState stores the first non-empty server-issued state and ignores
// later values, matching native Codex's per-turn OnceLock behavior.
func (t *CodexActiveTurn) CaptureTurnState(value string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.turnState == "" && !t.terminated {
		t.turnState = value
	}
}

func (t *CodexActiveTurn) reserve(now time.Time) {
	t.mu.Lock()
	t.activeRefs++
	t.lastUsed = now
	t.mu.Unlock()
}

func (t *CodexActiveTurn) unreserve(now time.Time) {
	t.mu.Lock()
	if t.activeRefs > 0 {
		t.activeRefs--
	}
	t.lastUsed = now
	t.mu.Unlock()
}

func (t *CodexActiveTurn) acquire(ctx context.Context, now time.Time) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.gate:
		t.mu.Lock()
		if t.terminated {
			t.mu.Unlock()
			t.gate <- struct{}{}
			return fmt.Errorf("codex active turn is terminated")
		}
		t.lastUsed = now
		t.mu.Unlock()
		return nil
	}
}

func (t *CodexActiveTurn) release(now time.Time) {
	t.gate <- struct{}{}
	t.unreserve(now)
}

func (t *CodexActiveTurn) usage() (time.Time, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.lastUsed, t.activeRefs > 0
}

func (t *CodexActiveTurn) terminate() {
	t.mu.Lock()
	t.terminated = true
	t.turnState = ""
	t.mu.Unlock()
}

// CodexActiveTurnStore is an in-memory, bounded store for Claude-to-Codex
// active-turn compatibility. It serializes requests within one turn so the
// response token from an initial request is available to its continuation.
type CodexActiveTurnStore struct {
	mu      sync.Mutex
	turns   map[CodexActiveTurnKey]*CodexActiveTurn
	ttl     time.Duration
	maxSize int
	now     func() time.Time
}

// NewCodexActiveTurnStore creates a bounded store. ttl and maxSize must be
// positive; callers should choose limits that cover legitimate long-running
// turns without retaining correlation state indefinitely.
func NewCodexActiveTurnStore(ttl time.Duration, maxSize int) *CodexActiveTurnStore {
	return &CodexActiveTurnStore{
		turns:   make(map[CodexActiveTurnKey]*CodexActiveTurn),
		ttl:     ttl,
		maxSize: maxSize,
		now:     time.Now,
	}
}

// Begin returns the stable turn and an idempotent release function. Concurrent
// requests for the same key wait for the previous request or context cancel.
func (s *CodexActiveTurnStore) Begin(ctx context.Context, key CodexActiveTurnKey, promptCacheID string) (*CodexActiveTurn, func(), error) {
	if s == nil || s.ttl <= 0 || s.maxSize <= 0 {
		return nil, nil, fmt.Errorf("codex active turn store is not configured")
	}
	if !key.valid() {
		return nil, nil, fmt.Errorf("codex active turn key is incomplete")
	}
	promptCacheID = strings.TrimSpace(promptCacheID)
	if promptCacheID == "" {
		return nil, nil, fmt.Errorf("codex active turn prompt cache id is empty")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	now := s.now()
	s.mu.Lock()
	s.cleanupLocked(now)
	turn := s.turns[key]
	if turn == nil {
		if len(s.turns) >= s.maxSize {
			s.evictOldestIdleLocked()
		}
		if len(s.turns) >= s.maxSize {
			s.mu.Unlock()
			return nil, nil, fmt.Errorf("codex active turn store capacity reached")
		}
		turn = newCodexActiveTurn(now, key, promptCacheID)
		s.turns[key] = turn
	}
	turn.reserve(now)
	s.mu.Unlock()

	if err := turn.acquire(ctx, s.now()); err != nil {
		turn.unreserve(s.now())
		return nil, nil, fmt.Errorf("acquire codex active turn: %w", err)
	}
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() { turn.release(s.now()) })
	}
	return turn, release, nil
}

// Terminate deletes a hard-failed turn and prevents already-waiting duplicate
// requests from replaying its backend state.
func (s *CodexActiveTurnStore) Terminate(key CodexActiveTurnKey, turn *CodexActiveTurn) {
	if s == nil || turn == nil {
		return
	}
	s.mu.Lock()
	if current := s.turns[key]; current == turn {
		delete(s.turns, key)
	}
	s.mu.Unlock()
	turn.terminate()
}

func (s *CodexActiveTurnStore) cleanupLocked(now time.Time) {
	for key, turn := range s.turns {
		lastUsed, inFlight := turn.usage()
		if !inFlight && now.Sub(lastUsed) >= s.ttl {
			delete(s.turns, key)
		}
	}
}

func (s *CodexActiveTurnStore) evictOldestIdleLocked() {
	var oldestKey CodexActiveTurnKey
	var oldestTime time.Time
	found := false
	for key, turn := range s.turns {
		lastUsed, inFlight := turn.usage()
		if inFlight || found && !lastUsed.Before(oldestTime) {
			continue
		}
		oldestKey = key
		oldestTime = lastUsed
		found = true
	}
	if found {
		delete(s.turns, oldestKey)
	}
}
