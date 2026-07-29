package helps

import (
	"context"
	"runtime"
	"testing"
	"time"
)

func activeTurnTestKey(prompt string) CodexActiveTurnKey {
	return CodexActiveTurnKey{
		CredentialID: "credential-a",
		SessionID:    "session-a",
		AgentID:      "agent-a",
		PromptID:     prompt,
	}
}

func TestCodexActiveTurnStoreReusesIdentityAndFirstServerState(t *testing.T) {
	store := NewCodexActiveTurnStore(time.Hour, 8)
	first, releaseFirst, err := store.Begin(context.Background(), activeTurnTestKey("prompt-a"), "cache-a")
	if err != nil {
		t.Fatalf("Begin first: %v", err)
	}
	first.CaptureTurnState("state-a")
	first.CaptureTurnState("state-b")
	releaseFirst()
	releaseFirst()

	second, releaseSecond, err := store.Begin(context.Background(), activeTurnTestKey("prompt-a"), "cache-b")
	if err != nil {
		t.Fatalf("Begin second: %v", err)
	}
	defer releaseSecond()
	if second != first {
		t.Fatal("same key returned a different turn")
	}
	if second.TurnState() != "state-a" {
		t.Fatalf("TurnState = %q, want state-a", second.TurnState())
	}
	if fingerprint := second.TurnStateFingerprint(); fingerprint == "" || fingerprint == "state-a" {
		t.Fatalf("TurnStateFingerprint = %q", fingerprint)
	}
	if second.TurnID() == "" || second.StartedAtUnixMilli() == 0 {
		t.Fatal("stable turn identity is incomplete")
	}
	if second.PromptCacheID() != "cache-a" || second.InstallationID() == "" {
		t.Fatal("stable native identity is incomplete")
	}
}

func TestCodexActiveTurnStoreSeparatesPromptsAndCredentials(t *testing.T) {
	store := NewCodexActiveTurnStore(time.Hour, 8)
	first, releaseFirst, err := store.Begin(context.Background(), activeTurnTestKey("prompt-a"), "cache-a")
	if err != nil {
		t.Fatalf("Begin first: %v", err)
	}
	releaseFirst()

	second, releaseSecond, err := store.Begin(context.Background(), activeTurnTestKey("prompt-b"), "cache-b")
	if err != nil {
		t.Fatalf("Begin second: %v", err)
	}
	releaseSecond()
	if first.TurnID() == second.TurnID() {
		t.Fatal("different prompts shared a turn id")
	}

	credentialKey := activeTurnTestKey("prompt-a")
	credentialKey.CredentialID = "credential-b"
	third, releaseThird, err := store.Begin(context.Background(), credentialKey, "cache-c")
	if err != nil {
		t.Fatalf("Begin third: %v", err)
	}
	defer releaseThird()
	if first.TurnID() == third.TurnID() {
		t.Fatal("different credentials shared a turn id")
	}
}

func TestCodexActiveTurnStoreSerializesSameTurnWithCancellation(t *testing.T) {
	store := NewCodexActiveTurnStore(time.Hour, 8)
	_, release, err := store.Begin(context.Background(), activeTurnTestKey("prompt-a"), "cache-a")
	if err != nil {
		t.Fatalf("Begin first: %v", err)
	}
	defer release()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, _, err = store.Begin(ctx, activeTurnTestKey("prompt-a"), "cache-a"); err == nil {
		t.Fatal("concurrent Begin unexpectedly succeeded")
	}
}

func TestCodexActiveTurnStoreExpiresIdleTurns(t *testing.T) {
	store := NewCodexActiveTurnStore(time.Minute, 1)
	now := time.Unix(100, 0)
	store.now = func() time.Time { return now }
	first, releaseFirst, err := store.Begin(context.Background(), activeTurnTestKey("prompt-a"), "cache-a")
	if err != nil {
		t.Fatalf("Begin first: %v", err)
	}
	releaseFirst()

	now = now.Add(2 * time.Minute)
	second, releaseSecond, err := store.Begin(context.Background(), activeTurnTestKey("prompt-b"), "cache-b")
	if err != nil {
		t.Fatalf("Begin second: %v", err)
	}
	defer releaseSecond()
	if first.TurnID() == second.TurnID() {
		t.Fatal("expired turn identity was reused")
	}
}

func TestCodexActiveTurnStoreTerminateDeletesStateAndRejectsWaiters(t *testing.T) {
	store := NewCodexActiveTurnStore(time.Hour, 8)
	key := activeTurnTestKey("prompt-a")
	turn, release, err := store.Begin(context.Background(), key, "cache-a")
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	turn.CaptureTurnState("state-a")

	wake := make(chan error, 1)
	go func() {
		_, _, errWait := store.Begin(context.Background(), key, "cache-a")
		wake <- errWait
	}()
	deadline := time.Now().Add(time.Second)
	for {
		turn.mu.RLock()
		refs := turn.activeRefs
		turn.mu.RUnlock()
		if refs == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("waiter did not reserve the turn")
		}
		runtime.Gosched()
	}
	store.Terminate(key, turn)
	release()
	if errWait := <-wake; errWait == nil {
		t.Fatal("terminated waiter unexpectedly acquired the old turn")
	}
	if turn.TurnState() != "" {
		t.Fatal("terminated turn retained backend state")
	}

	next, releaseNext, err := store.Begin(context.Background(), key, "cache-b")
	if err != nil {
		t.Fatalf("Begin next: %v", err)
	}
	defer releaseNext()
	if next == turn || next.TurnID() == turn.TurnID() {
		t.Fatal("hard-terminal turn identity was reused")
	}
}

func TestCodexActiveTurnStoreDoesNotEvictReservedWaiter(t *testing.T) {
	store := NewCodexActiveTurnStore(time.Hour, 2)
	keyA := activeTurnTestKey("prompt-a")
	turnA, releaseA, err := store.Begin(context.Background(), keyA, "cache-a")
	if err != nil {
		t.Fatalf("Begin A: %v", err)
	}

	wake := make(chan *CodexActiveTurn, 1)
	go func() {
		turn, release, errWait := store.Begin(context.Background(), keyA, "cache-a")
		if errWait != nil {
			wake <- nil
			return
		}
		release()
		wake <- turn
	}()
	deadline := time.Now().Add(time.Second)
	for {
		turnA.mu.RLock()
		refs := turnA.activeRefs
		turnA.mu.RUnlock()
		if refs == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("waiter did not reserve turn A")
		}
		runtime.Gosched()
	}

	keyB := activeTurnTestKey("prompt-b")
	_, releaseB, err := store.Begin(context.Background(), keyB, "cache-b")
	if err != nil {
		t.Fatalf("Begin B: %v", err)
	}
	releaseB()
	keyC := activeTurnTestKey("prompt-c")
	_, releaseC, err := store.Begin(context.Background(), keyC, "cache-c")
	if err != nil {
		t.Fatalf("Begin C: %v", err)
	}
	releaseC()

	releaseA()
	if got := <-wake; got != turnA {
		t.Fatal("reserved waiter lost turn A during capacity eviction")
	}
}
