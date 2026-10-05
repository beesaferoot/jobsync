package storagetest

import (
	"context"
	"testing"
	"time"

	"github.com/beesaferoot/jobsync"
)

// runLocker exercises jobsync.Locker. A driver without it still runs one-off
// jobs; it just cannot run recurring ones, because electing one server to fire a
// schedule is exactly what the lock is for.
func runLocker(t *testing.T, newStore New) {
	t.Run("AcquireAndRelease", func(t *testing.T) { testAcquireAndRelease(t, newStore) })
	t.Run("HeldLockIsNotGranted", func(t *testing.T) { testHeldLockIsNotGranted(t, newStore) })
	t.Run("WrongTokenDoesNotRelease", func(t *testing.T) { testWrongTokenDoesNotRelease(t, newStore) })
	t.Run("ExpiredLockIsGranted", func(t *testing.T) { testExpiredLockIsGranted(t, newStore) })
	t.Run("StaleUnlockSpareNewHolder", func(t *testing.T) { testStaleUnlockSparesNewHolder(t, newStore) })
}

func testAcquireAndRelease(t *testing.T, newStore New) {
	ctx := context.Background()
	l := locker(t, newStore)

	token, err := l.Lock(ctx, "nightly", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if token == "" {
		t.Fatal("Lock on a free key returned no token")
	}
	if err := l.Unlock(ctx, "nightly", token); err != nil {
		t.Fatal(err)
	}

	again, err := l.Lock(ctx, "nightly", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if again == "" {
		t.Fatal("Lock after Unlock returned no token")
	}
}

func testHeldLockIsNotGranted(t *testing.T, newStore New) {
	ctx := context.Background()
	l := locker(t, newStore)

	if _, err := l.Lock(ctx, "nightly", time.Minute); err != nil {
		t.Fatal(err)
	}

	// A held lock is the ordinary case, not a failure: every server but one sees
	// this on every tick. Returning an error would make normal operation noisy.
	token, err := l.Lock(ctx, "nightly", time.Minute)
	if err != nil {
		t.Fatalf("Lock on a held key must not error: %v", err)
	}
	if token != "" {
		t.Fatal("two servers hold the same lock at once")
	}
}

func testWrongTokenDoesNotRelease(t *testing.T, newStore New) {
	ctx := context.Background()
	l := locker(t, newStore)

	if _, err := l.Lock(ctx, "nightly", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := l.Unlock(ctx, "nightly", "not-the-token"); err != nil {
		t.Fatalf("Unlock with a wrong token must be a no-op, not an error: %v", err)
	}

	token, err := l.Lock(ctx, "nightly", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		t.Fatal("a wrong token released the lock")
	}
}

func testExpiredLockIsGranted(t *testing.T, newStore New) {
	ctx := context.Background()
	l := locker(t, newStore)

	if _, err := l.Lock(ctx, "nightly", grace); err != nil {
		t.Fatal(err)
	}
	time.Sleep(grace + grace/2)

	token, err := l.Lock(ctx, "nightly", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if token == "" {
		t.Fatal("an expired lock was never released; a crashed server blocks the schedule forever")
	}
}

// testStaleUnlockSparesNewHolder is the fencing test, and the reason Lock hands
// back a token at all.
//
// Server A takes the lock, stalls past the TTL, and server B takes it. A then
// finishes and releases. Without the token check A's Unlock deletes B's lock,
// B's schedule is unprotected, and a third server fires the same job — so a
// nightly billing run goes out twice. The TTL alone does not prevent this; only
// the token does.
func testStaleUnlockSparesNewHolder(t *testing.T, newStore New) {
	ctx := context.Background()
	l := locker(t, newStore)

	tokenA, err := l.Lock(ctx, "nightly", grace)
	if err != nil {
		t.Fatal(err)
	}
	if tokenA == "" {
		t.Fatal("first Lock returned no token")
	}

	time.Sleep(grace + grace/2)

	tokenB, err := l.Lock(ctx, "nightly", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if tokenB == "" {
		t.Fatal("expired lock was not reacquirable")
	}

	if err := l.Unlock(ctx, "nightly", tokenA); err != nil {
		t.Fatalf("a stale Unlock must be a no-op, not an error: %v", err)
	}

	stolen, err := l.Lock(ctx, "nightly", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if stolen != "" {
		t.Fatal("a stale Unlock released the lock its previous holder no longer owned")
	}
}

func locker(t *testing.T, newStore New) jobsync.Locker {
	t.Helper()
	l, ok := newStore(t).(jobsync.Locker)
	if !ok {
		t.Fatal("storage is not a jobsync.Locker")
	}
	return l
}
