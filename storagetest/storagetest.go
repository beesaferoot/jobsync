// Package storagetest is the conformance suite for jobsync Storage drivers. A
// driver is "official" when it passes Run with zero skips.
//
// This exists because a prose contract is not a contract. The subtle parts of
// Storage — that Fetch is atomic across processes, that a late Finish from a
// server whose lease expired is ignored rather than applied, that a terminal job
// releases its unique key — are exactly the parts a new driver gets wrong, and
// exactly the parts that stay invisible until production. Each is a test here.
//
// A driver's test file is three lines:
//
//	func TestConformance(t *testing.T) {
//		storagetest.Run(t, func(t *testing.T) jobsync.Storage { return newTestStore(t) })
//	}
package storagetest

import (
	"bytes"
	"context"
	"slices"
	"testing"
	"time"

	"github.com/beesaferoot/jobsync"
)

// New returns a storage with no jobs in it. It is called once per subtest and
// must clean up after itself via t.Cleanup.
type New func(t *testing.T) jobsync.Storage

// grace is how long a test waits past a deadline before concluding a job should
// have become due. Generous on purpose: these tests run against containers on
// loaded CI machines, and a flaky conformance suite gets ignored, which defeats
// the point of having one.
const grace = 500 * time.Millisecond

func Run(t *testing.T, newStore New) {
	t.Run("EnqueueThenFetch", func(t *testing.T) { testEnqueueThenFetch(t, newStore) })
	t.Run("FetchIsExclusive", func(t *testing.T) { testFetchIsExclusive(t, newStore) })
	t.Run("ScheduledNotFetchedEarly", func(t *testing.T) { testScheduledNotFetchedEarly(t, newStore) })
	t.Run("PriorityOrder", func(t *testing.T) { testPriorityOrder(t, newStore) })
	t.Run("ReclaimExpiredLease", func(t *testing.T) { testReclaimExpiredLease(t, newStore) })
	t.Run("FinishFromStaleOwnerIgnored", func(t *testing.T) { testFinishFromStaleOwner(t, newStore) })
	t.Run("RetryBecomesFetchable", func(t *testing.T) { testRetryBecomesFetchable(t, newStore) })
	t.Run("UniqueKeyDedupes", func(t *testing.T) { testUniqueKeyDedupes(t, newStore) })
	t.Run("QueueIsolation", func(t *testing.T) { testQueueIsolation(t, newStore) })
	t.Run("PayloadRoundTrip", func(t *testing.T) { testPayloadRoundTrip(t, newStore) })
	t.Run("DuplicateIDIsIgnored", func(t *testing.T) { testDuplicateIDIgnored(t, newStore) })
	t.Run("ScheduledAtKeepsItsInstant", func(t *testing.T) { testScheduledAtTimezone(t, newStore) })

	// Optional capabilities. Skipping here is legitimate in a way that skipping
	// for missing infrastructure never is: the contract says a driver may decline
	// these, and one that does still runs jobs correctly.
	t.Run("Locker", func(t *testing.T) {
		if _, ok := newStore(t).(jobsync.Locker); !ok {
			t.Skip("driver does not implement jobsync.Locker; it cannot run recurring jobs")
		}
		runLocker(t, newStore)
	})
	t.Run("Monitor", func(t *testing.T) {
		if _, ok := newStore(t).(jobsync.Monitor); !ok {
			t.Skip("driver does not implement jobsync.Monitor; the dashboard reports no introspection")
		}
		runMonitor(t, newStore)
	})
	t.Run("Schedules", func(t *testing.T) {
		if _, ok := newStore(t).(jobsync.Schedules); !ok {
			t.Skip("driver does not implement jobsync.Schedules; it cannot store recurring jobs")
		}
		runSchedules(t, newStore)
		t.Run("Pause", func(t *testing.T) { runSchedulePause(t, newStore) })
	})
	t.Run("QueueControl", func(t *testing.T) {
		if _, ok := newStore(t).(jobsync.QueueControl); !ok {
			t.Skip("driver does not implement jobsync.QueueControl; queues cannot be paused")
		}
		runQueueControl(t, newStore)
	})
}

func testEnqueueThenFetch(t *testing.T, newStore New) {
	s := newStore(t)

	want := job("a")
	want.Kind = "report.nightly"
	want.MaxAttempts = 7
	put(t, s, want)

	got := fetchOne(t, s, "srv-1", time.Minute)
	if got.ID != want.ID || got.Kind != want.Kind || got.MaxAttempts != want.MaxAttempts {
		t.Errorf("job did not round-trip: got %+v, want %+v", got, want)
	}
	if got.State != jobsync.StateRunning {
		t.Errorf("state after fetch = %q, want %q", got.State, jobsync.StateRunning)
	}
	if got.Owner != "srv-1" {
		t.Errorf("owner after fetch = %q, want srv-1", got.Owner)
	}
	if !got.LeasedUntil.After(time.Now()) {
		t.Errorf("lease expires at %v, which is not in the future", got.LeasedUntil)
	}

	// A claimed job is not still on offer.
	if n := len(fetch(t, s, "srv-2", 10, time.Minute, "default")); n != 0 {
		t.Errorf("claimed job was fetched again: got %d jobs, want 0", n)
	}
}

func testFetchIsExclusive(t *testing.T, newStore New) {
	s := newStore(t)
	put(t, s, job("a"))

	// Two servers, one job. Exactly one may win. A driver that locks
	// optimistically and retries, or that forgets SKIP LOCKED, fails here under
	// -race -count=50.
	claims := make(chan int, 2)
	for _, owner := range []string{"srv-1", "srv-2"} {
		go func() {
			jobs, err := s.Fetch(context.Background(), []string{"default"}, 1, owner, time.Minute)
			if err != nil {
				t.Error(err)
			}
			claims <- len(jobs)
		}()
	}

	got := <-claims + <-claims
	if got != 1 {
		t.Fatalf("one job claimed by two servers: got %d claims, want 1", got)
	}
}

func testScheduledNotFetchedEarly(t *testing.T, newStore New) {
	s := newStore(t)

	later := job("later")
	later.ScheduledAt = time.Now().Add(grace)
	later.State = jobsync.StateScheduled

	now := job("now")
	put(t, s, later, now)

	// Only the due one is on offer.
	got := fetch(t, s, "srv-1", 10, time.Minute, "default")
	if len(got) != 1 || got[0].ID != "now" {
		t.Fatalf("before the deadline, got %v, want [now]", ids(got))
	}

	time.Sleep(grace + grace/2)

	got = fetch(t, s, "srv-1", 10, time.Minute, "default")
	if len(got) != 1 || got[0].ID != "later" {
		t.Fatalf("after the deadline, got %v, want [later]", ids(got))
	}
}

func testPriorityOrder(t *testing.T, newStore New) {
	s := newStore(t)

	// Priorities stay within 0..9: drivers with coarse priority granularity clamp
	// to that range, so a test using 100 would be testing something no driver
	// promises. Lower runs first.
	high, mid, low := job("high"), job("mid"), job("low")
	high.Priority, mid.Priority, low.Priority = 0, 1, 2

	// Equal priority breaks the tie on ScheduledAt, oldest first. mid shares
	// priority 1 with these two and was scheduled now, so it sorts behind both.
	older, newer := job("older"), job("newer")
	older.Priority, newer.Priority = 1, 1
	older.ScheduledAt = time.Now().Add(-time.Hour)
	newer.ScheduledAt = time.Now().Add(-time.Minute)

	put(t, s, low, newer, high, older, mid)

	got := ids(fetch(t, s, "srv-1", 5, time.Minute, "default"))
	want := []string{"high", "older", "newer", "mid", "low"}
	if !slices.Equal(got, want) {
		t.Errorf("fetch order = %v, want %v", got, want)
	}
}

func testReclaimExpiredLease(t *testing.T, newStore New) {
	ctx := context.Background()
	s := newStore(t)
	put(t, s, job("a"))

	fetchOne(t, s, "srv-1", grace)

	// Still leased: nothing to reclaim, and nobody else may have it.
	if n, err := s.Reclaim(ctx); err != nil || n != 0 {
		t.Fatalf("Reclaim while leased = (%d, %v), want (0, nil)", n, err)
	}
	if n := len(fetch(t, s, "srv-2", 10, time.Minute, "default")); n != 0 {
		t.Fatalf("leased job was handed to a second server: got %d jobs, want 0", n)
	}

	time.Sleep(grace + grace/2)

	n, err := s.Reclaim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("Reclaim after lease expiry = %d, want 1", n)
	}
	if got := fetchOne(t, s, "srv-2", time.Minute); got.ID != "a" {
		t.Errorf("reclaimed job not refetchable: got %q", got.ID)
	}
}

func testFinishFromStaleOwner(t *testing.T, newStore New) {
	ctx := context.Background()
	s := newStore(t)
	put(t, s, job("a"))

	fetchOne(t, s, "srv-1", grace)
	time.Sleep(grace + grace/2)
	if _, err := s.Reclaim(ctx); err != nil {
		t.Fatal(err)
	}
	if got := fetchOne(t, s, "srv-2", time.Minute); got.ID != "a" {
		t.Fatalf("reclaimed job not refetchable: got %q", got.ID)
	}

	// srv-1 finally finishes. Its result describes an attempt that has been
	// superseded, so it must not mark the job succeeded out from under srv-2 —
	// and must not be an error either, or every reclaimed job logs a scary line
	// forever.
	if err := s.Finish(ctx, "a", "srv-1", jobsync.Result{State: jobsync.StateSucceeded}); err != nil {
		t.Fatalf("stale Finish must be ignored, not an error: %v", err)
	}
	if err := s.Finish(ctx, "a", "srv-2", jobsync.Result{State: jobsync.StateSucceeded}); err != nil {
		t.Fatalf("owner Finish rejected: %v", err)
	}
}

func testRetryBecomesFetchable(t *testing.T, newStore New) {
	ctx := context.Background()
	s := newStore(t)
	put(t, s, job("a"))

	fetchOne(t, s, "srv-1", time.Minute)
	err := s.Finish(ctx, "a", "srv-1", jobsync.Result{
		State:   jobsync.StateRetrying,
		RetryAt: time.Now().Add(grace),
		Err:     "smtp: connection refused",
	})
	if err != nil {
		t.Fatal(err)
	}

	if n := len(fetch(t, s, "srv-1", 10, time.Minute, "default")); n != 0 {
		t.Fatalf("retry was fetchable before RetryAt: got %d jobs, want 0", n)
	}

	time.Sleep(grace + grace/2)

	got := fetchOne(t, s, "srv-1", time.Minute)
	if got.Attempt != 1 {
		t.Errorf("Attempt after one failure = %d, want 1", got.Attempt)
	}
	if got.LastError != "smtp: connection refused" {
		t.Errorf("LastError = %q, want the message passed to Finish", got.LastError)
	}
}

func testUniqueKeyDedupes(t *testing.T, newStore New) {
	ctx := context.Background()
	s := newStore(t)

	first, second := job("first"), job("second")
	first.UniqueKey, second.UniqueKey = "report:2026-10-05", "report:2026-10-05"

	put(t, s, first)
	// A duplicate is dropped silently. It is not an error: the caller asked for
	// the work to happen once, and it is going to.
	put(t, s, second)

	got := fetch(t, s, "srv-1", 10, time.Minute, "default")
	if len(got) != 1 || got[0].ID != "first" {
		t.Fatalf("unique key did not dedupe: got %v, want [first]", ids(got))
	}

	if err := s.Finish(ctx, "first", "srv-1", jobsync.Result{State: jobsync.StateSucceeded}); err != nil {
		t.Fatal(err)
	}

	// The key is released on a terminal state. Without this, tomorrow's run of a
	// daily job is blocked by yesterday's success.
	third := job("third")
	third.UniqueKey = "report:2026-10-05"
	put(t, s, third)

	got = fetch(t, s, "srv-1", 10, time.Minute, "default")
	if len(got) != 1 || got[0].ID != "third" {
		t.Fatalf("unique key not released by a terminal state: got %v, want [third]", ids(got))
	}
}

func testQueueIsolation(t *testing.T, newStore New) {
	s := newStore(t)

	alpha, beta := job("alpha"), job("beta")
	alpha.Queue, beta.Queue = "alpha", "beta"
	put(t, s, alpha, beta)

	got := fetch(t, s, "srv-1", 10, time.Minute, "alpha")
	if len(got) != 1 || got[0].ID != "alpha" {
		t.Fatalf("fetching queue alpha got %v, want [alpha]", ids(got))
	}

	got = fetch(t, s, "srv-1", 10, time.Minute, "alpha", "beta")
	if len(got) != 1 || got[0].ID != "beta" {
		t.Fatalf("fetching both queues got %v, want [beta]", ids(got))
	}

	if n := len(fetch(t, s, "srv-1", 10, time.Minute, "gamma")); n != 0 {
		t.Errorf("fetching an unknown queue got %d jobs, want 0", n)
	}
}

func testPayloadRoundTrip(t *testing.T, newStore New) {
	s := newStore(t)

	// Payload is opaque bytes, not text. The NUL and the invalid UTF-8 byte catch
	// a driver that stored it in a TEXT/VARCHAR column: Postgres rejects NUL in
	// TEXT outright, and MySQL's utf8mb4 rejects 0xFF.
	payload := []byte("{\"to\":\"ünïcode@example.com\",\"note\":\"quote\\\" and \\u0000 nul\"}\x00\xff")

	want := job("a")
	want.Payload = payload
	want.Tags = []string{"billing", "tenant:42"}
	want.Kind = "invoice.send"
	put(t, s, want)

	got := fetchOne(t, s, "srv-1", time.Minute)
	if !bytes.Equal(got.Payload, payload) {
		t.Errorf("payload corrupted:\n got %q\nwant %q", got.Payload, payload)
	}
	if !slices.Equal(got.Tags, want.Tags) {
		t.Errorf("tags = %v, want %v", got.Tags, want.Tags)
	}
}

// A job whose id already exists must be ignored, not overwritten. The scheduler
// relies on it: a server crashing between enqueuing a tick and recording it
// refires the same tick, and overwriting would resurrect the finished job and
// run the work a second time.
//
// Postgres and MySQL get this free from ON CONFLICT DO NOTHING. The memory and
// Redis drivers both overwrote, which is why this test exists.
func testDuplicateIDIgnored(t *testing.T, newStore New) {
	ctx := context.Background()
	s := newStore(t)

	put(t, s, job("a"))
	fetchOne(t, s, "srv-1", time.Minute)
	if err := s.Finish(ctx, "a", "srv-1", jobsync.Result{State: jobsync.StateSucceeded}); err != nil {
		t.Fatal(err)
	}

	// Same id, enqueued again. The finished job must stay finished.
	put(t, s, job("a"))

	if n := len(fetch(t, s, "srv-1", 10, time.Minute, "default")); n != 0 {
		t.Fatalf("re-enqueuing a finished job's id made it runnable again: got %d jobs, want 0", n)
	}

	// And a duplicate of a job still waiting must not reset it either.
	put(t, s, job("b"))
	again := job("b")
	again.Kind = "overwritten"
	put(t, s, again)

	got := fetch(t, s, "srv-1", 10, time.Minute, "default")
	if len(got) != 1 {
		t.Fatalf("got %d jobs, want 1", len(got))
	}
	if got[0].Kind != "test" {
		t.Errorf("duplicate enqueue overwrote the existing job: Kind = %q, want the original", got[0].Kind)
	}
}

func testScheduledAtTimezone(t *testing.T, newStore New) {
	s := newStore(t)

	// Asia/Kolkata is UTC+05:30 on purpose. A half-hour offset catches both sign
	// errors and the whole-hour assumption that a European test timezone would
	// let through. This is the MySQL DATETIME-vs-TIMESTAMP trap: get it wrong and
	// jobs fire hours off while every other test still passes.
	kolkata, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		t.Skipf("tzdata unavailable: %v", err)
	}

	want := job("a")
	want.ScheduledAt = time.Now().Add(-time.Hour).In(kolkata).Truncate(time.Millisecond)
	put(t, s, want)

	got := fetchOne(t, s, "srv-1", time.Minute)
	if !got.ScheduledAt.Truncate(time.Millisecond).Equal(want.ScheduledAt) {
		t.Errorf("ScheduledAt lost its instant:\n got %v (%v)\nwant %v (%v)\n drift %v",
			got.ScheduledAt, got.ScheduledAt.UTC(),
			want.ScheduledAt, want.ScheduledAt.UTC(),
			got.ScheduledAt.Sub(want.ScheduledAt))
	}
}

// job returns a job that is due. Its ScheduledAt is backdated a second rather
// than set to time.Now(), because due-ness is evaluated against the storage's
// clock and this process's clock is not the storage's. Sub-millisecond skew
// between a host and a database container is routine, and a job scheduled for
// this exact instant can land marginally in the storage's future.
//
// This is not papering over a driver bug. Asserting that a job is fetchable in
// the same microsecond it was enqueued would be asserting that two machines
// agree on the time, which no distributed system promises. Tests that care about
// a deadline set ScheduledAt explicitly.
func job(id string) *jobsync.Job {
	now := time.Now()
	return &jobsync.Job{
		ID:          id,
		Kind:        "test",
		Queue:       "default",
		Payload:     []byte(`{}`),
		MaxAttempts: 3,
		State:       jobsync.StateEnqueued,
		CreatedAt:   now,
		ScheduledAt: now.Add(-time.Second),
	}
}

func put(t *testing.T, s jobsync.Storage, jobs ...*jobsync.Job) {
	t.Helper()
	if err := s.Enqueue(context.Background(), jobs); err != nil {
		t.Fatal(err)
	}
}

func fetch(t *testing.T, s jobsync.Storage, owner string, n int, lease time.Duration, queues ...string) []*jobsync.Job {
	t.Helper()
	jobs, err := s.Fetch(context.Background(), queues, n, owner, lease)
	if err != nil {
		t.Fatal(err)
	}
	return jobs
}

func fetchOne(t *testing.T, s jobsync.Storage, owner string, lease time.Duration) *jobsync.Job {
	t.Helper()
	jobs := fetch(t, s, owner, 1, lease, "default")
	if len(jobs) != 1 {
		t.Fatalf("got %d jobs, want 1", len(jobs))
	}
	return jobs[0]
}

func ids(jobs []*jobsync.Job) []string {
	out := make([]string, len(jobs))
	for i, j := range jobs {
		out[i] = j.ID
	}
	return out
}
