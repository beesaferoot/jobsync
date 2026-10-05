package storagetest

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/beesaferoot/jobsync"
)

func runQueueControl(t *testing.T, newStore New) {
	t.Run("PausedQueueYieldsNoJobs", func(t *testing.T) { testPausedQueueYieldsNoJobs(t, newStore) })
	t.Run("PauseIsPerQueue", func(t *testing.T) { testPauseIsPerQueue(t, newStore) })
	t.Run("ResumeDrainsTheBacklog", func(t *testing.T) { testResumeDrainsBacklog(t, newStore) })
	t.Run("PausedQueuesAreListed", func(t *testing.T) { testPausedQueuesListed(t, newStore) })
}

func queueControl(t *testing.T, s jobsync.Storage) jobsync.QueueControl {
	t.Helper()
	q, ok := s.(jobsync.QueueControl)
	if !ok {
		t.Fatal("storage is not a jobsync.QueueControl")
	}
	return q
}

func testPausedQueueYieldsNoJobs(t *testing.T, newStore New) {
	ctx := context.Background()
	s := newStore(t)
	q := queueControl(t, s)

	put(t, s, job("a"))
	if err := q.PauseQueue(ctx, "default"); err != nil {
		t.Fatal(err)
	}

	// The job is still there and still enqueued — pausing stops delivery, it does
	// not cancel work.
	if n := len(fetch(t, s, "srv-1", 10, time.Minute, "default")); n != 0 {
		t.Fatalf("a paused queue handed out %d jobs, want 0", n)
	}
	if m, ok := s.(jobsync.Monitor); ok {
		counts, err := m.Counts(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if counts.Enqueued != 1 {
			t.Errorf("Enqueued = %d, want 1: pausing must not lose the job", counts.Enqueued)
		}
	}
}

func testPauseIsPerQueue(t *testing.T, newStore New) {
	ctx := context.Background()
	s := newStore(t)
	q := queueControl(t, s)

	a, b := job("a"), job("b")
	a.Queue, b.Queue = "alpha", "beta"
	put(t, s, a, b)

	if err := q.PauseQueue(ctx, "alpha"); err != nil {
		t.Fatal(err)
	}

	// A server polling both queues still gets the unpaused one. Pausing one queue
	// must not stop the rest of the fleet's work.
	got := fetch(t, s, "srv-1", 10, time.Minute, "alpha", "beta")
	if len(got) != 1 || got[0].ID != "b" {
		t.Fatalf("fetch across a paused and a live queue = %v, want [b]", ids(got))
	}
}

func testResumeDrainsBacklog(t *testing.T, newStore New) {
	ctx := context.Background()
	s := newStore(t)
	q := queueControl(t, s)

	if err := q.PauseQueue(ctx, "default"); err != nil {
		t.Fatal(err)
	}
	put(t, s, job("first"), job("second"))
	if n := len(fetch(t, s, "srv-1", 10, time.Minute, "default")); n != 0 {
		t.Fatalf("paused queue handed out %d jobs", n)
	}

	if err := q.ResumeQueue(ctx, "default"); err != nil {
		t.Fatal(err)
	}
	got := ids(fetch(t, s, "srv-1", 10, time.Minute, "default"))
	slices.Sort(got)
	if !slices.Equal(got, []string{"first", "second"}) {
		t.Fatalf("after resume = %v, want both jobs", got)
	}
}

func testPausedQueuesListed(t *testing.T, newStore New) {
	ctx := context.Background()
	s := newStore(t)
	q := queueControl(t, s)

	if err := q.PauseQueue(ctx, "alpha"); err != nil {
		t.Fatal(err)
	}
	// Pausing twice is not an error: two dashboards with the same page open will
	// both send it.
	if err := q.PauseQueue(ctx, "alpha"); err != nil {
		t.Fatalf("pausing an already-paused queue: %v", err)
	}

	paused, err := q.PausedQueues(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(paused, []string{"alpha"}) {
		t.Fatalf("PausedQueues = %v, want [alpha]", paused)
	}

	if err := q.ResumeQueue(ctx, "alpha"); err != nil {
		t.Fatal(err)
	}
	if err := q.ResumeQueue(ctx, "alpha"); err != nil {
		t.Fatalf("resuming an already-live queue: %v", err)
	}
	if paused, _ = q.PausedQueues(ctx); len(paused) != 0 {
		t.Errorf("PausedQueues = %v after resume, want empty", paused)
	}

	// And the dashboard must see it, or an operator cannot tell a paused queue
	// from an idle one — which look identical and mean opposite things.
	if err := q.PauseQueue(ctx, "alpha"); err != nil {
		t.Fatal(err)
	}
	j := job("a")
	j.Queue = "alpha"
	put(t, s, j)

	if m, ok := s.(jobsync.Monitor); ok {
		stats, err := m.QueueStats(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, st := range stats {
			if st.Name == "alpha" && !st.Paused {
				t.Error("QueueStats reports a paused queue as live")
			}
		}
	}
}

func runSchedulePause(t *testing.T, newStore New) {
	ctx := context.Background()
	s := schedules(t, newStore)

	if err := s.SaveSchedule(ctx, schedule("nightly", time.Now().Add(-time.Minute))); err != nil {
		t.Fatal(err)
	}
	if err := s.SetSchedulePaused(ctx, "nightly", true); err != nil {
		t.Fatal(err)
	}

	due, err := s.DueSchedules(ctx, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 0 {
		t.Fatalf("a paused schedule is still due: %v", scheduleIDs(due))
	}

	// Pausing must not delete it: the dashboard has to list it to offer a resume,
	// and the definition has to survive to be resumed.
	all, err := s.ListSchedules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 || !all[0].Paused {
		t.Fatalf("ListSchedules = %+v, want one paused schedule", all)
	}

	if err := s.SetSchedulePaused(ctx, "nightly", false); err != nil {
		t.Fatal(err)
	}
	if due, _ = s.DueSchedules(ctx, time.Now()); len(due) != 1 {
		t.Fatalf("a resumed schedule is not due: %v", scheduleIDs(due))
	}

	if err := s.SetSchedulePaused(ctx, "no-such-schedule", true); err != nil {
		t.Errorf("pausing an absent schedule returned %v, want nil", err)
	}
}
