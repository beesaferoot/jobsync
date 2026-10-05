package storagetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/beesaferoot/jobsync"
)

// runMonitor exercises jobsync.Monitor, the read side the dashboard is built on.
//
// The states a Monitor reports are DERIVED, not stored. SQL drivers collapse
// Scheduled and Enqueued into one row state and work out the difference from
// scheduled_at at read time; Redis keeps them in priority sets. So these tests
// are written entirely against the contract's seven states and never assume a
// storage layout.
func runMonitor(t *testing.T, newStore New) {
	t.Run("CountsDeriveStates", func(t *testing.T) { testCountsDeriveStates(t, newStore) })
	t.Run("QueueStats", func(t *testing.T) { testQueueStats(t, newStore) })
	t.Run("ListJobsByState", func(t *testing.T) { testListJobsByState(t, newStore) })
	t.Run("ListJobsByQueueAndKind", func(t *testing.T) { testListJobsByQueueAndKind(t, newStore) })
	t.Run("ListJobsPaginates", func(t *testing.T) { testListJobsPaginates(t, newStore) })
	t.Run("JobHistory", func(t *testing.T) { testJobHistory(t, newStore) })
	t.Run("Requeue", func(t *testing.T) { testRequeue(t, newStore) })
	t.Run("Delete", func(t *testing.T) { testMonitorDelete(t, newStore) })
	t.Run("Throughput", func(t *testing.T) { testThroughput(t, newStore) })
	t.Run("SearchOrUnsupported", func(t *testing.T) { testSearchOrUnsupported(t, newStore) })
}

// seed builds one job in each of the five states reachable without waiting, and
// returns the storage. The ids say what they are.
func seed(t *testing.T, s jobsync.Storage) jobsync.Monitor {
	t.Helper()
	ctx := context.Background()
	m, ok := s.(jobsync.Monitor)
	if !ok {
		t.Fatal("storage is not a jobsync.Monitor")
	}

	enq := job("enqueued")

	sched := job("scheduled")
	sched.ScheduledAt = time.Now().Add(time.Hour)
	sched.State = jobsync.StateScheduled

	run := job("running")
	succ := job("succeeded")
	dead := job("dead")
	retry := job("retrying")
	put(t, s, enq, sched, run, succ, dead, retry)

	// Drive the rest through the real lifecycle rather than writing states
	// directly: a Monitor that only agrees with hand-placed rows is not telling
	// you about the jobs your servers actually ran.

	// Drive the rest through the real lifecycle rather than writing states
	// directly: a Monitor that only agrees with hand-placed rows is not telling
	// you about the jobs your servers actually ran.
	claimed, err := s.Fetch(ctx, []string{"default"}, 10, "srv-1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	have := map[string]bool{}
	for _, j := range claimed {
		have[j.ID] = true
	}
	for _, id := range []string{"enqueued", "running", "succeeded", "dead", "retrying"} {
		if !have[id] {
			t.Fatalf("seed could not claim %s; got %v", id, ids(claimed))
		}
	}

	finish := func(id string, r jobsync.Result) {
		t.Helper()
		if err := s.Finish(ctx, id, "srv-1", r); err != nil {
			t.Fatal(err)
		}
	}
	finish("succeeded", jobsync.Result{State: jobsync.StateSucceeded})
	finish("dead", jobsync.Result{State: jobsync.StateDead, Err: "boom"})
	finish("retrying", jobsync.Result{State: jobsync.StateRetrying, RetryAt: time.Now().Add(time.Hour), Err: "flaky"})

	// "enqueued" was claimed by the seed fetch; hand it back so it is enqueued
	// again rather than running.
	finish("enqueued", jobsync.Result{State: jobsync.StateRetrying, RetryAt: time.Now().Add(-time.Hour)})

	return m
}

func testCountsDeriveStates(t *testing.T, newStore New) {
	s := newStore(t)
	m := seed(t, s)

	got, err := m.Counts(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// "scheduled" is future-dated with no attempts; "retrying" is future-dated
	// after a failure. Both are stored the same way by the SQL drivers, and the
	// attempt count is the only thing that tells them apart.
	checks := []struct {
		name string
		got  int64
		want int64
	}{
		{"Scheduled", got.Scheduled, 1},
		{"Enqueued", got.Enqueued, 1},
		{"Running", got.Running, 1},
		{"Retrying", got.Retrying, 1},
		{"Succeeded", got.Succeeded, 1},
		{"Dead", got.Dead, 1},
		{"Cancelled", got.Cancelled, 0},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("Counts.%s = %d, want %d (full: %+v)", c.name, c.got, c.want, got)
		}
	}
}

func testQueueStats(t *testing.T, newStore New) {
	ctx := context.Background()
	s := newStore(t)
	m, ok := s.(jobsync.Monitor)
	if !ok {
		t.Fatal("storage is not a jobsync.Monitor")
	}

	a, b, c := job("a"), job("b"), job("c")
	a.Queue, b.Queue, c.Queue = "alpha", "alpha", "beta"
	put(t, s, a, b, c)
	if _, err := s.Fetch(ctx, []string{"beta"}, 1, "srv-1", time.Minute); err != nil {
		t.Fatal(err)
	}

	stats, err := m.QueueStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	by := map[string]jobsync.QueueStat{}
	for _, q := range stats {
		by[q.Name] = q
	}

	if by["alpha"].Enqueued != 2 {
		t.Errorf("alpha Enqueued = %d, want 2 (got %+v)", by["alpha"].Enqueued, stats)
	}
	if by["beta"].Running != 1 {
		t.Errorf("beta Running = %d, want 1 (got %+v)", by["beta"].Running, stats)
	}
	// The age of the head of the queue is the number worth alerting on: a queue
	// with 10 jobs that are all 2 seconds old is healthy, 1 job 40 minutes old is
	// not.
	if by["alpha"].OldestEnqueued <= 0 {
		t.Errorf("alpha OldestEnqueued = %v, want a positive age", by["alpha"].OldestEnqueued)
	}
}

func testListJobsByState(t *testing.T, newStore New) {
	s := newStore(t)
	m := seed(t, s)

	for _, state := range []jobsync.State{
		jobsync.StateEnqueued, jobsync.StateRunning, jobsync.StateSucceeded,
		jobsync.StateDead, jobsync.StateScheduled, jobsync.StateRetrying,
	} {
		jobs, total, err := m.ListJobs(context.Background(), jobsync.Filter{States: []jobsync.State{state}})
		if err != nil {
			t.Fatalf("ListJobs(%s): %v", state, err)
		}
		if total != 1 || len(jobs) != 1 {
			t.Errorf("ListJobs(%s) = %v (total %d), want exactly one job", state, ids(jobs), total)
			continue
		}
		if jobs[0].State != state {
			t.Errorf("ListJobs(%s) returned a job in state %s", state, jobs[0].State)
		}
	}
}

func testListJobsByQueueAndKind(t *testing.T, newStore New) {
	ctx := context.Background()
	s := newStore(t)
	m, ok := s.(jobsync.Monitor)
	if !ok {
		t.Fatal("storage is not a jobsync.Monitor")
	}

	mail, report := job("mail"), job("report")
	mail.Queue, mail.Kind = "alpha", "email.send"
	report.Queue, report.Kind = "beta", "report.build"
	put(t, s, mail, report)

	jobs, total, err := m.ListJobs(ctx, jobsync.Filter{Queues: []string{"alpha"}})
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(jobs) != 1 || jobs[0].ID != "mail" {
		t.Errorf("filter by queue = %v (total %d), want [mail]", ids(jobs), total)
	}

	jobs, total, err = m.ListJobs(ctx, jobsync.Filter{Kinds: []string{"report.build"}})
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(jobs) != 1 || jobs[0].ID != "report" {
		t.Errorf("filter by kind = %v (total %d), want [report]", ids(jobs), total)
	}
}

func testListJobsPaginates(t *testing.T, newStore New) {
	ctx := context.Background()
	s := newStore(t)
	m, ok := s.(jobsync.Monitor)
	if !ok {
		t.Fatal("storage is not a jobsync.Monitor")
	}

	for i := range 5 {
		put(t, s, job(string(rune('a'+i))))
	}

	jobs, total, err := m.ListJobs(ctx, jobsync.Filter{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 2 {
		t.Errorf("page size = %d, want 2", len(jobs))
	}
	// Total is the count matching the filter, not the page. Without that the
	// dashboard cannot render "showing 2 of 5" or a pager.
	if total != 5 {
		t.Errorf("total = %d, want 5 (the match count, not the page length)", total)
	}

	page2, _, err := m.ListJobs(ctx, jobsync.Filter{Limit: 2, Offset: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(page2) != 2 {
		t.Fatalf("second page = %d jobs, want 2", len(page2))
	}
	for _, a := range jobs {
		for _, b := range page2 {
			if a.ID == b.ID {
				t.Errorf("job %s appears on both pages; the ordering is not stable", a.ID)
			}
		}
	}
}

func testJobHistory(t *testing.T, newStore New) {
	ctx := context.Background()
	s := newStore(t)
	m, ok := s.(jobsync.Monitor)
	if !ok {
		t.Fatal("storage is not a jobsync.Monitor")
	}

	put(t, s, job("a"))
	fetchOne(t, s, "srv-1", time.Minute)
	err := s.Finish(ctx, "a", "srv-1", jobsync.Result{
		State: jobsync.StateRetrying, RetryAt: time.Now().Add(time.Hour), Err: "smtp refused",
	})
	if err != nil {
		t.Fatal(err)
	}

	job, transitions, err := m.JobHistory(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	if job == nil {
		t.Fatal("JobHistory returned no job")
	}
	if job.LastError != "smtp refused" {
		t.Errorf("LastError = %q, want the failure message", job.LastError)
	}
	if len(transitions) == 0 {
		t.Fatal("no transitions recorded; the job detail page has nothing to show")
	}
	last := transitions[len(transitions)-1]
	if last.State != jobsync.StateRetrying {
		t.Errorf("last transition state = %s, want retrying", last.State)
	}
	if last.Reason != "smtp refused" {
		t.Errorf("last transition reason = %q, want the failure message", last.Reason)
	}
	if last.At.IsZero() {
		t.Error("transition has no timestamp")
	}

	if _, _, err := m.JobHistory(ctx, "no-such-job"); err == nil {
		t.Error("JobHistory for an unknown id returned no error")
	}
}

func testRequeue(t *testing.T, newStore New) {
	ctx := context.Background()
	s := newStore(t)
	m := seed(t, s)

	if err := m.Requeue(ctx, []string{"dead"}); err != nil {
		t.Fatal(err)
	}

	jobs, _, err := m.ListJobs(ctx, jobsync.Filter{States: []jobsync.State{jobsync.StateEnqueued}})
	if err != nil {
		t.Fatal(err)
	}
	if !slicesContain(ids(jobs), "dead") {
		t.Fatalf("requeued job is not enqueued: %v", ids(jobs))
	}

	// And it must actually be claimable again, not merely relabelled.
	claimed := fetch(t, s, "srv-2", 10, time.Minute, "default")
	if !slicesContain(ids(claimed), "dead") {
		t.Errorf("requeued job was not fetchable: got %v", ids(claimed))
	}
}

func testMonitorDelete(t *testing.T, newStore New) {
	ctx := context.Background()
	s := newStore(t)
	m := seed(t, s)

	if err := m.Delete(ctx, []string{"succeeded", "dead"}); err != nil {
		t.Fatal(err)
	}

	counts, err := m.Counts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if counts.Succeeded != 0 || counts.Dead != 0 {
		t.Errorf("after Delete: Succeeded=%d Dead=%d, want 0 and 0", counts.Succeeded, counts.Dead)
	}

	// Deleting something already gone is not an error: two dashboards with the
	// same page open will both send it.
	if err := m.Delete(ctx, []string{"succeeded"}); err != nil {
		t.Errorf("deleting an absent job returned %v, want nil", err)
	}
}

func testThroughput(t *testing.T, newStore New) {
	ctx := context.Background()
	s := newStore(t)
	m := seed(t, s)

	buckets, err := m.Throughput(ctx, time.Now().Add(-time.Hour), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if buckets == nil {
		t.Skip("driver keeps no throughput history; the dashboard hides the graph")
	}

	var succeeded, failed int64
	for _, b := range buckets {
		succeeded += b.Succeeded
		failed += b.Failed
		if b.At.IsZero() {
			t.Error("bucket has no timestamp")
		}
	}
	if succeeded != 1 {
		t.Errorf("throughput succeeded = %d, want 1", succeeded)
	}
	if failed != 1 {
		t.Errorf("throughput failed = %d, want 1 (the dead job)", failed)
	}
}

// Search is the one filter a driver may decline. Either it works, or it says so
// precisely — what it must never do is silently return unfiltered results, which
// would show an operator the wrong jobs during an incident.
func testSearchOrUnsupported(t *testing.T, newStore New) {
	ctx := context.Background()
	s := newStore(t)
	m, ok := s.(jobsync.Monitor)
	if !ok {
		t.Fatal("storage is not a jobsync.Monitor")
	}

	mail, report := job("mail"), job("report")
	mail.Kind, report.Kind = "email.send", "report.build"
	put(t, s, mail, report)

	jobs, _, err := m.ListJobs(ctx, jobsync.Filter{Search: "email"})
	if errors.Is(err, jobsync.ErrUnsupportedFilter) {
		t.Log("driver declines Search; the dashboard disables the control")
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].ID != "mail" {
		t.Errorf("Search(%q) = %v, want [mail]; a driver that cannot filter must return ErrUnsupportedFilter rather than everything", "email", ids(jobs))
	}
}

func slicesContain(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}
