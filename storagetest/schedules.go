package storagetest

import (
	"context"
	"testing"
	"time"

	"github.com/beesaferoot/jobsync"
)

// runSchedules exercises jobsync.Schedules, the storage behind recurring jobs.
// Cron parsing is never tested here: the server computes NextRun and the driver
// stores a timestamp, so adding a backend never means reimplementing a scheduler.
func runSchedules(t *testing.T, newStore New) {
	t.Run("SaveAndList", func(t *testing.T) { testSaveAndList(t, newStore) })
	t.Run("SaveIsUpsert", func(t *testing.T) { testSaveIsUpsert(t, newStore) })
	t.Run("DueExcludesFuture", func(t *testing.T) { testDueExcludesFuture(t, newStore) })
	t.Run("DueExcludesPaused", func(t *testing.T) { testDueExcludesPaused(t, newStore) })
	t.Run("AdvanceClearsDue", func(t *testing.T) { testAdvanceClearsDue(t, newStore) })
	t.Run("Remove", func(t *testing.T) { testRemoveSchedule(t, newStore) })
}

func testSaveAndList(t *testing.T, newStore New) {
	ctx := context.Background()
	s := schedules(t, newStore)

	want := schedule("nightly", time.Now().Add(-time.Minute))
	want.Cron = "0 2 * * *"
	want.Timezone = "Africa/Lagos"
	want.Payload = []byte(`{"tenant":42}`)
	if err := s.SaveSchedule(ctx, want); err != nil {
		t.Fatal(err)
	}

	got, err := s.ListSchedules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d schedules, want 1", len(got))
	}
	if got[0].ID != want.ID || got[0].Cron != want.Cron || got[0].Timezone != want.Timezone {
		t.Errorf("schedule did not round-trip: got %+v, want %+v", got[0], want)
	}
	if string(got[0].Payload) != string(want.Payload) {
		t.Errorf("payload = %q, want %q", got[0].Payload, want.Payload)
	}
	if !got[0].NextRun.Truncate(time.Millisecond).Equal(want.NextRun.Truncate(time.Millisecond)) {
		t.Errorf("NextRun = %v, want %v", got[0].NextRun, want.NextRun)
	}
}

// A redeploy re-registers every schedule on startup. If that appended rather
// than replaced, the job would fire once per deploy that had ever happened.
func testSaveIsUpsert(t *testing.T, newStore New) {
	ctx := context.Background()
	s := schedules(t, newStore)

	first := schedule("nightly", time.Now().Add(-time.Minute))
	first.Cron = "0 2 * * *"
	if err := s.SaveSchedule(ctx, first); err != nil {
		t.Fatal(err)
	}

	second := first
	second.Cron = "0 5 * * *"
	if err := s.SaveSchedule(ctx, second); err != nil {
		t.Fatal(err)
	}

	got, err := s.ListSchedules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("re-saving the same id produced %d schedules, want 1", len(got))
	}
	if got[0].Cron != "0 5 * * *" {
		t.Errorf("Cron = %q, want the re-saved value", got[0].Cron)
	}
}

func testDueExcludesFuture(t *testing.T, newStore New) {
	ctx := context.Background()
	s := schedules(t, newStore)

	put := func(id string, next time.Time) {
		if err := s.SaveSchedule(ctx, schedule(id, next)); err != nil {
			t.Fatal(err)
		}
	}
	put("due", time.Now().Add(-time.Minute))
	put("later", time.Now().Add(time.Hour))

	got, err := s.DueSchedules(ctx, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != "due" {
		t.Fatalf("DueSchedules = %v, want [due]", scheduleIDs(got))
	}
}

func testDueExcludesPaused(t *testing.T, newStore New) {
	ctx := context.Background()
	s := schedules(t, newStore)

	paused := schedule("paused", time.Now().Add(-time.Minute))
	paused.Paused = true
	if err := s.SaveSchedule(ctx, paused); err != nil {
		t.Fatal(err)
	}

	got, err := s.DueSchedules(ctx, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("a paused schedule was due: %v", scheduleIDs(got))
	}

	// Pausing must not lose the schedule — the dashboard has to list it to offer
	// an unpause.
	all, err := s.ListSchedules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 || !all[0].Paused {
		t.Fatalf("ListSchedules = %+v, want one paused schedule", all)
	}
}

func testAdvanceClearsDue(t *testing.T, newStore New) {
	ctx := context.Background()
	s := schedules(t, newStore)

	if err := s.SaveSchedule(ctx, schedule("nightly", time.Now().Add(-time.Minute))); err != nil {
		t.Fatal(err)
	}

	lastRun := time.Now().Truncate(time.Millisecond)
	nextRun := lastRun.Add(time.Hour)
	if err := s.AdvanceSchedule(ctx, "nightly", lastRun, nextRun); err != nil {
		t.Fatal(err)
	}

	due, err := s.DueSchedules(ctx, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 0 {
		t.Fatalf("an advanced schedule is still due, so it would fire every tick: %v", scheduleIDs(due))
	}

	all, err := s.ListSchedules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 {
		t.Fatalf("got %d schedules, want 1", len(all))
	}
	if !all[0].NextRun.Truncate(time.Millisecond).Equal(nextRun) {
		t.Errorf("NextRun = %v, want %v", all[0].NextRun, nextRun)
	}
	if !all[0].LastRun.Truncate(time.Millisecond).Equal(lastRun) {
		t.Errorf("LastRun = %v, want %v", all[0].LastRun, lastRun)
	}
}

func testRemoveSchedule(t *testing.T, newStore New) {
	ctx := context.Background()
	s := schedules(t, newStore)

	if err := s.SaveSchedule(ctx, schedule("nightly", time.Now())); err != nil {
		t.Fatal(err)
	}
	if err := s.RemoveSchedule(ctx, "nightly"); err != nil {
		t.Fatal(err)
	}

	got, err := s.ListSchedules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("schedule survived removal: %v", scheduleIDs(got))
	}

	// Removing something that is not there is not an error: two dashboards with
	// the same page open will both send the delete.
	if err := s.RemoveSchedule(ctx, "nightly"); err != nil {
		t.Errorf("removing an absent schedule returned %v, want nil", err)
	}
}

func schedule(id string, next time.Time) jobsync.Schedule {
	return jobsync.Schedule{
		ID:       id,
		Kind:     "test",
		Queue:    "default",
		Payload:  []byte(`{}`),
		Cron:     "* * * * *",
		Timezone: "UTC",
		NextRun:  next,
	}
}

func scheduleIDs(ss []jobsync.Schedule) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = s.ID
	}
	return out
}

func schedules(t *testing.T, newStore New) jobsync.Schedules {
	t.Helper()
	s, ok := newStore(t).(jobsync.Schedules)
	if !ok {
		t.Fatal("storage is not a jobsync.Schedules")
	}
	return s
}
