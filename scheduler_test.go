package jobsync_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beesaferoot/jobsync"
	"github.com/beesaferoot/jobsync/driver/memory"
)

type nightlyArgs struct {
	Tenant int `json:"tenant"`
}

var nightly = jobsync.Declare[nightlyArgs]("test.nightly")

// A fleet fires a schedule once, not once per server. This is the property the
// Locker exists for, and the one whose failure mode is a nightly billing run
// going out three times.
func TestScheduleFiresOncePerFleet(t *testing.T) {
	store := memory.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A daily spec, overdue. Deliberately not "* * * * *": with a per-minute
	// schedule and a one-second window, a test that happens to straddle a minute
	// boundary sees a second, entirely legitimate fire and fails. The property
	// under test is "once per fleet", not "once per minute", so the spec is
	// chosen so that a correct scheduler cannot fire twice within the window.
	err := store.SaveSchedule(ctx, jobsync.Schedule{
		ID: "nightly", Kind: "test.nightly", Queue: "default",
		Payload: []byte(`{"tenant":42}`), Cron: "0 2 * * *", Timezone: "UTC",
		NextRun: time.Now().Add(-time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}

	var runs atomic.Int64
	var tenant atomic.Int64
	for range 3 {
		srv := jobsync.NewServer(store, jobsync.ServerConfig{
			Concurrency:      2,
			Lease:            5 * time.Second,
			PollInterval:     20 * time.Millisecond,
			ScheduleInterval: 20 * time.Millisecond,
		})
		nightly.Handle(srv, func(ctx context.Context, a nightlyArgs) error {
			runs.Add(1)
			tenant.Store(int64(a.Tenant))
			return nil
		})
		go srv.Run(ctx)
	}

	// Comfortably many scheduler ticks. The schedule is due on the first one;
	// every tick after that must find it already advanced.
	time.Sleep(time.Second)
	cancel()

	if n := runs.Load(); n != 1 {
		t.Fatalf("schedule fired %d times across a 3-server fleet, want 1", n)
	}
	if tenant.Load() != 42 {
		t.Errorf("handler saw tenant %d, want the scheduled payload's 42", tenant.Load())
	}

	// The schedule must have moved on, or it refires every tick forever.
	due, err := store.DueSchedules(context.Background(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 0 {
		t.Errorf("schedule is still due after firing: %+v", due)
	}
}

// A storage that stores schedules but cannot lock would fire once per server.
// Run must refuse rather than start and quietly multiply every recurring job.
func TestServerRefusesSchedulesWithoutLock(t *testing.T) {
	srv := jobsync.NewServer(schedulesWithoutLock{memory.New()}, jobsync.ServerConfig{
		PollInterval: 10 * time.Millisecond,
	})
	nightly.Handle(srv, func(context.Context, nightlyArgs) error { return nil })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	err := srv.Run(ctx)

	if err == nil {
		t.Fatal("Run started with schedules but no Locker; recurring jobs would fire once per server")
	}
	// Assert on the reason, not merely that something failed. The first version of
	// this test accepted any error and passed on "context deadline exceeded" —
	// i.e. it passed while Run happily started.
	if !strings.Contains(err.Error(), "Locker") {
		t.Fatalf("Run failed for the wrong reason: %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Run took %v to refuse; it must fail at startup, not after doing work", elapsed)
	}
	t.Logf("refused in %v: %v", time.Since(start).Round(time.Millisecond), err)
}

// schedulesWithoutLock keeps the memory driver's Schedules but is NOT a
// jobsync.Locker, which is the shape of a third-party driver that implemented
// one and not the other.
//
// The shadowing methods take deliberately wrong signatures. Embedding promotes
// Lock and Unlock, and redeclaring them with the RIGHT signature would still
// satisfy the interface — hiding a method in Go means breaking its shape.
type schedulesWithoutLock struct{ *memory.Storage }

func (schedulesWithoutLock) Lock(struct{})   {}
func (schedulesWithoutLock) Unlock(struct{}) {}

// Cron used to panic: NewClient discarded the Schedules type assertion with `_`,
// so c.schedules was nil for every driver that existed and SaveSchedule was
// called straight through it.
func TestCronRegistersAndValidates(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	c, err := jobsync.NewClient(store)
	if err != nil {
		t.Fatal(err)
	}

	if err := nightly.Cron(ctx, c, "nightly", "0 2 * * *", nightlyArgs{Tenant: 7}); err != nil {
		t.Fatalf("Cron: %v", err)
	}

	got, err := store.ListSchedules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d schedules, want 1", len(got))
	}
	if got[0].Cron != "0 2 * * *" || got[0].Kind != "test.nightly" {
		t.Errorf("stored %+v, want the registered spec and kind", got[0])
	}
	if got[0].NextRun.IsZero() || !got[0].NextRun.After(time.Now()) {
		t.Errorf("NextRun = %v, want a future time computed from the spec", got[0].NextRun)
	}

	// A bad spec is rejected at registration, not at the first fire — which would
	// otherwise be the middle of the night, in a log nobody is reading.
	if err := nightly.Cron(ctx, c, "bad", "99 * * * *", nightlyArgs{}); err == nil {
		t.Error("Cron accepted an invalid spec")
	}
	if err := nightly.Cron(ctx, c, "", "0 2 * * *", nightlyArgs{}); err == nil {
		t.Error("Cron accepted an empty id; a schedule needs a stable one across deploys")
	}
}

// A schedule stores its zone by name and every server in the fleet resolves that
// name itself. time.Local is named "Local", which loads successfully on every
// machine and means something different on each — so a fleet with mixed TZ would
// fire the same nightly job at several different hours, each server looking
// correct in isolation.
func TestCronRejectsLocalTimezone(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	c, err := jobsync.NewClient(store, jobsync.InLocation(time.Local))
	if err != nil {
		t.Fatal(err)
	}
	if err := nightly.Cron(ctx, c, "nightly", "0 2 * * *", nightlyArgs{}); err == nil {
		t.Fatal("Cron accepted time.Local; the stored zone would mean a different hour on every server")
	}

	// The default must be a real zone, not the process's local one.
	def, err := jobsync.NewClient(store)
	if err != nil {
		t.Fatal(err)
	}
	if err := nightly.Cron(ctx, def, "nightly", "0 2 * * *", nightlyArgs{}); err != nil {
		t.Fatalf("default client cannot register a schedule: %v", err)
	}
	got, err := store.ListSchedules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Timezone != "UTC" {
		t.Errorf("default timezone = %q, want UTC", got[0].Timezone)
	}
}
