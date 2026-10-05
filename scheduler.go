package jobsync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"
)

// scheduler fires recurring jobs. Exactly one server in a fleet fires any given
// schedule tick, which is what the Locker is for.
type scheduler struct {
	srv       *Server
	schedules Schedules
	locker    Locker
}

// newScheduler returns nil when the driver stores no schedules, which is not an
// error: a storage without Schedules runs one-off jobs perfectly well.
//
// It DOES error when a driver stores schedules but offers no lock. Firing
// without one means every server in the fleet enqueues the same nightly job, and
// a billing run that goes out once per replica is worse than one that does not
// go out at all.
func newScheduler(srv *Server) (*scheduler, error) {
	schedules, ok := srv.store.(Schedules)
	if !ok {
		return nil, nil
	}
	locker, ok := srv.store.(Locker)
	if !ok {
		return nil, fmt.Errorf("jobsync: %T stores schedules but is not a Locker, so recurring jobs would fire once per server", srv.store)
	}
	return &scheduler{srv: srv, schedules: schedules, locker: locker}, nil
}

func (s *scheduler) run(ctx context.Context) {
	for sleep(ctx, s.srv.cfg.ScheduleInterval) {
		if err := s.tick(ctx, time.Now()); err != nil && ctx.Err() == nil {
			s.srv.log.Error("jobsync: scheduler tick failed", "err", err)
		}
	}
}

func (s *scheduler) tick(ctx context.Context, now time.Time) error {
	due, err := s.schedules.DueSchedules(ctx, now)
	if err != nil {
		return err
	}
	for _, sc := range due {
		if err := s.fire(ctx, sc, now); err != nil {
			// One bad schedule must not stop the others: a typo'd cron spec in a
			// config file should break its own job, not the whole scheduler.
			s.srv.log.Error("jobsync: schedule failed", "schedule", sc.ID, "err", err)
		}
	}
	return nil
}

func (s *scheduler) fire(ctx context.Context, sc Schedule, now time.Time) error {
	loc, err := time.LoadLocation(sc.Timezone)
	if err != nil {
		// Not falling back to UTC: a schedule written for Africa/Lagos that
		// silently runs in UTC fires at the wrong hour every day, and nobody
		// notices until someone reads the logs.
		return fmt.Errorf("unknown timezone %q: %w", sc.Timezone, err)
	}

	// The next fire time is computed from now, not from sc.NextRun. A server that
	// was down for an hour owes one run of a per-minute job, not sixty.
	next, err := nextRun(sc.Cron, loc, now)
	if err != nil {
		return err
	}

	// Hold the lock across enqueue and advance together. Between the two the
	// schedule is still due, so a second server taking the lock here would
	// enqueue the same tick again.
	key := "schedule:" + sc.ID
	token, err := s.locker.Lock(ctx, key, s.lockTTL())
	if err != nil {
		return err
	}
	if token == "" {
		return nil // another server is firing this tick
	}
	defer s.locker.Unlock(ctx, key, token)

	job := &Job{
		ID:          scheduledJobID(sc.ID, sc.NextRun),
		Kind:        sc.Kind,
		Queue:       sc.Queue,
		Payload:     sc.Payload,
		MaxAttempts: s.srv.cfg.ScheduleMaxAttempts,
		State:       StateEnqueued,
		CreatedAt:   now,
		ScheduledAt: sc.NextRun,
		Tags:        []string{"schedule:" + sc.ID},
	}
	if err := s.srv.store.Enqueue(ctx, []*Job{job}); err != nil {
		return err
	}
	return s.schedules.AdvanceSchedule(ctx, sc.ID, sc.NextRun, next)
}

func (s *scheduler) lockTTL() time.Duration {
	ttl := s.srv.cfg.ScheduleInterval * 2
	if ttl < 5*time.Second {
		ttl = 5 * time.Second
	}
	return ttl
}

// scheduledJobID is deterministic in (schedule, tick). If a server crashes
// between Enqueue and AdvanceSchedule, the next server to take the lock fires
// the same tick again — and the duplicate insert is dropped on the primary key
// rather than running the job twice. That turns the gap between those two calls
// from a correctness bug into a no-op.
//
// It is hashed rather than spelled out because MySQL stores job ids in
// VARCHAR(64) and a schedule id is caller-chosen, so the readable form has no
// bound.
func scheduledJobID(scheduleID string, tick time.Time) string {
	sum := sha256.Sum256([]byte(scheduleID + ":" + strconv.FormatInt(tick.UnixMilli(), 10)))
	return hex.EncodeToString(sum[:16])
}
