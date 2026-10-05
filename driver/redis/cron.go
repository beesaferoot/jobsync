package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/beesaferoot/jobsync"
)

// Schedules live in one hash, id -> JSON. Unlike jobs there is no index and no
// sorted set: DueSchedules reads them all and filters in Go.
//
// That is a deliberate choice, not a shortcut. Schedules are a configuration-
// sized collection — tens, occasionally hundreds, written at deploy and read
// once per scheduler tick — so a secondary index would cost write complexity to
// optimise a read that is already trivial. Jobs get indexes because jobs are
// unbounded; schedules are not.
func (s *Storage) schedulesKey() string { return s.prefix + ":schedules" }

// storedSchedule is the on-disk shape. Times are Unix milliseconds rather than
// RFC 3339 so that a schedule written by one process and read by another cannot
// disagree about zone formatting.
type storedSchedule struct {
	ID       string `json:"id"`
	Kind     string `json:"kind"`
	Queue    string `json:"queue"`
	Payload  []byte `json:"payload"`
	Cron     string `json:"cron"`
	Timezone string `json:"timezone"`
	LastRun  int64  `json:"last_run_ms"`
	NextRun  int64  `json:"next_run_ms"`
	Paused   bool   `json:"paused"`
}

// SaveSchedule implements jobsync.Schedules. An upsert: every deploy
// re-registers its schedules, and appending would fire a job once per deploy
// that had ever happened.
//
// next_run is preserved when the schedule already exists. A running fleet has
// already advanced it, and letting a redeploy reset it would re-fire whatever
// had just run.
func (s *Storage) SaveSchedule(ctx context.Context, sc jobsync.Schedule) error {
	existing, err := s.rdb.HGet(ctx, s.schedulesKey(), sc.ID).Bytes()
	if err == nil {
		var prev storedSchedule
		if err := json.Unmarshal(existing, &prev); err == nil {
			sc.NextRun = time.UnixMilli(prev.NextRun)
			sc.LastRun = time.UnixMilli(prev.LastRun)
			if prev.LastRun == 0 {
				sc.LastRun = time.Time{}
			}
		}
	}

	blob, err := json.Marshal(storedSchedule{
		ID: sc.ID, Kind: sc.Kind, Queue: sc.Queue, Payload: sc.Payload,
		Cron: sc.Cron, Timezone: sc.Timezone, Paused: sc.Paused,
		LastRun: millisOrZero(sc.LastRun), NextRun: millisOrZero(sc.NextRun),
	})
	if err != nil {
		return fmt.Errorf("encode schedule %s: %w", sc.ID, err)
	}
	return s.rdb.HSet(ctx, s.schedulesKey(), sc.ID, blob).Err()
}

func (s *Storage) ListSchedules(ctx context.Context) ([]jobsync.Schedule, error) {
	return s.loadSchedules(ctx, func(jobsync.Schedule) bool { return true })
}

func (s *Storage) DueSchedules(ctx context.Context, now time.Time) ([]jobsync.Schedule, error) {
	return s.loadSchedules(ctx, func(sc jobsync.Schedule) bool {
		return !sc.Paused && !sc.NextRun.After(now)
	})
}

func (s *Storage) loadSchedules(ctx context.Context, keep func(jobsync.Schedule) bool) ([]jobsync.Schedule, error) {
	all, err := s.rdb.HGetAll(ctx, s.schedulesKey()).Result()
	if err != nil {
		return nil, err
	}

	var out []jobsync.Schedule
	for id, blob := range all {
		var stored storedSchedule
		if err := json.Unmarshal([]byte(blob), &stored); err != nil {
			return nil, fmt.Errorf("decode schedule %s: %w", id, err)
		}
		sc := jobsync.Schedule{
			ID: stored.ID, Kind: stored.Kind, Queue: stored.Queue,
			Payload: stored.Payload, Cron: stored.Cron, Timezone: stored.Timezone,
			Paused:  stored.Paused,
			NextRun: time.UnixMilli(stored.NextRun),
		}
		if stored.LastRun != 0 {
			sc.LastRun = time.UnixMilli(stored.LastRun)
		}
		if keep(sc) {
			out = append(out, sc)
		}
	}
	slices.SortFunc(out, func(a, b jobsync.Schedule) int {
		return slices.Compare([]string{a.ID}, []string{b.ID})
	})
	return out, nil
}

func (s *Storage) AdvanceSchedule(ctx context.Context, id string, lastRun, nextRun time.Time) error {
	blob, err := s.rdb.HGet(ctx, s.schedulesKey(), id).Bytes()
	if err != nil {
		return nil // already removed; nothing to advance
	}
	var stored storedSchedule
	if err := json.Unmarshal(blob, &stored); err != nil {
		return fmt.Errorf("decode schedule %s: %w", id, err)
	}
	stored.LastRun, stored.NextRun = millisOrZero(lastRun), millisOrZero(nextRun)

	updated, err := json.Marshal(stored)
	if err != nil {
		return err
	}
	return s.rdb.HSet(ctx, s.schedulesKey(), id, updated).Err()
}

// RemoveSchedule is not an error when the schedule is already gone: two
// dashboards with the same page open will both send the delete.
func (s *Storage) RemoveSchedule(ctx context.Context, id string) error {
	return s.rdb.HDel(ctx, s.schedulesKey(), id).Err()
}

func millisOrZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

// SetSchedulePaused implements jobsync.Schedules. Not an error when the
// schedule is absent: two dashboards with the same page open will both send it.
func (s *Storage) SetSchedulePaused(ctx context.Context, id string, paused bool) error {
	blob, err := s.rdb.HGet(ctx, s.schedulesKey(), id).Bytes()
	if err != nil {
		return nil
	}
	var stored storedSchedule
	if err := json.Unmarshal(blob, &stored); err != nil {
		return fmt.Errorf("decode schedule %s: %w", id, err)
	}
	stored.Paused = paused

	updated, err := json.Marshal(stored)
	if err != nil {
		return err
	}
	return s.rdb.HSet(ctx, s.schedulesKey(), id, updated).Err()
}

func (s *Storage) pausedKey() string { return s.prefix + ":paused" }

// PauseQueue implements jobsync.QueueControl. SADD is idempotent, so pausing an
// already-paused queue is harmless.
func (s *Storage) PauseQueue(ctx context.Context, name string) error {
	return s.rdb.SAdd(ctx, s.pausedKey(), name).Err()
}

func (s *Storage) ResumeQueue(ctx context.Context, name string) error {
	return s.rdb.SRem(ctx, s.pausedKey(), name).Err()
}

func (s *Storage) PausedQueues(ctx context.Context) ([]string, error) {
	out, err := s.rdb.SMembers(ctx, s.pausedKey()).Result()
	if err != nil {
		return nil, err
	}
	slices.Sort(out)
	return out, nil
}
