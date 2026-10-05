package postgres

import (
	"context"
	"database/sql"
	"time"

	"github.com/beesaferoot/jobsync"
)

const scheduleColumns = `id, kind, queue, payload, cron, timezone, last_run, next_run, paused`

// SaveSchedule implements jobsync.Schedules. An upsert, not an insert: every
// deploy re-registers its schedules, and appending would fire a job once per
// deploy that had ever happened.
//
// next_run is deliberately NOT overwritten on conflict. A running fleet has
// already advanced it; letting a redeploy reset it would re-fire whatever had
// just run. The spec is updated, and the next advance picks up the new one.
func (s *Storage) SaveSchedule(ctx context.Context, sc jobsync.Schedule) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jobsync_schedules (`+scheduleColumns+`)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT (id) DO UPDATE SET
			kind = EXCLUDED.kind, queue = EXCLUDED.queue, payload = EXCLUDED.payload,
			cron = EXCLUDED.cron, timezone = EXCLUDED.timezone, paused = EXCLUDED.paused`,
		sc.ID, sc.Kind, sc.Queue, sc.Payload, sc.Cron, sc.Timezone,
		nullTime(sc.LastRun), sc.NextRun, sc.Paused)
	return err
}

func (s *Storage) ListSchedules(ctx context.Context) ([]jobsync.Schedule, error) {
	rows, err := s.pool.Query(ctx,
		"SELECT "+scheduleColumns+" FROM jobsync_schedules ORDER BY id")
	if err != nil {
		return nil, err
	}
	return scanSchedules(rows)
}

func (s *Storage) DueSchedules(ctx context.Context, now time.Time) ([]jobsync.Schedule, error) {
	rows, err := s.pool.Query(ctx,
		"SELECT "+scheduleColumns+" FROM jobsync_schedules WHERE NOT paused AND next_run <= $1 ORDER BY id", now)
	if err != nil {
		return nil, err
	}
	return scanSchedules(rows)
}

func (s *Storage) AdvanceSchedule(ctx context.Context, id string, lastRun, nextRun time.Time) error {
	_, err := s.pool.Exec(ctx,
		"UPDATE jobsync_schedules SET last_run = $2, next_run = $3 WHERE id = $1",
		id, lastRun, nextRun)
	return err
}

// RemoveSchedule is not an error when the schedule is already gone: two
// dashboards with the same page open will both send the delete.
func (s *Storage) RemoveSchedule(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, "DELETE FROM jobsync_schedules WHERE id = $1", id)
	return err
}

func scanSchedules(rows interface {
	Next() bool
	Scan(...any) error
	Err() error
	Close()
}) ([]jobsync.Schedule, error) {
	defer rows.Close()

	var out []jobsync.Schedule
	for rows.Next() {
		var sc jobsync.Schedule
		var lastRun sql.NullTime
		err := rows.Scan(&sc.ID, &sc.Kind, &sc.Queue, &sc.Payload, &sc.Cron,
			&sc.Timezone, &lastRun, &sc.NextRun, &sc.Paused)
		if err != nil {
			return nil, err
		}
		sc.LastRun = lastRun.Time
		out = append(out, sc)
	}
	return out, rows.Err()
}

func nullTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

// SetSchedulePaused implements jobsync.Schedules. Not an error when the
// schedule is absent: two dashboards with the same page open will both send it.
func (s *Storage) SetSchedulePaused(ctx context.Context, id string, paused bool) error {
	_, err := s.pool.Exec(ctx,
		"UPDATE jobsync_schedules SET paused = $2 WHERE id = $1", id, paused)
	return err
}

// PauseQueue implements jobsync.QueueControl. ON CONFLICT DO NOTHING so pausing
// an already-paused queue is a no-op rather than an error.
func (s *Storage) PauseQueue(ctx context.Context, name string) error {
	_, err := s.pool.Exec(ctx,
		"INSERT INTO jobsync_paused_queues (name) VALUES ($1) ON CONFLICT DO NOTHING", name)
	return err
}

func (s *Storage) ResumeQueue(ctx context.Context, name string) error {
	_, err := s.pool.Exec(ctx, "DELETE FROM jobsync_paused_queues WHERE name = $1", name)
	return err
}

func (s *Storage) PausedQueues(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, "SELECT name FROM jobsync_paused_queues ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []string{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	return out, rows.Err()
}
