package mysql

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
// next_run is deliberately not overwritten on a duplicate key. A running fleet
// has already advanced it, and letting a redeploy reset it would re-fire
// whatever had just run.
func (s *Storage) SaveSchedule(ctx context.Context, sc jobsync.Schedule) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO jobsync_schedules (`+scheduleColumns+`)
		VALUES (?,?,?,?,?,?,?,?,?)
		ON DUPLICATE KEY UPDATE
			kind = VALUES(kind), queue = VALUES(queue), payload = VALUES(payload),
			cron = VALUES(cron), timezone = VALUES(timezone), paused = VALUES(paused)`,
		sc.ID, sc.Kind, sc.Queue, sc.Payload, sc.Cron, sc.Timezone,
		nullTime(sc.LastRun), sc.NextRun.UTC(), sc.Paused)
	return err
}

func (s *Storage) ListSchedules(ctx context.Context) ([]jobsync.Schedule, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT "+scheduleColumns+" FROM jobsync_schedules ORDER BY id")
	if err != nil {
		return nil, err
	}
	return scanSchedules(rows)
}

func (s *Storage) DueSchedules(ctx context.Context, now time.Time) ([]jobsync.Schedule, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT "+scheduleColumns+" FROM jobsync_schedules WHERE paused = 0 AND next_run <= ? ORDER BY id",
		now.UTC())
	if err != nil {
		return nil, err
	}
	return scanSchedules(rows)
}

func (s *Storage) AdvanceSchedule(ctx context.Context, id string, lastRun, nextRun time.Time) error {
	_, err := s.db.ExecContext(ctx,
		"UPDATE jobsync_schedules SET last_run = ?, next_run = ? WHERE id = ?",
		lastRun.UTC(), nextRun.UTC(), id)
	return err
}

// RemoveSchedule is not an error when the schedule is already gone: two
// dashboards with the same page open will both send the delete.
func (s *Storage) RemoveSchedule(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM jobsync_schedules WHERE id = ?", id)
	return err
}

func scanSchedules(rows *sql.Rows) ([]jobsync.Schedule, error) {
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
	return t.UTC()
}

// SetSchedulePaused implements jobsync.Schedules. Not an error when the
// schedule is absent: two dashboards with the same page open will both send it.
func (s *Storage) SetSchedulePaused(ctx context.Context, id string, paused bool) error {
	_, err := s.db.ExecContext(ctx,
		"UPDATE jobsync_schedules SET paused = ? WHERE id = ?", paused, id)
	return err
}

// PauseQueue implements jobsync.QueueControl. The no-op ON DUPLICATE KEY UPDATE
// makes pausing an already-paused queue harmless.
func (s *Storage) PauseQueue(ctx context.Context, name string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO jobsync_paused_queues (name, paused_at) VALUES (?, UTC_TIMESTAMP(6))
		ON DUPLICATE KEY UPDATE name = name`, name)
	return err
}

func (s *Storage) ResumeQueue(ctx context.Context, name string) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM jobsync_paused_queues WHERE name = ?", name)
	return err
}

func (s *Storage) PausedQueues(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT name FROM jobsync_paused_queues ORDER BY name")
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
