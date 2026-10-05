package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/beesaferoot/jobsync"
)

// derivedState maps the two stored states onto the contract's seven, exactly as
// the Postgres driver does. UTC_TIMESTAMP(6) rather than NOW(6) so the answer
// does not depend on the server's session timezone.
const derivedState = `
	CASE
		WHEN state <> '` + stateAvailable + `'     THEN state
		WHEN scheduled_at <= UTC_TIMESTAMP(6)      THEN '` + string(jobsync.StateEnqueued) + `'
		WHEN attempt > 0                           THEN '` + string(jobsync.StateRetrying) + `'
		ELSE '` + string(jobsync.StateScheduled) + `'
	END`

func (s *Storage) Counts(ctx context.Context) (jobsync.Counts, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT "+derivedState+" AS st, COUNT(*) FROM jobsync_jobs GROUP BY st")
	if err != nil {
		return jobsync.Counts{}, err
	}
	defer rows.Close()

	var c jobsync.Counts
	for rows.Next() {
		var state string
		var n int64
		if err := rows.Scan(&state, &n); err != nil {
			return c, err
		}
		switch jobsync.State(state) {
		case jobsync.StateScheduled:
			c.Scheduled = n
		case jobsync.StateEnqueued:
			c.Enqueued = n
		case jobsync.StateRunning:
			c.Running = n
		case jobsync.StateRetrying:
			c.Retrying = n
		case jobsync.StateSucceeded:
			c.Succeeded = n
		case jobsync.StateDead:
			c.Dead = n
		case jobsync.StateCancelled:
			c.Cancelled = n
		}
	}
	return c, rows.Err()
}

// MySQL has no FILTER clause, so conditional aggregates are SUM(CASE ...).
func (s *Storage) QueueStats(ctx context.Context) ([]jobsync.QueueStat, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT t.queue,
		       SUM(CASE WHEN st = 'enqueued' THEN 1 ELSE 0 END),
		       SUM(CASE WHEN st = 'running'  THEN 1 ELSE 0 END),
		       COALESCE(MAX(CASE WHEN st = 'enqueued'
		                    THEN TIMESTAMPDIFF(MICROSECOND, scheduled_at, UTC_TIMESTAMP(6))
		                    END), 0),
		       MAX(p.name IS NOT NULL)
		FROM (SELECT queue, scheduled_at, `+derivedState+` AS st FROM jobsync_jobs) t
		LEFT JOIN jobsync_paused_queues p ON p.name = t.queue
		GROUP BY t.queue ORDER BY t.queue`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []jobsync.QueueStat
	for rows.Next() {
		var q jobsync.QueueStat
		var oldestMicros int64
		if err := rows.Scan(&q.Name, &q.Enqueued, &q.Running, &oldestMicros, &q.Paused); err != nil {
			return nil, err
		}
		q.OldestEnqueued = time.Duration(oldestMicros) * time.Microsecond
		out = append(out, q)
	}
	return out, rows.Err()
}

func (s *Storage) ListJobs(ctx context.Context, f jobsync.Filter) ([]*jobsync.Job, int64, error) {
	where, args := buildFilter(f)
	inner := "(SELECT " + derivedState + " AS st, jobsync_jobs.* FROM jobsync_jobs) t "

	var total int64
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+inner+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	limit := f.Limit
	if limit <= 0 {
		limit = 50
	}
	// created_at DESC, id makes the ordering total: without the id tiebreak two
	// jobs created in the same instant can swap between pages.
	rows, err := s.db.QueryContext(ctx,
		"SELECT "+selectColumns+", st FROM "+inner+where+" ORDER BY created_at DESC, id LIMIT ? OFFSET ?",
		append(args, limit, f.Offset)...)
	if err != nil {
		return nil, 0, err
	}
	jobs, err := scanJobsWithState(rows)
	return jobs, total, err
}

func buildFilter(f jobsync.Filter) (string, []any) {
	var clauses []string
	var args []any

	if len(f.States) > 0 {
		states := make([]any, len(f.States))
		for i, st := range f.States {
			states[i] = string(st)
		}
		clauses = append(clauses, "st IN ("+placeholders(len(states))+")")
		args = append(args, states...)
	}
	if len(f.Queues) > 0 {
		clauses = append(clauses, "queue IN ("+placeholders(len(f.Queues))+")")
		args = append(args, toAny(f.Queues)...)
	}
	if len(f.Kinds) > 0 {
		clauses = append(clauses, "kind IN ("+placeholders(len(f.Kinds))+")")
		args = append(args, toAny(f.Kinds)...)
	}
	for _, tag := range f.Tags {
		// JSON_CONTAINS wants a JSON document, not a bare string, so the tag is
		// quoted into one.
		clauses = append(clauses, "JSON_CONTAINS(tags, ?)")
		args = append(args, `"`+tag+`"`)
	}
	if !f.Since.IsZero() {
		clauses = append(clauses, "created_at >= ?")
		args = append(args, f.Since.UTC())
	}
	if !f.Until.IsZero() {
		clauses = append(clauses, "created_at <= ?")
		args = append(args, f.Until.UTC())
	}
	if f.Search != "" {
		// utf8mb4_0900_ai_ci is case-insensitive, so LIKE needs no lower().
		clauses = append(clauses, "(kind LIKE ? OR id LIKE ? OR last_error LIKE ?)")
		pattern := "%" + f.Search + "%"
		args = append(args, pattern, pattern, pattern)
	}
	if len(clauses) == 0 {
		return "", args
	}
	return "WHERE " + strings.Join(clauses, " AND "), args
}

func (s *Storage) JobHistory(ctx context.Context, id string) (*jobsync.Job, []jobsync.Transition, error) {
	row := s.db.QueryRowContext(ctx,
		"SELECT "+selectColumns+", st, transitions FROM (SELECT "+derivedState+" AS st, jobsync_jobs.* FROM jobsync_jobs) t WHERE id = ?", id)

	j := &jobsync.Job{}
	var (
		key, lastErr sql.NullString
		tags, raw    []byte
		leased, fin  sql.NullTime
		state        string
	)
	err := row.Scan(&j.ID, &j.Kind, &j.Queue, &j.Payload, &j.Priority, &j.Attempt,
		&j.MaxAttempts, &key, &tags, &lastErr, &j.CreatedAt, &j.ScheduledAt,
		&leased, &fin, &j.Owner, &state, &raw)
	if err == sql.ErrNoRows {
		return nil, nil, fmt.Errorf("jobsync: no job %s", id)
	}
	if err != nil {
		return nil, nil, err
	}

	j.UniqueKey, j.LastError = key.String, lastErr.String
	j.LeasedUntil, j.FinishedAt = leased.Time, fin.Time
	j.State = jobsync.State(state)
	if err := decodeTags(tags, j); err != nil {
		return nil, nil, err
	}

	transitions, err := decodeTransitions(raw)
	return j, transitions, err
}

type storedTransition struct {
	State  string `json:"state"`
	At     string `json:"at"`
	Reason string `json:"reason"`
}

func decodeTransitions(raw []byte) ([]jobsync.Transition, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var stored []storedTransition
	if err := json.Unmarshal(raw, &stored); err != nil {
		return nil, fmt.Errorf("decode job transitions: %w", err)
	}

	out := make([]jobsync.Transition, 0, len(stored))
	for _, st := range stored {
		// MySQL renders a DATETIME into JSON with a space separator and no zone.
		// The value was written by UTC_TIMESTAMP(6), so it is UTC.
		at, err := time.ParseInLocation("2006-01-02 15:04:05.999999", st.At, time.UTC)
		if err != nil {
			return nil, fmt.Errorf("decode transition timestamp %q: %w", st.At, err)
		}
		out = append(out, jobsync.Transition{
			State: jobsync.State(st.State), At: at, Reason: st.Reason,
		})
	}
	return out, nil
}

// Requeue skips running jobs: one is not stuck, it is working, and requeueing it
// would run the same work twice.
func (s *Storage) Requeue(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE jobsync_jobs SET
			state = '`+stateAvailable+`', attempt = 0, last_error = '',
			scheduled_at = DATE_SUB(UTC_TIMESTAMP(6), INTERVAL 1 SECOND),
			finished_at = NULL, owner = '', leased_until = NULL,
			transitions = JSON_ARRAY_APPEND(transitions, '$', JSON_OBJECT(
				'state', '`+string(jobsync.StateEnqueued)+`', 'at', UTC_TIMESTAMP(6),
				'reason', 'requeued from the dashboard'))
		WHERE state <> '`+stateRunning+`' AND id IN (`+placeholders(len(ids))+`)`,
		toAny(ids)...)
	return err
}

func (s *Storage) Delete(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := s.db.ExecContext(ctx,
		"DELETE FROM jobsync_jobs WHERE state <> '"+stateRunning+"' AND id IN ("+placeholders(len(ids))+")",
		toAny(ids)...)
	return err
}

func (s *Storage) Throughput(ctx context.Context, since time.Time, bucket time.Duration) ([]jobsync.Bucket, error) {
	if bucket <= 0 {
		return nil, fmt.Errorf("jobsync: throughput bucket must be positive")
	}
	seconds := int64(bucket.Seconds())
	if seconds < 1 {
		seconds = 1
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(finished_at) / ?) * ?) AS at,
		       SUM(CASE WHEN state = 'succeeded' THEN 1 ELSE 0 END),
		       SUM(CASE WHEN state IN ('dead','cancelled') THEN 1 ELSE 0 END)
		FROM jobsync_jobs
		WHERE finished_at IS NOT NULL AND finished_at >= ?
		GROUP BY at ORDER BY at`, seconds, seconds, since.UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []jobsync.Bucket{}
	for rows.Next() {
		var b jobsync.Bucket
		if err := rows.Scan(&b.At, &b.Succeeded, &b.Failed); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

func (s *Storage) Heartbeat(ctx context.Context, info jobsync.ServerInfo, ttl time.Duration) error {
	queues, err := json.Marshal(nonNilTags(info.Queues))
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO jobsync_servers (id, hostname, queues, concurrency, started_at, heartbeat_at, expires_at)
		VALUES (?,?,?,?,?, UTC_TIMESTAMP(6), DATE_ADD(UTC_TIMESTAMP(6), INTERVAL ? MICROSECOND))
		ON DUPLICATE KEY UPDATE
			queues = VALUES(queues), concurrency = VALUES(concurrency),
			heartbeat_at = VALUES(heartbeat_at), expires_at = VALUES(expires_at)`,
		info.ID, info.Hostname, queues, info.Concurrency, info.StartedAt.UTC(), ttl.Microseconds())
	return err
}

// Servers lists only unexpired rows, so a server that died mid-deploy stops
// appearing on its own without a janitor.
func (s *Storage) Servers(ctx context.Context) ([]jobsync.ServerInfo, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, hostname, queues, concurrency, started_at, heartbeat_at
		FROM jobsync_servers WHERE expires_at > UTC_TIMESTAMP(6) ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []jobsync.ServerInfo
	for rows.Next() {
		var info jobsync.ServerInfo
		var queues []byte
		err := rows.Scan(&info.ID, &info.Hostname, &queues, &info.Concurrency,
			&info.StartedAt, &info.HeartbeatAt)
		if err != nil {
			return nil, err
		}
		if len(queues) > 0 {
			if err := json.Unmarshal(queues, &info.Queues); err != nil {
				return nil, fmt.Errorf("decode server queues: %w", err)
			}
		}
		out = append(out, info)
	}
	return out, rows.Err()
}

func decodeTags(raw []byte, j *jobsync.Job) error {
	if len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, &j.Tags); err != nil {
		return fmt.Errorf("decode tags for job %s: %w", j.ID, err)
	}
	if len(j.Tags) == 0 {
		j.Tags = nil
	}
	return nil
}

func scanJobsWithState(rows *sql.Rows) ([]*jobsync.Job, error) {
	defer rows.Close()

	var out []*jobsync.Job
	for rows.Next() {
		j := &jobsync.Job{}
		var (
			key, lastErr sql.NullString
			tags         []byte
			leased, fin  sql.NullTime
			state        string
		)
		err := rows.Scan(&j.ID, &j.Kind, &j.Queue, &j.Payload, &j.Priority, &j.Attempt,
			&j.MaxAttempts, &key, &tags, &lastErr, &j.CreatedAt, &j.ScheduledAt,
			&leased, &fin, &j.Owner, &state)
		if err != nil {
			return nil, err
		}
		j.UniqueKey, j.LastError = key.String, lastErr.String
		j.LeasedUntil, j.FinishedAt = leased.Time, fin.Time
		j.State = jobsync.State(state)
		if err := decodeTags(tags, j); err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}
