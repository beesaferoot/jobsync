package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/beesaferoot/jobsync"
	"github.com/jackc/pgx/v5"
)

// derivedState maps the two stored states onto the contract's seven. Scheduled,
// Enqueued and Retrying are all `available` in the table — the difference is
// whether the job is due yet and whether it has already failed once. Keeping it
// as one expression means the list page, the counts and the queue stats can
// never disagree about what state a job is in.
const derivedState = `
	CASE
		WHEN state <> '` + stateAvailable + `' THEN state
		WHEN scheduled_at <= now()            THEN '` + string(jobsync.StateEnqueued) + `'
		WHEN attempt > 0                      THEN '` + string(jobsync.StateRetrying) + `'
		ELSE '` + string(jobsync.StateScheduled) + `'
	END`

func (s *Storage) Counts(ctx context.Context) (jobsync.Counts, error) {
	rows, err := s.pool.Query(ctx,
		"SELECT "+derivedState+" AS st, count(*) FROM jobsync_jobs GROUP BY st")
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

func (s *Storage) QueueStats(ctx context.Context) ([]jobsync.QueueStat, error) {
	// LEFT JOIN rather than a second query: an operator cannot tell a paused
	// queue from an idle one, and they look identical while meaning opposite
	// things, so the flag has to arrive with the counts.
	rows, err := s.pool.Query(ctx, `
		SELECT t.queue,
		       count(*) FILTER (WHERE st = 'enqueued'),
		       count(*) FILTER (WHERE st = 'running'),
		       COALESCE(EXTRACT(EPOCH FROM max(now() - scheduled_at)
		                FILTER (WHERE st = 'enqueued')), 0),
		       bool_or(p.name IS NOT NULL)
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
		var oldestSeconds float64
		if err := rows.Scan(&q.Name, &q.Enqueued, &q.Running, &oldestSeconds, &q.Paused); err != nil {
			return nil, err
		}
		q.OldestEnqueued = time.Duration(oldestSeconds * float64(time.Second))
		out = append(out, q)
	}
	return out, rows.Err()
}

func (s *Storage) ListJobs(ctx context.Context, f jobsync.Filter) ([]*jobsync.Job, int64, error) {
	where, args := buildFilter(f)

	var total int64
	err := s.pool.QueryRow(ctx,
		"SELECT count(*) FROM (SELECT "+derivedState+" AS st, * FROM jobsync_jobs) t "+where,
		args...).Scan(&total)
	if err != nil {
		return nil, 0, err
	}

	limit := f.Limit
	if limit <= 0 {
		limit = 50
	}
	// created_at DESC, id makes the ordering total. Without the id tiebreak two
	// jobs created in the same instant can swap between pages, so the pager skips
	// one and shows another twice.
	q := "SELECT " + selectColumns + ", st FROM (SELECT " + derivedState + " AS st, * FROM jobsync_jobs) t " +
		where + " ORDER BY created_at DESC, id LIMIT $" + strconv.Itoa(len(args)+1) +
		" OFFSET $" + strconv.Itoa(len(args)+2)
	args = append(args, limit, f.Offset)

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, 0, err
	}
	jobs, err := scanJobRows(rows, true)
	return jobs, total, err
}

// buildFilter renders the WHERE clause. Written by hand rather than with a query
// builder: there are six optional predicates and they never compose in anything
// but this one shape.
func buildFilter(f jobsync.Filter) (string, []any) {
	var clauses []string
	var args []any
	next := func(v any) string {
		args = append(args, v)
		return "$" + strconv.Itoa(len(args))
	}

	if len(f.States) > 0 {
		states := make([]string, len(f.States))
		for i, st := range f.States {
			states[i] = string(st)
		}
		clauses = append(clauses, "st = ANY("+next(states)+")")
	}
	if len(f.Queues) > 0 {
		clauses = append(clauses, "queue = ANY("+next(f.Queues)+")")
	}
	if len(f.Kinds) > 0 {
		clauses = append(clauses, "kind = ANY("+next(f.Kinds)+")")
	}
	if len(f.Tags) > 0 {
		clauses = append(clauses, "tags @> "+next(f.Tags))
	}
	if !f.Since.IsZero() {
		clauses = append(clauses, "created_at >= "+next(f.Since))
	}
	if !f.Until.IsZero() {
		clauses = append(clauses, "created_at <= "+next(f.Until))
	}
	if f.Search != "" {
		p := next("%" + f.Search + "%")
		clauses = append(clauses, "(kind ILIKE "+p+" OR id ILIKE "+p+" OR last_error ILIKE "+p+")")
	}
	if len(clauses) == 0 {
		return "", args
	}
	return "WHERE " + strings.Join(clauses, " AND "), args
}

func (s *Storage) JobHistory(ctx context.Context, id string) (*jobsync.Job, []jobsync.Transition, error) {
	var raw []byte
	rows, err := s.pool.Query(ctx,
		"SELECT "+selectColumns+", st, transitions FROM (SELECT "+derivedState+" AS st, * FROM jobsync_jobs) t WHERE id = $1", id)
	if err != nil {
		return nil, nil, err
	}

	var job *jobsync.Job
	for rows.Next() {
		j := &jobsync.Job{}
		var key *string
		var leased, finished *time.Time
		var state string
		dest := append(jobScanTargets(j, &key, &leased, &finished), &state, &raw)
		if err := rows.Scan(dest...); err != nil {
			rows.Close()
			return nil, nil, err
		}
		applyNullable(j, key, leased, finished)
		j.State = jobsync.State(state)
		job = j
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	if job == nil {
		return nil, nil, fmt.Errorf("jobsync: no job %s", id)
	}

	transitions, err := decodeTransitions(raw)
	return job, transitions, err
}

// storedTransition is the shape jsonb_build_object writes in Finish.
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
		// Postgres renders timestamptz into JSON without the T separator, so
		// RFC3339 alone does not parse it.
		at, err := time.Parse("2006-01-02T15:04:05.999999-07:00", st.At)
		if err != nil {
			at, err = time.Parse("2006-01-02 15:04:05.999999-07", st.At)
			if err != nil {
				return nil, fmt.Errorf("decode transition timestamp %q: %w", st.At, err)
			}
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
	_, err := s.pool.Exec(ctx, `
		UPDATE jobsync_jobs SET
			state = '`+stateAvailable+`', attempt = 0, last_error = '',
			scheduled_at = now() - interval '1 second',
			finished_at = NULL, owner = '', leased_until = NULL,
			transitions = transitions || jsonb_build_object(
				'state', '`+string(jobsync.StateEnqueued)+`', 'at', now(), 'reason', 'requeued from the dashboard')
		WHERE id = ANY($1) AND state <> '`+stateRunning+`'`, ids)
	return err
}

func (s *Storage) Delete(ctx context.Context, ids []string) error {
	_, err := s.pool.Exec(ctx,
		"DELETE FROM jobsync_jobs WHERE id = ANY($1) AND state <> '"+stateRunning+"'", ids)
	return err
}

func (s *Storage) Throughput(ctx context.Context, since time.Time, bucket time.Duration) ([]jobsync.Bucket, error) {
	if bucket <= 0 {
		return nil, fmt.Errorf("jobsync: throughput bucket must be positive")
	}
	seconds := bucket.Seconds()

	rows, err := s.pool.Query(ctx, `
		SELECT to_timestamp(floor(extract(epoch FROM finished_at) / $2) * $2) AS at,
		       count(*) FILTER (WHERE state = 'succeeded'),
		       count(*) FILTER (WHERE state IN ('dead','cancelled'))
		FROM jobsync_jobs
		WHERE finished_at IS NOT NULL AND finished_at >= $1
		GROUP BY at ORDER BY at`, since, seconds)
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
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jobsync_servers (id, hostname, queues, concurrency, started_at, heartbeat_at, expires_at)
		VALUES ($1,$2,$3,$4,$5, now(), now() + $6::interval)
		ON CONFLICT (id) DO UPDATE SET
			queues = EXCLUDED.queues, concurrency = EXCLUDED.concurrency,
			heartbeat_at = now(), expires_at = EXCLUDED.expires_at`,
		info.ID, info.Hostname, info.Queues, info.Concurrency, info.StartedAt, ttl)
	return err
}

// Servers lists only unexpired rows. A server that died mid-deploy stops
// appearing on its own, so the page does not need a janitor to stay honest.
func (s *Storage) Servers(ctx context.Context) ([]jobsync.ServerInfo, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, hostname, queues, concurrency, started_at, heartbeat_at
		FROM jobsync_servers WHERE expires_at > now() ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []jobsync.ServerInfo
	for rows.Next() {
		var info jobsync.ServerInfo
		err := rows.Scan(&info.ID, &info.Hostname, &info.Queues, &info.Concurrency,
			&info.StartedAt, &info.HeartbeatAt)
		if err != nil {
			return nil, err
		}
		out = append(out, info)
	}
	return out, rows.Err()
}

// jobScanTargets matches selectColumns. leased_until and finished_at are
// nullable — a job that is not running holds no lease, and one that has not
// finished has no finish time — so they scan through pointers and settle to the
// zero time, which is what Job documents those fields to mean when absent.
func jobScanTargets(j *jobsync.Job, key **string, leased, finished **time.Time) []any {
	return []any{&j.ID, &j.Kind, &j.Queue, &j.Payload, &j.Priority, &j.Attempt,
		&j.MaxAttempts, key, &j.Tags, &j.LastError, &j.CreatedAt, &j.ScheduledAt,
		leased, finished, &j.Owner}
}

func applyNullable(j *jobsync.Job, key *string, leased, finished *time.Time) {
	if key != nil {
		j.UniqueKey = *key
	}
	if leased != nil {
		j.LeasedUntil = *leased
	}
	if finished != nil {
		j.FinishedAt = *finished
	}
}

// scanJobRows reads the standard column list. withState consumes a trailing
// derived-state column.
func scanJobRows(rows pgx.Rows, withState bool) ([]*jobsync.Job, error) {
	defer rows.Close()

	var out []*jobsync.Job
	for rows.Next() {
		j := &jobsync.Job{}
		var key *string
		var leased, finished *time.Time
		var state string
		dest := jobScanTargets(j, &key, &leased, &finished)
		if withState {
			dest = append(dest, &state)
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		applyNullable(j, key, leased, finished)
		if withState {
			j.State = jobsync.State(state)
		}
		out = append(out, j)
	}
	return out, rows.Err()
}
