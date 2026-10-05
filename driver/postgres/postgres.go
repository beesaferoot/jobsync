// Package postgres is the reference jobsync storage driver.
//
// It is the reference in a specific sense: where the Storage contract is
// ambiguous, what this driver does is what the contract means. The other drivers
// are checked against the same conformance suite, not against this code.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/beesaferoot/jobsync"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Stored states. Scheduled and Enqueued collapse into one row state: a job is
// available, subject to scheduled_at. The contract's distinction is derived at
// read time, which is what lets this driver skip the scheduled-to-enqueued
// promotion sweeper that Hangfire and Sidekiq both need. There is no background
// task moving due jobs anywhere — the fetch query simply starts matching them.
const (
	stateAvailable = "available"
	stateRunning   = "running"
)

type Storage struct {
	pool *pgxpool.Pool
	// Retention is how long terminal jobs are kept for the dashboard before the
	// janitor deletes them. This table is high-churn; without a bound, both
	// autovacuum and the dashboard degrade.
	Retention time.Duration
}

// Open connects, migrates and returns a ready storage.
func Open(ctx context.Context, url string) (*Storage, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		pool.Close()
		return nil, err
	}
	err = Migrate(ctx, conn.Conn())
	conn.Release()
	if err != nil {
		pool.Close()
		return nil, err
	}
	return &Storage{pool: pool, Retention: 7 * 24 * time.Hour}, nil
}

func (s *Storage) Close() error {
	s.pool.Close()
	return nil
}

const insertJob = `
INSERT INTO jobsync_jobs (
	id, kind, queue, payload, priority, attempt, max_attempts,
	state, unique_key, tags, last_error, created_at, scheduled_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'',$11,$12)
ON CONFLICT DO NOTHING`

func (s *Storage) Enqueue(ctx context.Context, jobs []*jobsync.Job) error {
	batch := &pgx.Batch{}
	for _, j := range jobs {
		batch.Queue(insertJob, insertArgs(j)...)
	}
	return s.pool.SendBatch(ctx, batch).Close()
}

// EnqueueTx inserts inside the caller's transaction, so a job and the rows that
// justify it commit together. Implements jobsync.TxEnqueuer.
func (s *Storage) EnqueueTx(ctx context.Context, tx jobsync.Tx, jobs []*jobsync.Job) error {
	for _, j := range jobs {
		if _, err := tx.ExecContext(ctx, insertJob, insertArgs(j)...); err != nil {
			return err
		}
	}
	return nil
}

func insertArgs(j *jobsync.Job) []any {
	// A duplicate unique_key must be dropped silently, which ON CONFLICT DO
	// NOTHING handles via the partial unique index. NULL rather than '' is what
	// makes that index ignore jobs with no key at all.
	var key any
	if j.UniqueKey != "" {
		key = j.UniqueKey
	}
	tags := j.Tags
	if tags == nil {
		tags = []string{}
	}
	return []any{
		j.ID, j.Kind, j.Queue, j.Payload, j.Priority, j.Attempt, j.MaxAttempts,
		stateAvailable, key, tags, j.CreatedAt, j.ScheduledAt,
	}
}

// Fetch claims in one statement. SKIP LOCKED is what makes this safe for many
// servers at once: a row another transaction already locked is passed over
// instead of blocking, so N servers polling do not serialise behind each other.
const selectColumns = `id, kind, queue, payload, priority, attempt, max_attempts,
	unique_key, tags, last_error, created_at, scheduled_at, leased_until, finished_at, owner`

const fetchJobs = `
UPDATE jobsync_jobs SET
	state        = '` + stateRunning + `',
	owner        = $3,
	leased_until = now() + $4::interval
WHERE id IN (
	SELECT id FROM jobsync_jobs
	WHERE state = '` + stateAvailable + `'
	  AND queue = ANY($1)
	  AND scheduled_at <= now()
	  AND queue NOT IN (SELECT name FROM jobsync_paused_queues)
	ORDER BY priority, scheduled_at
	LIMIT $2
	FOR UPDATE SKIP LOCKED
)
RETURNING id, kind, queue, payload, priority, attempt, max_attempts,
          unique_key, tags, last_error, created_at, scheduled_at, leased_until, finished_at, owner`

func (s *Storage) Fetch(ctx context.Context, queues []string, n int, owner string, lease time.Duration) ([]*jobsync.Job, error) {
	rows, err := s.pool.Query(ctx, fetchJobs, queues, n, owner, lease)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []*jobsync.Job
	for rows.Next() {
		j := &jobsync.Job{State: jobsync.StateRunning}
		var key *string
		var leased, finished *time.Time
		err := rows.Scan(&j.ID, &j.Kind, &j.Queue, &j.Payload, &j.Priority, &j.Attempt,
			&j.MaxAttempts, &key, &j.Tags, &j.LastError, &j.CreatedAt, &j.ScheduledAt,
			&leased, &finished, &j.Owner)
		if err != nil {
			return nil, err
		}
		if key != nil {
			j.UniqueKey = *key
		}
		if leased != nil {
			j.LeasedUntil = *leased
		}
		if finished != nil {
			j.FinishedAt = *finished
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// RETURNING does not honour the subquery's ORDER BY, so the contract's
	// priority order has to be restored here rather than assumed.
	sortByPriority(jobs)
	return jobs, nil
}

func (s *Storage) Extend(ctx context.Context, ids []string, owner string, lease time.Duration) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE jobsync_jobs SET leased_until = now() + $3::interval
		WHERE id = ANY($1) AND owner = $2 AND state = '`+stateRunning+`'`,
		ids, owner, lease)
	return err
}

func (s *Storage) Finish(ctx context.Context, id, owner string, r jobsync.Result) error {
	// Every branch carries `AND owner = $2 AND state = 'running'`. Zero rows
	// affected means this server's lease was already reclaimed and the job may be
	// running elsewhere — the contract says drop the result, so no error.
	if r.State == jobsync.StateRetrying {
		_, err := s.pool.Exec(ctx, `
			UPDATE jobsync_jobs SET
				state        = '`+stateAvailable+`',
				attempt      = attempt + 1,
				scheduled_at = $3,
				last_error   = $4,
				owner        = '',
				leased_until = NULL,
				transitions  = transitions || jsonb_build_object(
					'state', 'retrying', 'at', now(), 'reason', $4::text)
			WHERE id = $1 AND owner = $2 AND state = '`+stateRunning+`'`,
			id, owner, r.RetryAt, r.Err)
		return err
	}

	// Terminal. unique_key goes to NULL so the same work can be requested again:
	// without this a job keyed report:2026-10-05 blocks its own re-run forever.
	_, err := s.pool.Exec(ctx, `
		UPDATE jobsync_jobs SET
			state        = $3,
			last_error   = $4,
			unique_key   = NULL,
			owner        = '',
			leased_until = NULL,
			finished_at  = now(),
			transitions  = transitions || jsonb_build_object(
				'state', $3::text, 'at', now(), 'reason', $4::text)
		WHERE id = $1 AND owner = $2 AND state = '`+stateRunning+`'`,
		id, owner, string(r.State), r.Err)
	return err
}

func (s *Storage) Reclaim(ctx context.Context) (int, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobsync_jobs SET state = '`+stateAvailable+`', owner = '', leased_until = NULL
		WHERE state = '`+stateRunning+`' AND leased_until < now()`)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

// Lock implements jobsync.Locker. A locks table rather than pg_advisory_lock:
// advisory locks are session-scoped with no TTL, so a dropped connection
// releases them silently, and the pattern does not port to MySQL.
func (s *Storage) Lock(ctx context.Context, key string, ttl time.Duration) (string, error) {
	token := newToken()
	var got string
	err := s.pool.QueryRow(ctx, `
		INSERT INTO jobsync_locks (key, token, expires_at)
		VALUES ($1, $2, now() + $3::interval)
		ON CONFLICT (key) DO UPDATE
			SET token = EXCLUDED.token, expires_at = EXCLUDED.expires_at
			WHERE jobsync_locks.expires_at < now()
		RETURNING token`, key, token, ttl).Scan(&got)

	// No row returned: the DO UPDATE's WHERE excluded it, so the lock is held and
	// unexpired. That is the ordinary outcome, not a failure.
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return got, nil
}

func (s *Storage) Unlock(ctx context.Context, key, token string) error {
	// The token check is what stops a server whose lock already expired from
	// releasing the lock a different server now holds.
	_, err := s.pool.Exec(ctx,
		"DELETE FROM jobsync_locks WHERE key = $1 AND token = $2", key, token)
	return err
}

// Janitor deletes terminal jobs older than Retention until ctx is cancelled.
func (s *Storage) Janitor(ctx context.Context, every time.Duration) error {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			_, err := s.pool.Exec(ctx, `
				DELETE FROM jobsync_jobs
				WHERE state IN ('succeeded','dead','cancelled')
				  AND created_at < now() - $1::interval`, s.Retention)
			if err != nil {
				return fmt.Errorf("jobsync: janitor: %w", err)
			}
		}
	}
}

// Truncate empties every table. For tests only; it is exported because the
// conformance suite needs a clean slate per subtest and a driver cannot offer
// that through the Storage contract.
func (s *Storage) Truncate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, "TRUNCATE jobsync_jobs, jobsync_locks, jobsync_schedules, jobsync_servers, jobsync_paused_queues")
	return err
}
