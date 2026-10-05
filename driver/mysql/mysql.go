// Package mysql is the MySQL/MariaDB jobsync storage driver.
//
// It is a port of the Postgres driver, and where the two differ it is because
// MySQL cannot express what Postgres does:
//
//   - No RETURNING, so Fetch is a three-statement transaction instead of one
//     statement.
//   - No partial indexes, so the fetch index carries every row rather than only
//     the available ones.
//   - No TIMESTAMPTZ. Every timestamp is a DATETIME(6) holding UTC, and every
//     query uses UTC_TIMESTAMP(6) rather than NOW(6) — see the note on Open.
//
// Minimum server: MySQL 8.0 or MariaDB 10.6, for SELECT ... FOR UPDATE SKIP
// LOCKED. Open refuses anything older.
package mysql

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/beesaferoot/jobsync"
	"github.com/go-sql-driver/mysql"
)

// Stored states, as in the Postgres driver: scheduled and enqueued collapse into
// one row state and the contract's distinction is derived at read time. No
// promotion sweeper.
const (
	stateAvailable = "available"
	stateRunning   = "running"
)

const duplicateEntry = 1062

type Storage struct {
	db *sql.DB
	// Retention is how long terminal jobs are kept for the dashboard before
	// Janitor deletes them.
	Retention time.Duration
}

// Open connects, verifies the server version, migrates and returns a ready
// storage.
//
// The DSN must carry parseTime=true and loc=UTC; Open adds them if missing.
// Without parseTime the driver hands back DATETIME as []byte; without loc=UTC it
// interprets stored values in the process's local zone, which silently shifts
// every scheduled job. The queries do not depend on this — they use
// UTC_TIMESTAMP(6), not NOW(6), so the server's own timezone is irrelevant — but
// the scan path does.
func Open(ctx context.Context, dsn string) (*Storage, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	cfg.ParseTime = true
	cfg.Loc = time.UTC

	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, err
	}
	if err := Migrate(ctx, db); err != nil {
		db.Close()
		return nil, err
	}
	return &Storage{db: db, Retention: 7 * 24 * time.Hour}, nil
}

func (s *Storage) Close() error { return s.db.Close() }

// ON DUPLICATE KEY UPDATE id = id is a deliberate no-op: a job whose unique_key
// is already held must be dropped silently. INSERT IGNORE would do the same but
// also swallow truncation and conversion errors, turning a corrupted payload
// into a success.
const insertJob = `
INSERT INTO jobsync_jobs (
	id, kind, queue, payload, priority, attempt, max_attempts,
	state, unique_key, tags, last_error, transitions, created_at, scheduled_at
) VALUES (?,?,?,?,?,?,?,?,?,?,'','[]',?,?)
ON DUPLICATE KEY UPDATE id = id`

func (s *Storage) Enqueue(ctx context.Context, jobs []*jobsync.Job) error {
	for _, j := range jobs {
		args, err := insertArgs(j)
		if err != nil {
			return err
		}
		if _, err := s.db.ExecContext(ctx, insertJob, args...); err != nil {
			return err
		}
	}
	return nil
}

// EnqueueTx implements jobsync.TxEnqueuer.
func (s *Storage) EnqueueTx(ctx context.Context, tx jobsync.Tx, jobs []*jobsync.Job) error {
	for _, j := range jobs {
		args, err := insertArgs(j)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, insertJob, args...); err != nil {
			return err
		}
	}
	return nil
}

func insertArgs(j *jobsync.Job) ([]any, error) {
	tags, err := json.Marshal(nonNilTags(j.Tags))
	if err != nil {
		return nil, fmt.Errorf("encode tags for job %s: %w", j.ID, err)
	}
	// NULL rather than '': the unique index must ignore jobs with no key, and
	// MySQL permits any number of NULLs in a unique index.
	var key any
	if j.UniqueKey != "" {
		key = j.UniqueKey
	}
	return []any{
		j.ID, j.Kind, j.Queue, j.Payload, j.Priority, j.Attempt, j.MaxAttempts,
		stateAvailable, key, tags, j.CreatedAt.UTC(), j.ScheduledAt.UTC(),
	}, nil
}

const selectColumns = `id, kind, queue, payload, priority, attempt, max_attempts,
	unique_key, tags, last_error, created_at, scheduled_at, leased_until, finished_at, owner`

// Fetch is three statements in one transaction, because MySQL has no RETURNING.
// The SELECT ... FOR UPDATE SKIP LOCKED holds the claim for the life of the
// transaction, so the UPDATE cannot lose a row to a concurrent server between
// the two statements.
func (s *Storage) Fetch(ctx context.Context, queues []string, n int, owner string, lease time.Duration) ([]*jobsync.Job, error) {
	if len(queues) == 0 || n <= 0 {
		return nil, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	args := append(toAny(queues), n)
	rows, err := tx.QueryContext(ctx, `
		SELECT id FROM jobsync_jobs
		WHERE state = '`+stateAvailable+`'
		  AND queue IN (`+placeholders(len(queues))+`)
		  AND scheduled_at <= UTC_TIMESTAMP(6)
		  AND queue NOT IN (SELECT name FROM jobsync_paused_queues)
		ORDER BY priority, scheduled_at
		LIMIT ?
		FOR UPDATE SKIP LOCKED`, args...)
	if err != nil {
		return nil, err
	}

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, tx.Commit()
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE jobsync_jobs SET
			state        = '`+stateRunning+`',
			owner        = ?,
			leased_until = DATE_ADD(UTC_TIMESTAMP(6), INTERVAL ? MICROSECOND)
		WHERE id IN (`+placeholders(len(ids))+`)`,
		append([]any{owner, lease.Microseconds()}, toAny(ids)...)...)
	if err != nil {
		return nil, err
	}

	rows, err = tx.QueryContext(ctx, `
		SELECT `+selectColumns+` FROM jobsync_jobs
		WHERE id IN (`+placeholders(len(ids))+`)
		ORDER BY priority, scheduled_at`, toAny(ids)...)
	if err != nil {
		return nil, err
	}
	jobs, err := scanJobs(rows)
	if err != nil {
		return nil, err
	}
	return jobs, tx.Commit()
}

func (s *Storage) Extend(ctx context.Context, ids []string, owner string, lease time.Duration) error {
	if len(ids) == 0 {
		return nil
	}
	args := append([]any{lease.Microseconds(), owner}, toAny(ids)...)
	_, err := s.db.ExecContext(ctx, `
		UPDATE jobsync_jobs
		SET leased_until = DATE_ADD(UTC_TIMESTAMP(6), INTERVAL ? MICROSECOND)
		WHERE owner = ? AND state = '`+stateRunning+`'
		  AND id IN (`+placeholders(len(ids))+`)`, args...)
	return err
}

func (s *Storage) Finish(ctx context.Context, id, owner string, r jobsync.Result) error {
	// Every branch carries `AND owner = ? AND state = 'running'`. Zero rows
	// affected means the lease was already reclaimed and the job may be running
	// elsewhere: the contract says drop the result, so no error.
	if r.State == jobsync.StateRetrying {
		_, err := s.db.ExecContext(ctx, `
			UPDATE jobsync_jobs SET
				state        = '`+stateAvailable+`',
				attempt      = attempt + 1,
				scheduled_at = ?,
				last_error   = ?,
				owner        = '',
				leased_until = NULL,
				transitions  = JSON_ARRAY_APPEND(transitions, '$', JSON_OBJECT(
					'state', 'retrying', 'at', UTC_TIMESTAMP(6), 'reason', ?))
			WHERE id = ? AND owner = ? AND state = '`+stateRunning+`'`,
			r.RetryAt.UTC(), r.Err, r.Err, id, owner)
		return err
	}

	// Terminal: unique_key goes to NULL so the same work can be requested again.
	_, err := s.db.ExecContext(ctx, `
		UPDATE jobsync_jobs SET
			state        = ?,
			last_error   = ?,
			unique_key   = NULL,
			owner        = '',
			leased_until = NULL,
			finished_at  = UTC_TIMESTAMP(6),
			transitions  = JSON_ARRAY_APPEND(transitions, '$', JSON_OBJECT(
				'state', ?, 'at', UTC_TIMESTAMP(6), 'reason', ?))
		WHERE id = ? AND owner = ? AND state = '`+stateRunning+`'`,
		string(r.State), r.Err, string(r.State), r.Err, id, owner)
	return err
}

func (s *Storage) Reclaim(ctx context.Context) (int, error) {
	res, err := s.db.ExecContext(ctx, `
		UPDATE jobsync_jobs SET state = '`+stateAvailable+`', owner = '', leased_until = NULL
		WHERE state = '`+stateRunning+`' AND leased_until < UTC_TIMESTAMP(6)`)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	return int(n), err
}

// Lock implements jobsync.Locker. MySQL has no conditional ON DUPLICATE KEY
// UPDATE, so this is delete-if-expired then insert, in a transaction: a
// duplicate-key error on the insert means the lock is held and unexpired, which
// is the ordinary outcome rather than a failure.
//
// Deliberately not GET_LOCK: that is connection-scoped with no TTL, so a dropped
// connection releases it silently.
func (s *Storage) Lock(ctx context.Context, key string, ttl time.Duration) (string, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx,
		"DELETE FROM jobsync_locks WHERE lock_key = ? AND expires_at < UTC_TIMESTAMP(6)", key)
	if err != nil {
		return "", err
	}

	token := newToken()
	_, err = tx.ExecContext(ctx, `
		INSERT INTO jobsync_locks (lock_key, token, expires_at)
		VALUES (?, ?, DATE_ADD(UTC_TIMESTAMP(6), INTERVAL ? MICROSECOND))`,
		key, token, ttl.Microseconds())

	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == duplicateEntry {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return token, tx.Commit()
}

func (s *Storage) Unlock(ctx context.Context, key, token string) error {
	// The token check stops a server whose lock already expired from releasing
	// the lock a different server now holds.
	_, err := s.db.ExecContext(ctx,
		"DELETE FROM jobsync_locks WHERE lock_key = ? AND token = ?", key, token)
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
			_, err := s.db.ExecContext(ctx, `
				DELETE FROM jobsync_jobs
				WHERE state IN ('succeeded','dead','cancelled')
				  AND created_at < DATE_SUB(UTC_TIMESTAMP(6), INTERVAL ? MICROSECOND)`,
				s.Retention.Microseconds())
			if err != nil {
				return fmt.Errorf("jobsync: janitor: %w", err)
			}
		}
	}
}

// Truncate empties every table. For tests only.
func (s *Storage) Truncate(ctx context.Context) error {
	for _, t := range []string{"jobsync_jobs", "jobsync_locks", "jobsync_schedules", "jobsync_servers", "jobsync_paused_queues"} {
		if _, err := s.db.ExecContext(ctx, "TRUNCATE TABLE "+t); err != nil {
			return err
		}
	}
	return nil
}

func scanJobs(rows *sql.Rows) ([]*jobsync.Job, error) {
	defer rows.Close()

	var jobs []*jobsync.Job
	for rows.Next() {
		j := &jobsync.Job{State: jobsync.StateRunning}
		var (
			key         sql.NullString
			lastErr     sql.NullString
			tags        []byte
			leased, fin sql.NullTime
		)
		err := rows.Scan(&j.ID, &j.Kind, &j.Queue, &j.Payload, &j.Priority, &j.Attempt,
			&j.MaxAttempts, &key, &tags, &lastErr, &j.CreatedAt, &j.ScheduledAt,
			&leased, &fin, &j.Owner)
		if err != nil {
			return nil, err
		}
		j.UniqueKey = key.String
		j.LastError = lastErr.String
		j.LeasedUntil, j.FinishedAt = leased.Time, fin.Time
		if err := decodeTags(tags, j); err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

// placeholders builds "?,?,?" for an IN list. MySQL has no array parameter, so
// unlike Postgres's ANY($1) the list has to be expanded. Callers guard against
// n == 0, which would produce the syntax error IN ().
func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

func toAny(ss []string) []any {
	out := make([]any, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}

func nonNilTags(tags []string) []string {
	if tags == nil {
		return []string{}
	}
	return tags
}

func newToken() string {
	var b [16]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
