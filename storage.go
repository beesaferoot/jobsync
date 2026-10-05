package jobsync

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// ErrUnsupportedFilter is returned by Monitor.ListJobs for a Filter field the
// driver cannot serve. Filter.Search is the usual one: Postgres answers it with
// a trigram index, Redis cannot answer it at all without indexing every job's
// text. The dashboard disables the control rather than showing wrong results.
//
// State, Queue and Kind filters are mandatory for every driver; they are a
// secondary index, not a search.
var ErrUnsupportedFilter = errors.New("jobsync: storage cannot serve this filter")

// Storage is everything a driver MUST implement. Five methods: that is the whole
// bar for running one-off, delayed and retried jobs.
//
// Deliberately absent: sets, hashes, lists, counters. Hangfire's storage API
// leaks Redis data structures into every driver, which is why its SQL storages
// are a pile of emulation tables. This contract speaks jobs, queues and leases,
// and each driver answers it however its engine is good at.
//
// Delivery is at-least-once. Every implementation must satisfy:
//
//   - Fetch is atomic across processes. Two servers polling the same queue never
//     receive the same job. SQL drivers use SELECT ... FOR UPDATE SKIP LOCKED;
//     Redis uses an atomic pop into a lease set.
//   - A job in StateRunning whose lease has expired is recoverable by Reclaim,
//     even if the server that held it never comes back.
//   - Finish is ignored (not an error) when owner no longer holds the lease.
//     The job was already reclaimed and may be running elsewhere; the late
//     result must not overwrite the new attempt.
//
// Everything beyond this is an optional interface below, discovered by type
// assertion. A driver that implements only Storage still runs jobs; it just
// lights up fewer panels in the dashboard.
type Storage interface {
	// Enqueue persists jobs. A job whose ScheduledAt is in the future is stored
	// in StateScheduled and becomes fetchable when it comes due; otherwise it is
	// stored in StateEnqueued.
	//
	// A job whose ID already exists is ignored, silently. Callers rely on this
	// for idempotency: the scheduler derives a job id from (schedule, tick), so a
	// server crashing between enqueuing a tick and recording it refires the same
	// tick and the duplicate is a no-op instead of a second run.
	//
	// Ignored means ignored — not overwritten. Overwriting would resurrect a
	// finished job as a fresh one and run the work twice, which is the exact
	// failure the rule exists to prevent.
	//
	// When UniqueKey is set and a non-terminal job already holds that key, the
	// new job is dropped silently rather than returning an error.
	Enqueue(ctx context.Context, jobs []*Job) error

	// Fetch never returns a job from a paused queue, if the driver implements
	// QueueControl. The check belongs here and not in the caller: a paused queue
	// must be paused for every server, including one that has not noticed yet,
	// and a server filtering its own list would keep draining a queue an operator
	// just stopped.
	//
	// Fetch atomically claims at most n due jobs from queues, moving each to
	// StateRunning with a lease held by owner until now+lease. An empty result is
	// not an error.
	//
	// A job is due when its ScheduledAt has passed according to the STORAGE's
	// clock, never the calling process's. That is the only clock every server
	// shares, so it is the only one they can agree on. The cost is that a job
	// enqueued with ScheduledAt = time.Now() on a host whose clock runs ahead of
	// the storage is not instantly fetchable — it waits out the skew, then the
	// next poll. Callers that need a job to run now should not depend on it being
	// fetchable in the same millisecond it was enqueued.
	//
	// Jobs are returned in Priority then ScheduledAt order, best-effort within the
	// driver's priority granularity. SQL drivers honour Priority exactly. Redis
	// cannot: a sorted set score encodes one dimension, so ordering by priority
	// and filtering by due time at once needs a structure per priority level, and
	// an unbounded number of levels is unbounded by user input. Drivers with
	// coarse priority clamp it and document the range.
	Fetch(ctx context.Context, queues []string, n int, owner string, lease time.Duration) ([]*Job, error)

	// Extend renews the lease on jobs still running under owner. Jobs no longer
	// owned are skipped, not reported.
	Extend(ctx context.Context, ids []string, owner string, lease time.Duration) error

	// Finish applies r to a job owner holds. On Retrying the job returns to
	// StateScheduled with ScheduledAt set to r.RetryAt and Attempt incremented.
	//
	// Any terminal state (Succeeded, Dead, Cancelled) must also clear UniqueKey.
	// Otherwise a job keyed report:2026-10-05 holds that key forever once it
	// succeeds, and can never be enqueued again — including by a manual retry
	// from the dashboard.
	Finish(ctx context.Context, id, owner string, r Result) error

	// Reclaim returns jobs whose lease expired to StateEnqueued and reports how
	// many were recovered. Every server calls it periodically, so it must be
	// safe to run concurrently on every node.
	Reclaim(ctx context.Context) (int, error)

	Close() error
}

// Locker is required for recurring jobs: it elects one server to fire a given
// schedule. A driver without it can still run one-off jobs; Server.Run refuses
// to start if schedules are registered and the driver is not a Locker.
//
// Lock returns an empty token when the lock is held elsewhere, which is the
// normal case, not an error. Unlock must verify the token so a server whose
// lease already expired cannot release the lock a different server now holds.
type Locker interface {
	Lock(ctx context.Context, key string, ttl time.Duration) (token string, err error)
	Unlock(ctx context.Context, key, token string) error
}

// Heartbeat records that a server is alive and what it is working on. Servers
// call it periodically; Monitor.Servers reads back the ones still within ttl.
//
// A driver may skip it, and then the dashboard's server list is empty rather
// than wrong. Nothing about running jobs depends on it — it is observability,
// not coordination, and in particular it is NOT how abandoned work is
// recovered. That is Reclaim, driven by lease expiry, which keeps working
// whether or not anyone is writing heartbeats.
type Heartbeat interface {
	Heartbeat(ctx context.Context, info ServerInfo, ttl time.Duration) error
}

// QueueControl lets an operator stop a queue without stopping the servers. The
// usual reason is a downstream outage: the work is still worth doing, just not
// right now, and pausing beats letting ten thousand jobs burn their retries
// against a dead API.
//
// Pausing affects only Fetch. Jobs continue to enqueue, nothing is lost, and
// resuming drains the backlog in the normal order.
type QueueControl interface {
	PauseQueue(ctx context.Context, name string) error
	ResumeQueue(ctx context.Context, name string) error
	PausedQueues(ctx context.Context) ([]string, error)
}

// Schedules stores recurring job definitions. Cron expressions are parsed by the
// server, which writes back the computed NextRun, so drivers store opaque text.
type Schedules interface {
	SaveSchedule(ctx context.Context, s Schedule) error
	ListSchedules(ctx context.Context) ([]Schedule, error)
	RemoveSchedule(ctx context.Context, id string) error

	// DueSchedules returns schedules with NextRun <= now and Paused false.
	DueSchedules(ctx context.Context, now time.Time) ([]Schedule, error)

	// AdvanceSchedule records that a run was fired. The caller holds the Locker
	// lock for the schedule, so this needs no atomicity of its own.
	AdvanceSchedule(ctx context.Context, id string, lastRun, nextRun time.Time) error

	// SetSchedulePaused stops or resumes a schedule without deleting it, which is
	// what an operator wants during an incident: the definition and its history
	// survive, and resuming does not re-fire whatever was missed.
	//
	// Setting it on an absent schedule is not an error.
	SetSchedulePaused(ctx context.Context, id string, paused bool) error
}

// Tx is the subset of a database handle needed to insert a job inside a caller's
// transaction. *sql.Tx satisfies it. Native pgx users wrap their pool with
// pgx/stdlib, or use the pgx driver's own EnqueueTxPgx.
type Tx interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// TxEnqueuer is implemented by SQL drivers. It makes a job and the rows that
// justify it commit together, so a job can never reference a row that rolled
// back. Redis cannot offer this; code that needs it should type-assert and fail
// loudly at startup rather than degrade silently.
type TxEnqueuer interface {
	EnqueueTx(ctx context.Context, tx Tx, jobs []*Job) error
}

// Monitor backs the dashboard. It is one interface rather than a read half and a
// write half because the UI needs all of it to be useful: implement it and every
// panel works, skip it and the dashboard reports that this driver has no
// introspection.
type Monitor interface {
	Counts(ctx context.Context) (Counts, error)
	QueueStats(ctx context.Context) ([]QueueStat, error)
	Servers(ctx context.Context) ([]ServerInfo, error)

	// ListJobs returns a page of jobs and the total matching f.
	ListJobs(ctx context.Context, f Filter) ([]*Job, int64, error)
	JobHistory(ctx context.Context, id string) (*Job, []Transition, error)

	// Throughput returns succeeded and failed counts bucketed over [since, now].
	// Drivers that keep no history may return nil; the dashboard hides the graph.
	//
	// The window is bounded by whatever retention the driver is configured with:
	// a job deleted by the janitor is no longer counted. Asking for a year of
	// throughput from a store that keeps seven days of jobs returns seven days.
	Throughput(ctx context.Context, since time.Time, bucket time.Duration) ([]Bucket, error)

	// Requeue moves jobs back to StateEnqueued with Attempt reset. Delete removes
	// them outright. Both are no-ops for jobs currently in StateRunning.
	Requeue(ctx context.Context, ids []string) error
	Delete(ctx context.Context, ids []string) error
}

// Counts is a tally per state. A struct rather than a map because the state set
// is closed and the dashboard reads every field.
type Counts struct {
	Scheduled, Enqueued, Running, Retrying, Succeeded, Dead, Cancelled int64
}

type QueueStat struct {
	Name     string
	Enqueued int64
	Running  int64
	Paused   bool
	// OldestEnqueued is the age of the head of the queue, the number to alert on.
	OldestEnqueued time.Duration
}

type ServerInfo struct {
	ID          string
	Hostname    string
	Queues      []string
	Concurrency int
	StartedAt   time.Time
	HeartbeatAt time.Time
}

// Transition is one entry in a job's state history, shown on the job detail page.
type Transition struct {
	State  State
	At     time.Time
	Reason string
}

type Bucket struct {
	At        time.Time
	Succeeded int64
	Failed    int64
}

// Filter is the dashboard's query. Zero fields mean no constraint on that field;
// Limit of 0 means the driver's default page size.
type Filter struct {
	States []State
	Queues []string
	Kinds  []string
	Tags   []string
	Search string // substring match over Kind, ID and LastError
	Since  time.Time
	Until  time.Time
	Offset int
	Limit  int
}
