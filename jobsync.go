// Package jobsync is a background job system with a Hangfire-style API and
// pluggable storage. Postgres, MySQL and Redis drivers are maintained in this
// repository; the Storage contract in storage.go is small enough that a new
// driver is a weekend, not a month.
package jobsync

import "time"

// State is the lifecycle position of a job. The set is closed: drivers must not
// invent states, because the dashboard and the retry policy both switch on it.
//
//	Scheduled ──due──> Enqueued ──fetch──> Running ─┬─> Succeeded
//	    ^                  ^                        ├─> Retrying ──backoff──> Enqueued
//	    └──────────────────┴── lease expiry ────────┼─> Dead
//	                                                └─> Cancelled
type State string

const (
	StateScheduled State = "scheduled"
	StateEnqueued  State = "enqueued"
	StateRunning   State = "running"
	StateRetrying  State = "retrying"
	StateSucceeded State = "succeeded"
	StateDead      State = "dead"
	StateCancelled State = "cancelled"
)

// Job is the unit of work as it is persisted. Drivers own the storage layout but
// must be able to round-trip every field here.
type Job struct {
	ID          string
	Kind        string // handler name, from Declare
	Queue       string
	Payload     []byte // JSON-encoded args
	Priority    int    // lower runs first
	Attempt     int    // completed attempts; 0 until the first failure
	MaxAttempts int
	State       State
	UniqueKey   string // empty means no deduplication
	Tags        []string
	LastError   string

	CreatedAt   time.Time
	ScheduledAt time.Time // when the job becomes fetchable
	LeasedUntil time.Time // meaningful only in StateRunning
	FinishedAt  time.Time // zero until the job reaches a terminal state
	Owner       string    // server ID holding the lease
}

// Result is the outcome a server reports for a job it holds the lease on.
type Result struct {
	State   State     // Succeeded, Retrying, Dead or Cancelled
	RetryAt time.Time // when State is Retrying
	Err     string
}

// Schedule is a recurring job definition. The next due time is computed by the
// server, not the driver, so drivers never need a cron parser.
type Schedule struct {
	ID       string // caller-chosen, stable across deploys
	Kind     string
	Queue    string
	Payload  []byte
	Cron     string
	Timezone string
	LastRun  time.Time
	NextRun  time.Time
	Paused   bool
}
