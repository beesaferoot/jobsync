package jobsync

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// JobType is a typed handle to one kind of job. Declare it at package level; it
// carries no dependencies, so it can be referenced from anywhere that enqueues:
//
//	var SendWelcome = jobsync.Declare[WelcomeArgs]("email.welcome")
//
// The implementation is bound separately, at wiring time, where dependencies
// actually exist:
//
//	SendWelcome.Handle(srv, func(ctx context.Context, a WelcomeArgs) error {
//		return mailer.Send(ctx, a.Email)
//	})
//
// That split is the point. Hangfire resolves the job target from an IoC
// container at execution time; Go has no ambient container, and the usual
// answer — an args type with a Kind() method plus a registered worker struct —
// makes you write three declarations for one job. Here the enqueue side needs
// no dependencies and the handler side is a closure over them.
type JobType[T any] struct {
	kind string
}

// Declare creates a job type. kind is the name persisted with every job, so it
// must stay stable across deploys: renaming it strands jobs already in storage.
func Declare[T any](kind string) *JobType[T] {
	if kind == "" {
		panic("jobsync: job kind must not be empty")
	}
	return &JobType[T]{kind: kind}
}

// Kind is the name persisted with every job of this type.
func (j *JobType[T]) Kind() string { return j.kind }

// Enqueue queues args for execution as soon as a server picks it up.
func (j *JobType[T]) Enqueue(ctx context.Context, c *Client, args T, opts ...Option) (string, error) {
	job, err := j.build(args, opts)
	if err != nil {
		return "", err
	}
	if err := c.store.Enqueue(ctx, []*Job{job}); err != nil {
		return "", err
	}
	return job.ID, nil
}

// EnqueueTx queues args inside the caller's transaction, so the job and the rows
// that justify it commit together. It fails if the driver is not a TxEnqueuer —
// loudly, because silently falling back to a separate write would reintroduce
// exactly the race the caller is avoiding.
func (j *JobType[T]) EnqueueTx(ctx context.Context, c *Client, tx Tx, args T, opts ...Option) (string, error) {
	txe, ok := c.store.(TxEnqueuer)
	if !ok {
		return "", fmt.Errorf("jobsync: %T does not support transactional enqueue", c.store)
	}
	job, err := j.build(args, opts)
	if err != nil {
		return "", err
	}
	if err := txe.EnqueueTx(ctx, tx, []*Job{job}); err != nil {
		return "", err
	}
	return job.ID, nil
}

// Schedule queues args for execution at or after at.
func (j *JobType[T]) Schedule(ctx context.Context, c *Client, args T, at time.Time, opts ...Option) (string, error) {
	return j.Enqueue(ctx, c, args, append(opts, runAt(at))...)
}

// Cron registers a recurring job. id must be stable across deploys; re-running
// with the same id updates the schedule in place rather than duplicating it.
func (j *JobType[T]) Cron(ctx context.Context, c *Client, id, spec string, args T, opts ...Option) error {
	if c.schedules == nil {
		return fmt.Errorf("jobsync: %T does not store schedules, so it cannot run recurring jobs", c.store)
	}
	if id == "" {
		return fmt.Errorf("jobsync: a recurring job needs a stable id")
	}
	job, err := j.build(args, opts)
	if err != nil {
		return err
	}
	// A schedule's zone travels with it to every server in the fleet, so an
	// unresolvable name is refused here rather than at the first fire. "Local" is
	// the one that matters: it loads successfully everywhere and means something
	// different on each machine.
	if c.location == time.Local || c.location.String() == "Local" {
		return fmt.Errorf("jobsync: recurring jobs need an explicit timezone; pass jobsync.InLocation with a named zone such as UTC or Africa/Lagos")
	}
	next, err := nextRun(spec, c.location, time.Now())
	if err != nil {
		return err
	}
	return c.schedules.SaveSchedule(ctx, Schedule{
		ID:       id,
		Kind:     j.kind,
		Queue:    job.Queue,
		Payload:  job.Payload,
		Cron:     spec,
		Timezone: c.location.String(),
		NextRun:  next,
	})
}

// Handle binds the implementation of j on srv. Calling it twice for the same
// kind panics: a duplicate registration means one of the two handlers would
// silently never run.
func (j *JobType[T]) Handle(srv *Server, fn func(context.Context, T) error) {
	srv.register(j.kind, func(ctx context.Context, payload []byte) error {
		var args T
		if err := json.Unmarshal(payload, &args); err != nil {
			// Permanent by construction: a payload that will not decode now will
			// not decode on the tenth attempt either.
			return fmt.Errorf("decode %s args: %w: %w", j.kind, err, ErrPermanent)
		}
		return fn(ctx, args)
	})
}

func (j *JobType[T]) build(args T, opts []Option) (*Job, error) {
	payload, err := json.Marshal(args)
	if err != nil {
		return nil, fmt.Errorf("encode %s args: %w", j.kind, err)
	}
	now := time.Now()
	job := &Job{
		ID:          newID(),
		Kind:        j.kind,
		Queue:       "default",
		Payload:     payload,
		MaxAttempts: 10,
		State:       StateEnqueued,
		CreatedAt:   now,
		ScheduledAt: now,
	}
	for _, opt := range opts {
		opt(job)
	}
	if job.ScheduledAt.After(now) {
		job.State = StateScheduled
	}
	return job, nil
}

// Option adjusts a job at enqueue time. Every field it touches has a working
// default, which is why these are options and not parameters.
//
// Pass them to Enqueue, Schedule, EnqueueTx or Cron:
//
//	SendWelcome.Enqueue(ctx, client, args,
//		jobsync.Queue("mail"),
//		jobsync.MaxAttempts(5),
//	)
type Option func(*Job)

// Queue routes the job to a named queue. Default: "default". A server only runs
// the queues it was configured with, so this is how work is partitioned across
// pools.
func Queue(name string) Option { return func(j *Job) { j.Queue = name } }

// Priority orders the job within its queue. Lower runs first; default 0.
// Honoured exactly by the SQL drivers, and clamped to 0..9 by Redis, where a
// sorted-set score cannot encode both priority and due time.
func Priority(p int) Option { return func(j *Job) { j.Priority = p } }

// MaxAttempts caps how many times the job runs before it is marked dead.
// Default 10. A handler returning an error wrapping ErrPermanent skips the
// remaining attempts regardless.
func MaxAttempts(n int) Option { return func(j *Job) { j.MaxAttempts = n } }

// Tags attaches labels for filtering on the dashboard. They carry no meaning to
// the scheduler.
func Tags(tags ...string) Option { return func(j *Job) { j.Tags = tags } }

// In delays the job by d.
func In(d time.Duration) Option { return runAt(time.Now().Add(d)) }

// Unique drops the job if another holding the same key has not yet reached a
// terminal state — so a sweep enqueued by three replicas runs once. The key is
// released when the job finishes, which is what lets a daily job keyed by date
// run again tomorrow.
func Unique(key string) Option { return func(j *Job) { j.UniqueKey = key } }

func runAt(at time.Time) Option { return func(j *Job) { j.ScheduledAt = at } }
