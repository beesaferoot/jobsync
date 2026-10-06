package jobsync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"os"
	"sync"
	"time"
)

// Server runs jobs. Its whole job is to turn the five Storage methods into a
// correct worker loop, which is the proof that the contract is sufficient: if
// the server needs something a driver cannot express, the contract is wrong.
type Server struct {
	store    Storage
	cfg      ServerConfig
	handlers map[string]handler
	id       string
	log      *slog.Logger
}

type handler func(context.Context, []byte) error

type ServerConfig struct {
	// Queues are polled in the order given, so the first is the highest priority.
	Queues []string
	// Concurrency is the number of jobs this server runs at once.
	Concurrency int
	// Lease is how long a claimed job stays owned without a heartbeat. It bounds
	// how long a crashed server's work sits idle, so keep it well under the
	// patience of whoever is waiting for the job.
	Lease time.Duration
	// PollInterval is how long to wait after an empty fetch.
	PollInterval time.Duration
	// ScheduleInterval is how often recurring jobs are checked. Cron resolution
	// is one minute, so the default trades a little latency for far fewer
	// queries; lowering it does not make a cron job fire more often.
	ScheduleInterval time.Duration
	// ScheduleMaxAttempts is MaxAttempts for jobs created from a schedule.
	ScheduleMaxAttempts int
	// Backoff returns how long to wait before retrying a job that failed on
	// attempt n (1-based). Defaults to exponential with jitter, capped at an hour.
	Backoff func(attempt int) time.Duration
	Logger  *slog.Logger
}

func NewServer(store Storage, cfg ServerConfig) *Server {
	if len(cfg.Queues) == 0 {
		cfg.Queues = []string{"default"}
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 10
	}
	if cfg.Lease <= 0 {
		cfg.Lease = 30 * time.Second
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second
	}
	if cfg.ScheduleInterval <= 0 {
		cfg.ScheduleInterval = 10 * time.Second
	}
	if cfg.ScheduleMaxAttempts <= 0 {
		cfg.ScheduleMaxAttempts = 10
	}
	if cfg.Backoff == nil {
		cfg.Backoff = exponentialBackoff
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	host, _ := os.Hostname()
	return &Server{
		store:    store,
		cfg:      cfg,
		handlers: map[string]handler{},
		id:       host + "/" + newID()[:8],
		log:      cfg.Logger,
	}
}

func (s *Server) register(kind string, h handler) {
	if _, dup := s.handlers[kind]; dup {
		panic("jobsync: duplicate handler for job kind " + kind)
	}
	s.handlers[kind] = h
}

// Run processes jobs until ctx is cancelled, then waits for in-flight jobs to
// finish. Jobs still running when ctx's deadline passes keep their lease, so
// another server reclaims them rather than losing them.
func (s *Server) Run(ctx context.Context) error {
	if len(s.handlers) == 0 {
		return errors.New("jobsync: no handlers registered")
	}

	queue := make(chan *Job)
	var wg sync.WaitGroup
	for range s.cfg.Concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range queue {
				s.run(ctx, job)
			}
		}()
	}

	sched, err := newScheduler(s)
	if err != nil {
		return err
	}
	if sched != nil {
		go sched.run(ctx)
	}

	if hb, ok := s.store.(Heartbeat); ok {
		go s.heartbeatLoop(ctx, hb)
	}

	go s.reclaimLoop(ctx)

	s.fetchLoop(ctx, queue)
	close(queue)
	wg.Wait()
	return ctx.Err()
}

// fetchLoop keeps at most Concurrency jobs claimed: it blocks on the unbuffered
// send until a worker is free, so a job is never claimed before there is someone
// to run it. That matters because a claimed job holds a lease, and leases that
// sit in a local buffer during a crash are leases nobody can see.
func (s *Server) fetchLoop(ctx context.Context, queue chan<- *Job) {
	for {
		jobs, err := s.store.Fetch(ctx, s.cfg.Queues, 1, s.id, s.cfg.Lease)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.log.Error("jobsync: fetch failed", "err", err)
			if !sleep(ctx, s.cfg.PollInterval) {
				return
			}
			continue
		}
		if len(jobs) == 0 {
			if !sleep(ctx, s.cfg.PollInterval) {
				return
			}
			continue
		}
		for _, job := range jobs {
			select {
			case queue <- job:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (s *Server) run(ctx context.Context, job *Job) {
	h, ok := s.handlers[job.Kind]
	if !ok {
		// Another server in this fleet may know this kind; a deploy in progress
		// is the usual cause. Release it rather than killing it.
		s.finish(ctx, job, Result{State: StateRetrying, RetryAt: time.Now().Add(s.cfg.PollInterval), Err: "no handler on " + s.id})
		return
	}

	done := make(chan struct{})
	go s.heartbeat(ctx, job.ID, done)

	err := safely(ctx, h, job.Payload)
	close(done)

	if err == nil {
		s.finish(ctx, job, Result{State: StateSucceeded})
		return
	}

	attempt := job.Attempt + 1
	// A permanent failure skips the remaining attempts: retrying something the
	// job can never resolve only buries the real failures under noise.
	if errors.Is(err, ErrPermanent) {
		s.log.Error("jobsync: job failed permanently", "id", job.ID, "kind", job.Kind, "err", err)
		s.finish(ctx, job, Result{State: StateDead, Err: err.Error()})
		return
	}
	if attempt >= job.MaxAttempts {
		s.log.Error("jobsync: job dead", "id", job.ID, "kind", job.Kind, "attempts", attempt, "err", err)
		s.finish(ctx, job, Result{State: StateDead, Err: err.Error()})
		return
	}
	s.finish(ctx, job, Result{
		State:   StateRetrying,
		RetryAt: time.Now().Add(s.cfg.Backoff(attempt)),
		Err:     err.Error(),
	})
}

// safely runs the handler, converting a panic into a failed attempt. This is one
// of the few places recover is right: a panicking job must not take down every
// other job on the server.
func safely(ctx context.Context, h handler, payload []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return h(ctx, payload)
}

func (s *Server) heartbeat(ctx context.Context, id string, done <-chan struct{}) {
	t := time.NewTicker(s.cfg.Lease / 3)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			if err := s.store.Extend(ctx, []string{id}, s.id, s.cfg.Lease); err != nil {
				s.log.Warn("jobsync: lease extension failed", "id", id, "err", err)
			}
		case <-done:
			return
		case <-ctx.Done():
			return
		}
	}
}

// finish reports the outcome with a fresh context: the job is done either way,
// and losing the result because the server is shutting down would make the job
// run a second time for no reason.
func (s *Server) finish(ctx context.Context, job *Job, r Result) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := s.store.Finish(ctx, job.ID, s.id, r); err != nil {
		s.log.Error("jobsync: could not record outcome", "id", job.ID, "state", r.State, "err", err)
	}
}

// heartbeatLoop keeps this server visible on the dashboard. It is observability
// only: nothing about running or recovering jobs depends on it, so a failure is
// logged and the loop carries on rather than taking the server down.
func (s *Server) heartbeatLoop(ctx context.Context, hb Heartbeat) {
	const ttl = 30 * time.Second
	host, _ := os.Hostname()
	info := ServerInfo{
		ID:          s.id,
		Hostname:    host,
		Queues:      s.cfg.Queues,
		Concurrency: s.cfg.Concurrency,
		StartedAt:   time.Now(),
	}

	// Write one immediately: a server that appears only after the first tick
	// looks dead for its first half-minute, which is exactly when someone is
	// watching a deploy.
	if err := hb.Heartbeat(ctx, info, ttl); err != nil {
		s.log.Warn("jobsync: heartbeat failed", "err", err)
	}
	for sleep(ctx, ttl/3) {
		if err := hb.Heartbeat(ctx, info, ttl); err != nil {
			s.log.Warn("jobsync: heartbeat failed", "err", err)
		}
	}
}

func (s *Server) reclaimLoop(ctx context.Context) {
	for sleep(ctx, s.cfg.Lease) {
		n, err := s.store.Reclaim(ctx)
		if err != nil {
			s.log.Error("jobsync: reclaim failed", "err", err)
			continue
		}
		if n > 0 {
			s.log.Info("jobsync: reclaimed abandoned jobs", "count", n)
		}
	}
}

func exponentialBackoff(attempt int) time.Duration {
	d := time.Duration(math.Pow(2, float64(attempt))) * time.Second
	if d > time.Hour {
		d = time.Hour
	}
	return d/2 + time.Duration(rand.Int64N(int64(d/2)))
}

// sleep reports whether it slept without the context being cancelled.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
