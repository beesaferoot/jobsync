// Package memory is an in-process jobsync storage driver.
//
// It is not for production: jobs do not survive a restart, and every operation
// takes a process-wide lock. It exists as the control for the conformance
// suite. When a real driver fails a test, the memory driver answers the question
// "is the test wrong, or is the driver wrong?" — if the simplest possible
// implementation cannot satisfy it, the test is the thing to fix.
//
// It is also genuinely useful in application tests, where a real queue is
// usually more infrastructure than the test needed.
package memory

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/beesaferoot/jobsync"
)

type Storage struct {
	mu          sync.Mutex
	jobs        map[string]*jobsync.Job
	locks       map[string]lock
	schedules   map[string]jobsync.Schedule
	transitions map[string][]jobsync.Transition
	servers     map[string]serverEntry
	// pausedQueues is read on the Fetch path, so a paused queue is paused for
	// every server rather than only the one that was told.
	pausedQueues map[string]struct{}
}

type serverEntry struct {
	info    jobsync.ServerInfo
	expires time.Time
}

// New returns an empty in-process storage.
func New() *Storage {
	return &Storage{
		jobs:         map[string]*jobsync.Job{},
		locks:        map[string]lock{},
		schedules:    map[string]jobsync.Schedule{},
		transitions:  map[string][]jobsync.Transition{},
		servers:      map[string]serverEntry{},
		pausedQueues: map[string]struct{}{},
	}
}

func (s *Storage) Enqueue(ctx context.Context, jobs []*jobsync.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, j := range jobs {
		// Ignored, never overwritten: overwriting would resurrect a finished job
		// as a fresh one and run the work twice.
		if _, exists := s.jobs[j.ID]; exists {
			continue
		}
		if j.UniqueKey != "" && s.uniqueKeyHeld(j.UniqueKey) {
			continue
		}
		s.jobs[j.ID] = clone(j)
	}
	return nil
}

// uniqueKeyHeld reports whether a live job already holds key. Terminal jobs have
// their key cleared by Finish, so they never block a re-enqueue.
func (s *Storage) uniqueKeyHeld(key string) bool {
	for _, j := range s.jobs {
		if j.UniqueKey == key {
			return true
		}
	}
	return false
}

func (s *Storage) Fetch(ctx context.Context, queues []string, n int, owner string, lease time.Duration) ([]*jobsync.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	var due []*jobsync.Job
	for _, j := range s.jobs {
		if !available(j.State) || !slices.Contains(queues, j.Queue) || j.ScheduledAt.After(now) {
			continue
		}
		if _, paused := s.pausedQueues[j.Queue]; paused {
			continue
		}
		due = append(due, j)
	}

	slices.SortFunc(due, func(a, b *jobsync.Job) int {
		if a.Priority != b.Priority {
			return a.Priority - b.Priority
		}
		return a.ScheduledAt.Compare(b.ScheduledAt)
	})

	if len(due) > n {
		due = due[:n]
	}

	claimed := make([]*jobsync.Job, len(due))
	for i, j := range due {
		j.State = jobsync.StateRunning
		j.Owner = owner
		j.LeasedUntil = now.Add(lease)
		claimed[i] = clone(j)
	}
	return claimed, nil
}

func (s *Storage) Extend(ctx context.Context, ids []string, owner string, lease time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, id := range ids {
		if j, ok := s.jobs[id]; ok && j.Owner == owner && j.State == jobsync.StateRunning {
			j.LeasedUntil = time.Now().Add(lease)
		}
	}
	return nil
}

func (s *Storage) Finish(ctx context.Context, id, owner string, r jobsync.Result) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[id]
	// A job this owner no longer holds was reclaimed and may be running
	// elsewhere. Dropping the result is the contract, not a failure.
	if !ok || j.Owner != owner || j.State != jobsync.StateRunning {
		return nil
	}

	j.State = r.State
	j.LastError = r.Err
	j.Owner = ""
	j.LeasedUntil = time.Time{}
	s.record(id, r.State, r.Err)

	if r.State == jobsync.StateRetrying {
		j.Attempt++
		j.ScheduledAt = r.RetryAt
		j.State = jobsync.StateScheduled
		return nil
	}
	// Terminal: release the unique key so the same work can be requested again.
	j.UniqueKey = ""
	j.FinishedAt = time.Now()
	return nil
}

func (s *Storage) Reclaim(ctx context.Context) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	var n int
	for _, j := range s.jobs {
		if j.State != jobsync.StateRunning || j.LeasedUntil.After(now) {
			continue
		}
		j.State = jobsync.StateEnqueued
		j.Owner = ""
		j.LeasedUntil = time.Time{}
		n++
	}
	return n, nil
}

func (s *Storage) Close() error { return nil }

// available reports whether a job is a candidate for Fetch. Scheduled and
// Enqueued are both "available, subject to ScheduledAt" — the distinction is
// presentational, which is why SQL drivers can collapse them into one stored
// state and skip the promotion sweeper entirely.
func available(state jobsync.State) bool {
	return state == jobsync.StateEnqueued || state == jobsync.StateScheduled
}

// clone keeps callers from mutating stored jobs through the returned pointer,
// which a real driver gets for free by scanning into a fresh struct.
func clone(j *jobsync.Job) *jobsync.Job {
	out := *j
	out.Payload = slices.Clone(j.Payload)
	out.Tags = slices.Clone(j.Tags)
	return &out
}
