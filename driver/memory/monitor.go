package memory

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/beesaferoot/jobsync"
)

// derive turns the stored state into the contract's seven. Scheduled, Enqueued
// and Retrying share one stored state here exactly as they do in the SQL
// drivers, so the control and the real drivers answer the same question the same
// way.
func derive(j *jobsync.Job, now time.Time) jobsync.State {
	if !available(j.State) {
		return j.State
	}
	if !j.ScheduledAt.After(now) {
		return jobsync.StateEnqueued
	}
	if j.Attempt > 0 {
		return jobsync.StateRetrying
	}
	return jobsync.StateScheduled
}

func (s *Storage) Counts(ctx context.Context) (jobsync.Counts, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	var c jobsync.Counts
	for _, j := range s.jobs {
		switch derive(j, now) {
		case jobsync.StateScheduled:
			c.Scheduled++
		case jobsync.StateEnqueued:
			c.Enqueued++
		case jobsync.StateRunning:
			c.Running++
		case jobsync.StateRetrying:
			c.Retrying++
		case jobsync.StateSucceeded:
			c.Succeeded++
		case jobsync.StateDead:
			c.Dead++
		case jobsync.StateCancelled:
			c.Cancelled++
		}
	}
	return c, nil
}

func (s *Storage) QueueStats(ctx context.Context) ([]jobsync.QueueStat, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	stats := map[string]*jobsync.QueueStat{}
	for _, j := range s.jobs {
		q, ok := stats[j.Queue]
		if !ok {
			_, paused := s.pausedQueues[j.Queue]
			q = &jobsync.QueueStat{Name: j.Queue, Paused: paused}
			stats[j.Queue] = q
		}
		switch derive(j, now) {
		case jobsync.StateEnqueued:
			q.Enqueued++
			if age := now.Sub(j.ScheduledAt); age > q.OldestEnqueued {
				q.OldestEnqueued = age
			}
		case jobsync.StateRunning:
			q.Running++
		}
	}

	out := make([]jobsync.QueueStat, 0, len(stats))
	for _, q := range stats {
		out = append(out, *q)
	}
	slices.SortFunc(out, func(a, b jobsync.QueueStat) int { return cmpString(a.Name, b.Name) })
	return out, nil
}

// Servers reports nothing: an in-process storage has exactly one server and it
// is the one reading this. Returning an empty list is the contract's answer for
// a driver that keeps no registry.
func (s *Storage) Servers(ctx context.Context) ([]jobsync.ServerInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]jobsync.ServerInfo, 0, len(s.servers))
	cutoff := time.Now()
	for _, srv := range s.servers {
		if srv.expires.After(cutoff) {
			out = append(out, srv.info)
		}
	}
	slices.SortFunc(out, func(a, b jobsync.ServerInfo) int { return cmpString(a.ID, b.ID) })
	return out, nil
}

func (s *Storage) Heartbeat(ctx context.Context, info jobsync.ServerInfo, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	info.HeartbeatAt = time.Now()
	s.servers[info.ID] = serverEntry{info: info, expires: time.Now().Add(ttl)}
	return nil
}

func (s *Storage) ListJobs(ctx context.Context, f jobsync.Filter) ([]*jobsync.Job, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	var matched []*jobsync.Job
	for _, j := range s.jobs {
		if matches(j, f, now) {
			matched = append(matched, clone(j))
		}
	}

	// Newest first, then by id so the ordering is total. Without the tiebreak,
	// two jobs created in the same instant could swap between pages and the
	// pager would skip one and repeat another.
	slices.SortFunc(matched, func(a, b *jobsync.Job) int {
		if c := b.CreatedAt.Compare(a.CreatedAt); c != 0 {
			return c
		}
		return cmpString(a.ID, b.ID)
	})

	total := int64(len(matched))
	for i := range matched {
		matched[i].State = derive(matched[i], now)
	}

	if f.Offset >= len(matched) {
		return nil, total, nil
	}
	matched = matched[f.Offset:]
	limit := f.Limit
	if limit <= 0 {
		limit = 50
	}
	if len(matched) > limit {
		matched = matched[:limit]
	}
	return matched, total, nil
}

func matches(j *jobsync.Job, f jobsync.Filter, now time.Time) bool {
	if len(f.States) > 0 && !slices.Contains(f.States, derive(j, now)) {
		return false
	}
	if len(f.Queues) > 0 && !slices.Contains(f.Queues, j.Queue) {
		return false
	}
	if len(f.Kinds) > 0 && !slices.Contains(f.Kinds, j.Kind) {
		return false
	}
	if !f.Since.IsZero() && j.CreatedAt.Before(f.Since) {
		return false
	}
	if !f.Until.IsZero() && j.CreatedAt.After(f.Until) {
		return false
	}
	for _, tag := range f.Tags {
		if !slices.Contains(j.Tags, tag) {
			return false
		}
	}
	if f.Search != "" {
		hay := j.Kind + " " + j.ID + " " + j.LastError
		if !strings.Contains(strings.ToLower(hay), strings.ToLower(f.Search)) {
			return false
		}
	}
	return true
}

func (s *Storage) JobHistory(ctx context.Context, id string) (*jobsync.Job, []jobsync.Transition, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[id]
	if !ok {
		return nil, nil, fmt.Errorf("jobsync: no job %s", id)
	}
	out := clone(j)
	out.State = derive(j, time.Now())
	return out, slices.Clone(s.transitions[id]), nil
}

func (s *Storage) Requeue(ctx context.Context, ids []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	for _, id := range ids {
		j, ok := s.jobs[id]
		// A running job is left alone: it is not stuck, it is working, and
		// requeueing it would run the same work twice.
		if !ok || j.State == jobsync.StateRunning {
			continue
		}
		j.State = jobsync.StateEnqueued
		j.Attempt = 0
		j.ScheduledAt = now.Add(-time.Second)
		j.LastError = ""
		j.FinishedAt = time.Time{}
		s.record(id, jobsync.StateEnqueued, "requeued from the dashboard")
	}
	return nil
}

func (s *Storage) Delete(ctx context.Context, ids []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, id := range ids {
		if j, ok := s.jobs[id]; ok && j.State == jobsync.StateRunning {
			continue
		}
		delete(s.jobs, id)
		delete(s.transitions, id)
	}
	return nil
}

func (s *Storage) Throughput(ctx context.Context, since time.Time, bucket time.Duration) ([]jobsync.Bucket, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if bucket <= 0 {
		return nil, fmt.Errorf("jobsync: throughput bucket must be positive")
	}

	byBucket := map[int64]*jobsync.Bucket{}
	for _, j := range s.jobs {
		if j.FinishedAt.IsZero() || j.FinishedAt.Before(since) {
			continue
		}
		at := j.FinishedAt.Truncate(bucket)
		b, ok := byBucket[at.UnixNano()]
		if !ok {
			b = &jobsync.Bucket{At: at}
			byBucket[at.UnixNano()] = b
		}
		switch j.State {
		case jobsync.StateSucceeded:
			b.Succeeded++
		case jobsync.StateDead, jobsync.StateCancelled:
			b.Failed++
		}
	}

	out := make([]jobsync.Bucket, 0, len(byBucket))
	for _, b := range byBucket {
		out = append(out, *b)
	}
	slices.SortFunc(out, func(a, b jobsync.Bucket) int { return a.At.Compare(b.At) })
	return out, nil
}

// record appends to a job's transition history. Called with s.mu held.
func (s *Storage) record(id string, state jobsync.State, reason string) {
	s.transitions[id] = append(s.transitions[id], jobsync.Transition{
		State: state, At: time.Now(), Reason: reason,
	})
}
