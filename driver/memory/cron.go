package memory

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"slices"
	"time"

	"github.com/beesaferoot/jobsync"
)

type lock struct {
	token   string
	expires time.Time
}

// Lock implements jobsync.Locker.
func (s *Storage) Lock(ctx context.Context, key string, ttl time.Duration) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if held, ok := s.locks[key]; ok && held.expires.After(time.Now()) {
		return "", nil
	}
	token := newToken()
	s.locks[key] = lock{token: token, expires: time.Now().Add(ttl)}
	return token, nil
}

// Unlock implements jobsync.Locker. The token check is what stops a server whose
// lock already expired from releasing the lock a different server now holds.
func (s *Storage) Unlock(ctx context.Context, key, token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if held, ok := s.locks[key]; ok && held.token == token {
		delete(s.locks, key)
	}
	return nil
}

// SaveSchedule implements jobsync.Schedules. It is an upsert: every deploy
// re-registers its schedules, and appending would fire a job once per deploy
// that had ever happened.
func (s *Storage) SaveSchedule(ctx context.Context, sc jobsync.Schedule) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.schedules[sc.ID] = sc
	return nil
}

func (s *Storage) ListSchedules(ctx context.Context) ([]jobsync.Schedule, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]jobsync.Schedule, 0, len(s.schedules))
	for _, sc := range s.schedules {
		out = append(out, sc)
	}
	slices.SortFunc(out, func(a, b jobsync.Schedule) int { return cmpString(a.ID, b.ID) })
	return out, nil
}

func (s *Storage) RemoveSchedule(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.schedules, id)
	return nil
}

func (s *Storage) DueSchedules(ctx context.Context, now time.Time) ([]jobsync.Schedule, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var out []jobsync.Schedule
	for _, sc := range s.schedules {
		if !sc.Paused && !sc.NextRun.After(now) {
			out = append(out, sc)
		}
	}
	slices.SortFunc(out, func(a, b jobsync.Schedule) int { return cmpString(a.ID, b.ID) })
	return out, nil
}

func (s *Storage) AdvanceSchedule(ctx context.Context, id string, lastRun, nextRun time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	sc, ok := s.schedules[id]
	if !ok {
		return nil
	}
	sc.LastRun, sc.NextRun = lastRun, nextRun
	s.schedules[id] = sc
	return nil
}

func cmpString(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func newToken() string {
	var b [16]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// SetSchedulePaused implements jobsync.Schedules.
func (s *Storage) SetSchedulePaused(ctx context.Context, id string, paused bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	sc, ok := s.schedules[id]
	if !ok {
		return nil
	}
	sc.Paused = paused
	s.schedules[id] = sc
	return nil
}

// PauseQueue implements jobsync.QueueControl.
func (s *Storage) PauseQueue(ctx context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.pausedQueues[name] = struct{}{}
	return nil
}

func (s *Storage) ResumeQueue(ctx context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.pausedQueues, name)
	return nil
}

func (s *Storage) PausedQueues(ctx context.Context) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]string, 0, len(s.pausedQueues))
	for name := range s.pausedQueues {
		out = append(out, name)
	}
	slices.Sort(out)
	return out, nil
}
