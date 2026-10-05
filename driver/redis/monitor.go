package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/beesaferoot/jobsync"
	"github.com/redis/go-redis/v9"
)

// The dashboard's read side. Redis has no WHERE clause, so every question the
// Monitor answers has to be an index that the write path maintains:
//
//	{q}:p<level>     available, scored by due time   -> Scheduled / Enqueued
//	{q}:retrying     retries,   scored by due time   -> Retrying
//	{q}:leases       running,   scored by lease end  -> Running
//	{q}:done:<state> terminal,  scored by finish     -> Succeeded / Dead / Cancelled, Throughput
//	{q}:all          everything, scored by creation  -> ListJobs paging
//
// Counts and Throughput are therefore pure ZCARD/ZCOUNT and cost nothing.
// ListJobs is the expensive one: with no server-side predicate it reads candidate
// hashes and filters in Go. That is fine for a dashboard page and wrong for a
// hot path, which is why nothing but the dashboard calls it.

func (s *Storage) Counts(ctx context.Context) (jobsync.Counts, error) {
	queues, err := s.queues(ctx)
	if err != nil {
		return jobsync.Counts{}, err
	}
	now := strconv.FormatInt(time.Now().UnixMilli(), 10)

	var c jobsync.Counts
	for _, q := range queues {
		for l := range Levels {
			key := s.levelKey(q, l)
			due, err := s.rdb.ZCount(ctx, key, "-inf", now).Result()
			if err != nil {
				return c, err
			}
			total, err := s.rdb.ZCard(ctx, key).Result()
			if err != nil {
				return c, err
			}
			c.Enqueued += due
			c.Scheduled += total - due
		}

		// A retry that has come due reads as Enqueued, same as the SQL drivers:
		// the contract keys Enqueued off the due time, not the attempt count. So
		// only the still-pending part of the retry index counts as Retrying, and
		// it is subtracted from Scheduled, which counted it a moment ago.
		retrying, err := s.rdb.ZCount(ctx, s.retryingKey(q), now, "+inf").Result()
		if err != nil {
			return c, err
		}
		c.Retrying += retrying
		c.Scheduled -= retrying

		running, err := s.rdb.ZCard(ctx, s.leaseKey(q)).Result()
		if err != nil {
			return c, err
		}
		c.Running += running

		for state, into := range map[jobsync.State]*int64{
			jobsync.StateSucceeded: &c.Succeeded,
			jobsync.StateDead:      &c.Dead,
			jobsync.StateCancelled: &c.Cancelled,
		} {
			n, err := s.rdb.ZCard(ctx, s.doneKey(q, state)).Result()
			if err != nil {
				return c, err
			}
			*into += n
		}
	}
	return c, nil
}

func (s *Storage) QueueStats(ctx context.Context) ([]jobsync.QueueStat, error) {
	queues, err := s.queues(ctx)
	if err != nil {
		return nil, err
	}
	nowMs := time.Now().UnixMilli()
	now := strconv.FormatInt(nowMs, 10)

	paused, err := s.rdb.SMembers(ctx, s.pausedKey()).Result()
	if err != nil {
		return nil, err
	}

	var out []jobsync.QueueStat
	for _, q := range queues {
		// An operator cannot tell a paused queue from an idle one, and they look
		// identical while meaning opposite things.
		stat := jobsync.QueueStat{Name: q, Paused: slices.Contains(paused, q)}
		for l := range Levels {
			key := s.levelKey(q, l)
			due, err := s.rdb.ZCount(ctx, key, "-inf", now).Result()
			if err != nil {
				return nil, err
			}
			stat.Enqueued += due

			// The head of the queue is the lowest due score, and its age is the
			// number worth alerting on: ten jobs two seconds old is healthy, one
			// job forty minutes old is not.
			if due > 0 {
				head, err := s.rdb.ZRangeWithScores(ctx, key, 0, 0).Result()
				if err != nil {
					return nil, err
				}
				if len(head) > 0 {
					if age := time.Duration(nowMs-int64(head[0].Score)) * time.Millisecond; age > stat.OldestEnqueued {
						stat.OldestEnqueued = age
					}
				}
			}
		}
		if stat.Running, err = s.rdb.ZCard(ctx, s.leaseKey(q)).Result(); err != nil {
			return nil, err
		}
		out = append(out, stat)
	}
	slices.SortFunc(out, func(a, b jobsync.QueueStat) int {
		return slices.Compare([]string{a.Name}, []string{b.Name})
	})
	return out, nil
}

// ListJobs reads candidate ids from {q}:all and filters in Go.
//
// Search is declined rather than approximated: answering it would mean indexing
// every job's kind, id and error text, and quietly returning unfiltered results
// instead would show an operator the wrong jobs during an incident.
func (s *Storage) ListJobs(ctx context.Context, f jobsync.Filter) ([]*jobsync.Job, int64, error) {
	if f.Search != "" {
		return nil, 0, fmt.Errorf("jobsync/redis: Search: %w", jobsync.ErrUnsupportedFilter)
	}

	queues, err := s.queues(ctx)
	if err != nil {
		return nil, 0, err
	}
	if len(f.Queues) > 0 {
		queues = slices.DeleteFunc(queues, func(q string) bool {
			return !slices.Contains(f.Queues, q)
		})
	}

	now := time.Now()
	var matched []*jobsync.Job
	for _, q := range queues {
		ids, err := s.rdb.ZRevRange(ctx, s.allKey(q), 0, -1).Result()
		if err != nil {
			return nil, 0, err
		}
		for _, id := range ids {
			fields, err := s.rdb.HGetAll(ctx, s.jobKey(q, id)).Result()
			if err != nil {
				return nil, 0, err
			}
			if len(fields) == 0 {
				continue // expired by retention; the index entry is stale
			}
			j := decodeJob(fields)
			j.State = derive(j, now)
			if keep(j, f) {
				matched = append(matched, j)
			}
		}
	}

	// Newest first, then id so the ordering is total. Without the tiebreak two
	// jobs created in the same millisecond can swap between pages, and the pager
	// skips one while repeating another.
	slices.SortFunc(matched, func(a, b *jobsync.Job) int {
		if c := b.CreatedAt.Compare(a.CreatedAt); c != 0 {
			return c
		}
		return slices.Compare([]string{a.ID}, []string{b.ID})
	})

	total := int64(len(matched))
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

// derive turns the stored state into the contract's seven, matching the SQL
// drivers exactly: available and due is Enqueued, available and pending is
// Retrying if it has already failed and Scheduled if it has not.
func derive(j *jobsync.Job, now time.Time) jobsync.State {
	if j.State != jobsync.State("available") {
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

func keep(j *jobsync.Job, f jobsync.Filter) bool {
	if len(f.States) > 0 && !slices.Contains(f.States, j.State) {
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
	return true
}

func (s *Storage) JobHistory(ctx context.Context, id string) (*jobsync.Job, []jobsync.Transition, error) {
	queues, err := s.queues(ctx)
	if err != nil {
		return nil, nil, err
	}

	for _, q := range queues {
		fields, err := s.rdb.HGetAll(ctx, s.jobKey(q, id)).Result()
		if err != nil {
			return nil, nil, err
		}
		if len(fields) == 0 {
			continue
		}
		j := decodeJob(fields)
		j.State = derive(j, time.Now())

		raw, err := s.rdb.LRange(ctx, s.histKey(q, id), 0, -1).Result()
		if err != nil {
			return nil, nil, err
		}
		transitions := make([]jobsync.Transition, 0, len(raw))
		for _, entry := range raw {
			var t struct {
				State  string  `json:"state"`
				At     float64 `json:"at"`
				Reason string  `json:"reason"`
			}
			if err := json.Unmarshal([]byte(entry), &t); err != nil {
				return nil, nil, fmt.Errorf("decode transition for %s: %w", id, err)
			}
			transitions = append(transitions, jobsync.Transition{
				State:  jobsync.State(t.State),
				At:     time.UnixMilli(int64(t.At)),
				Reason: t.Reason,
			})
		}
		return j, transitions, nil
	}
	return nil, nil, fmt.Errorf("jobsync: no job %s", id)
}

// Requeue skips running jobs: one is not stuck, it is working, and requeueing it
// would run the same work twice.
func (s *Storage) Requeue(ctx context.Context, ids []string) error {
	queues, err := s.queues(ctx)
	if err != nil {
		return err
	}
	now := time.Now()
	due := now.Add(-time.Second).UnixMilli()

	for _, q := range queues {
		for _, id := range ids {
			fields, err := s.rdb.HGetAll(ctx, s.jobKey(q, id)).Result()
			if err != nil {
				return err
			}
			if len(fields) == 0 || fields["state"] == "running" {
				continue
			}
			j := decodeJob(fields)
			pipe := s.rdb.TxPipeline()
			pipe.HSet(ctx, s.jobKey(q, id),
				"state", "available", "attempt", 0, "last_error", "",
				"scheduled_at", due, "finished_at", 0, "owner", "", "leased_until", "0")
			pipe.ZAdd(ctx, s.levelKey(q, level(j.Priority)), redis.Z{Score: float64(due), Member: id})
			pipe.ZRem(ctx, s.retryingKey(q), id)
			for _, st := range []jobsync.State{jobsync.StateSucceeded, jobsync.StateDead, jobsync.StateCancelled} {
				pipe.ZRem(ctx, s.doneKey(q, st), id)
			}
			// Retention put a TTL on the hash when it finished; requeueing makes it
			// live again, so the TTL has to come off or the job vanishes mid-flight.
			pipe.Persist(ctx, s.jobKey(q, id))
			pipe.Persist(ctx, s.histKey(q, id))
			pipe.RPush(ctx, s.histKey(q, id),
				`{"state":"enqueued","at":`+strconv.FormatInt(now.UnixMilli(), 10)+`,"reason":"requeued from the dashboard"}`)
			if _, err := pipe.Exec(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Storage) Delete(ctx context.Context, ids []string) error {
	queues, err := s.queues(ctx)
	if err != nil {
		return err
	}

	for _, q := range queues {
		for _, id := range ids {
			state, err := s.rdb.HGet(ctx, s.jobKey(q, id), "state").Result()
			if err == redis.Nil {
				continue
			}
			if err != nil {
				return err
			}
			if state == "running" {
				continue
			}
			pipe := s.rdb.TxPipeline()
			pipe.Del(ctx, s.jobKey(q, id), s.histKey(q, id))
			pipe.ZRem(ctx, s.allKey(q), id)
			pipe.ZRem(ctx, s.retryingKey(q), id)
			pipe.ZRem(ctx, s.leaseKey(q), id)
			for l := range Levels {
				pipe.ZRem(ctx, s.levelKey(q, l), id)
			}
			for _, st := range []jobsync.State{jobsync.StateSucceeded, jobsync.StateDead, jobsync.StateCancelled} {
				pipe.ZRem(ctx, s.doneKey(q, st), id)
			}
			if _, err := pipe.Exec(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

// Throughput is a ZCOUNT per bucket over the done indexes, which are already
// scored by finish time. No scan, no history table.
func (s *Storage) Throughput(ctx context.Context, since time.Time, bucket time.Duration) ([]jobsync.Bucket, error) {
	if bucket <= 0 {
		return nil, fmt.Errorf("jobsync: throughput bucket must be positive")
	}
	queues, err := s.queues(ctx)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	buckets := map[int64]*jobsync.Bucket{}
	for _, q := range queues {
		for _, st := range []jobsync.State{jobsync.StateSucceeded, jobsync.StateDead, jobsync.StateCancelled} {
			entries, err := s.rdb.ZRangeByScoreWithScores(ctx, s.doneKey(q, st), &redis.ZRangeBy{
				Min: strconv.FormatInt(since.UnixMilli(), 10),
				Max: strconv.FormatInt(now.UnixMilli(), 10),
			}).Result()
			if err != nil {
				return nil, err
			}
			for _, e := range entries {
				at := time.UnixMilli(int64(e.Score)).Truncate(bucket)
				b, ok := buckets[at.UnixNano()]
				if !ok {
					b = &jobsync.Bucket{At: at}
					buckets[at.UnixNano()] = b
				}
				if st == jobsync.StateSucceeded {
					b.Succeeded++
				} else {
					b.Failed++
				}
			}
		}
	}

	out := make([]jobsync.Bucket, 0, len(buckets))
	for _, b := range buckets {
		out = append(out, *b)
	}
	slices.SortFunc(out, func(a, b jobsync.Bucket) int { return a.At.Compare(b.At) })
	return out, nil
}

func (s *Storage) Heartbeat(ctx context.Context, info jobsync.ServerInfo, ttl time.Duration) error {
	info.HeartbeatAt = time.Now()
	blob, err := json.Marshal(info)
	if err != nil {
		return err
	}
	// One key per server with its own TTL, so a server that dies disappears on
	// its own. A single hash would need a janitor to stay honest.
	return s.rdb.Set(ctx, s.prefix+":server:"+info.ID, blob, ttl).Err()
}

func (s *Storage) Servers(ctx context.Context) ([]jobsync.ServerInfo, error) {
	var out []jobsync.ServerInfo
	iter := s.rdb.Scan(ctx, 0, s.prefix+":server:*", 100).Iterator()
	for iter.Next(ctx) {
		blob, err := s.rdb.Get(ctx, iter.Val()).Bytes()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			return nil, err
		}
		var info jobsync.ServerInfo
		if err := json.Unmarshal(blob, &info); err != nil {
			return nil, fmt.Errorf("decode server %s: %w", iter.Val(), err)
		}
		out = append(out, info)
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	slices.SortFunc(out, func(a, b jobsync.ServerInfo) int {
		return slices.Compare([]string{a.ID}, []string{b.ID})
	})
	return out, nil
}
