// Package redis is the Redis jobsync storage driver.
//
// Every mutation is a Lua script. Each one is a read-modify-write across several
// keys — pop from a priority set, write the job hash, add to the lease set — and
// a pipeline would let another server interleave in the middle of that. Lua runs
// atomically on the server, which is the only way Fetch can promise exclusivity.
//
// Two places where Redis cannot match a SQL driver, both documented in the
// contract rather than faked here:
//
//   - Priority is clamped to 0..9 (see Levels). A sorted set score encodes one
//     dimension, so ordering by priority while filtering by due time needs one
//     set per level, and an unbounded number of levels is unbounded by user
//     input.
//   - Monitor.ListJobs cannot serve Filter.Search; it returns
//     jobsync.ErrUnsupportedFilter and the dashboard disables the control.
package redis

import (
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/hex"
	"slices"
	"strconv"
	"time"

	"github.com/beesaferoot/jobsync"
	"github.com/redis/go-redis/v9"
)

// Levels is how many priority levels this driver distinguishes. Priority is
// clamped into [0, Levels). Nine covers "urgent / normal / batch" with room to
// spare; more costs a sorted set and a Lua loop iteration per level per fetch.
const Levels = 10

//go:embed fetch.lua
var fetchScript string

//go:embed finish.lua
var finishScript string

//go:embed reclaim.lua
var reclaimScript string

type Storage struct {
	rdb    *redis.Client
	prefix string
	// Retention is the TTL put on a job hash when it reaches a terminal state.
	// Redis expires it; unlike the SQL drivers there is no janitor to run.
	Retention time.Duration

	fetch, finish, reclaim *redis.Script
}

// Open connects using a redis:// URL. Use New to supply a client you have
// already configured — a cluster client, or one with custom timeouts.
func Open(url string) (*Storage, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	return New(redis.NewClient(opt)), nil
}

// New wraps an existing Redis client.
func New(rdb *redis.Client) *Storage {
	return &Storage{
		rdb:       rdb,
		prefix:    "jobsync",
		Retention: 7 * 24 * time.Hour,
		fetch:     redis.NewScript(fetchScript),
		finish:    redis.NewScript(finishScript),
		reclaim:   redis.NewScript(reclaimScript),
	}
}

func (s *Storage) Close() error { return s.rdb.Close() }

// Key layout. The {q:<queue>} hash tag is mandatory, not cosmetic: on a Redis
// Cluster, keys without a shared tag land in different slots and every
// multi-key Lua script fails with CROSSSLOT. That never shows up in single-node
// testing, so it has to be right by construction.
//
//	jobsync:{q:<queue>}:p<level>   ZSET  job id -> scheduled_at_ms
//	jobsync:{q:<queue>}:leases     ZSET  job id -> lease_expiry_ms
//	jobsync:{q:<queue>}:job:<id>   HASH  the job
//	jobsync:{q:<queue>}:unique:<k> STR   job id
//	jobsync:queues                 SET   every queue name ever seen
func (s *Storage) levelKey(queue string, level int) string {
	return s.prefix + ":{q:" + queue + "}:p" + strconv.Itoa(level)
}

func (s *Storage) leaseKey(queue string) string {
	return s.prefix + ":{q:" + queue + "}:leases"
}

func (s *Storage) jobKey(queue, id string) string {
	return s.prefix + ":{q:" + queue + "}:job:" + id
}

func (s *Storage) uniqueKey(queue, key string) string {
	return s.prefix + ":{q:" + queue + "}:unique:" + key
}

func (s *Storage) queuesKey() string { return s.prefix + ":queues" }

func (s *Storage) qPrefix(queue string) string { return s.prefix + ":{q:" + queue + "}:" }

func (s *Storage) retryingKey(queue string) string { return s.qPrefix(queue) + "retrying" }

func (s *Storage) allKey(queue string) string { return s.qPrefix(queue) + "all" }

func (s *Storage) doneKey(queue string, state jobsync.State) string {
	return s.qPrefix(queue) + "done:" + string(state)
}

func (s *Storage) histKey(queue, id string) string { return s.qPrefix(queue) + "hist:" + id }

func (s *Storage) queues(ctx context.Context) ([]string, error) {
	return s.rdb.SMembers(ctx, s.queuesKey()).Result()
}

func level(priority int) int {
	if priority < 0 {
		return 0
	}
	if priority >= Levels {
		return Levels - 1
	}
	return priority
}

func ms(t time.Time) int64 { return t.UnixMilli() }

func (s *Storage) Enqueue(ctx context.Context, jobs []*jobsync.Job) error {
	for _, j := range jobs {
		if err := s.enqueueOne(ctx, j); err != nil {
			return err
		}
	}
	return nil
}

//go:embed enqueue.lua
var enqueueScript string

func (s *Storage) enqueueOne(ctx context.Context, j *jobsync.Job) error {
	keys := []string{
		s.jobKey(j.Queue, j.ID),
		s.levelKey(j.Queue, level(j.Priority)),
		s.queuesKey(),
		"",
		s.allKey(j.Queue),
	}
	if j.UniqueKey != "" {
		keys[3] = s.uniqueKey(j.Queue, j.UniqueKey)
	}

	return redis.NewScript(enqueueScript).Run(ctx, s.rdb, keys,
		j.ID, j.Kind, j.Queue, string(j.Payload), j.Priority, j.Attempt,
		j.MaxAttempts, j.UniqueKey, encodeTags(j.Tags),
		ms(j.CreatedAt), ms(j.ScheduledAt),
	).Err()
}

func (s *Storage) Fetch(ctx context.Context, queues []string, n int, owner string, lease time.Duration) ([]*jobsync.Job, error) {
	var out []*jobsync.Job

	// Re-read on every Fetch rather than caching: the set is tiny, polls are
	// seconds apart, and a server acting on a stale copy would keep draining a
	// queue an operator just stopped.
	paused, err := s.rdb.SMembers(ctx, s.pausedKey()).Result()
	if err != nil {
		return nil, err
	}

	// Queues are polled in the order given, highest priority first, so a later
	// queue only gets a look once the earlier ones are drained.
	for _, queue := range queues {
		if len(out) >= n {
			break
		}
		if slices.Contains(paused, queue) {
			continue
		}
		keys := make([]string, 0, Levels+1)
		for l := range Levels {
			keys = append(keys, s.levelKey(queue, l))
		}
		keys = append(keys, s.leaseKey(queue), s.retryingKey(queue))

		res, err := s.fetch.Run(ctx, s.rdb, keys,
			n-len(out), owner, lease.Milliseconds(),
			s.jobKey(queue, ""), s.qPrefix(queue),
		).Result()
		if err != nil {
			return nil, err
		}

		jobs, err := decodeJobs(res)
		if err != nil {
			return nil, err
		}
		out = append(out, jobs...)
	}
	return out, nil
}

func (s *Storage) Extend(ctx context.Context, ids []string, owner string, lease time.Duration) error {
	queues, err := s.rdb.SMembers(ctx, s.queuesKey()).Result()
	if err != nil {
		return err
	}
	// A job id does not say which queue it is in, and the contract does not pass
	// one. Checking each queue is correct and cheap: queue counts are small, and
	// the owner guard makes a miss a no-op.
	pipe := s.rdb.Pipeline()
	for _, queue := range queues {
		for _, id := range ids {
			pipe.Eval(ctx, `
				if redis.call('HGET', KEYS[1], 'owner') == ARGV[1] then
					local t = redis.call('TIME')
					local expiry = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
					                 + tonumber(ARGV[2])
					redis.call('HSET', KEYS[1], 'leased_until', expiry)
					redis.call('ZADD', KEYS[2], expiry, ARGV[3])
				end`,
				[]string{s.jobKey(queue, id), s.leaseKey(queue)},
				owner, lease.Milliseconds(), id)
		}
	}
	_, err = pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return err
	}
	return nil
}

func (s *Storage) Finish(ctx context.Context, id, owner string, r jobsync.Result) error {
	queues, err := s.queues(ctx)
	if err != nil {
		return err
	}

	retryAt := int64(0)
	if r.State == jobsync.StateRetrying {
		retryAt = ms(r.RetryAt)
	}

	for _, queue := range queues {
		keys := []string{
			s.jobKey(queue, id),
			s.leaseKey(queue),
			s.retryingKey(queue),
			s.qPrefix(queue),
		}
		err := s.finish.Run(ctx, s.rdb, keys,
			owner, string(r.State), r.Err, retryAt,
			s.Retention.Milliseconds(), id,
		).Err()
		if err != nil && err != redis.Nil {
			return err
		}
	}
	return nil
}

func (s *Storage) Reclaim(ctx context.Context) (int, error) {
	queues, err := s.queues(ctx)
	if err != nil {
		return 0, err
	}

	var total int
	for _, queue := range queues {
		keys := []string{s.leaseKey(queue), s.qPrefix(queue)}
		res, err := s.reclaim.Run(ctx, s.rdb, keys).Result()
		if err != nil && err != redis.Nil {
			return total, err
		}
		if n, ok := res.(int64); ok {
			total += int(n)
		}
	}
	return total, nil
}

// Lock implements jobsync.Locker: SET NX with a TTL, and a token-checked delete
// so a server whose lock already expired cannot release the one a different
// server now holds.
func (s *Storage) Lock(ctx context.Context, key string, ttl time.Duration) (string, error) {
	token := newToken()
	ok, err := s.rdb.SetNX(ctx, s.prefix+":lock:"+key, token, ttl).Result()
	if err != nil {
		return "", err
	}
	if !ok {
		return "", nil
	}
	return token, nil
}

func (s *Storage) Unlock(ctx context.Context, key, token string) error {
	return s.rdb.Eval(ctx, `
		if redis.call('GET', KEYS[1]) == ARGV[1] then
			return redis.call('DEL', KEYS[1])
		end
		return 0`, []string{s.prefix + ":lock:" + key}, token).Err()
}

// FlushAll empties the keyspace. For tests only.
func (s *Storage) FlushAll(ctx context.Context) error {
	return s.rdb.FlushDB(ctx).Err()
}

func newToken() string {
	var b [16]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
