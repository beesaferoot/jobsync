package redis

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/beesaferoot/jobsync"
)

// Redis hashes are flat string maps, so the job has to be encoded and decoded by
// hand. This is the one place in the driver where a field is addressed by name
// at runtime; everything downstream of decodeJob works on a typed *jobsync.Job.

const tagSep = "\x1f" // unit separator: cannot appear in a sane tag

func encodeTags(tags []string) string { return strings.Join(tags, tagSep) }

func decodeTags(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, tagSep)
}

// decodeJobs converts the nested reply of fetch.lua — a list of HGETALL results,
// each a flat [k, v, k, v, ...] array — into jobs.
func decodeJobs(res any) ([]*jobsync.Job, error) {
	rows, ok := res.([]any)
	if !ok {
		return nil, fmt.Errorf("jobsync/redis: fetch returned %T, want a list", res)
	}

	jobs := make([]*jobsync.Job, 0, len(rows))
	for _, row := range rows {
		flat, ok := row.([]any)
		if !ok {
			return nil, fmt.Errorf("jobsync/redis: fetch row is %T, want a list", row)
		}
		fields := make(map[string]string, len(flat)/2)
		for i := 0; i+1 < len(flat); i += 2 {
			fields[toString(flat[i])] = toString(flat[i+1])
		}
		jobs = append(jobs, decodeJob(fields))
	}
	return jobs, nil
}

func decodeJob(f map[string]string) *jobsync.Job {
	return &jobsync.Job{
		ID:          f["id"],
		Kind:        f["kind"],
		Queue:       f["queue"],
		Payload:     []byte(f["payload"]),
		Priority:    atoi(f["priority"]),
		Attempt:     atoi(f["attempt"]),
		MaxAttempts: atoi(f["max_attempts"]),
		State:       jobsync.State(f["state"]),
		UniqueKey:   f["unique_key"],
		Tags:        decodeTags(f["tags"]),
		LastError:   f["last_error"],
		Owner:       f["owner"],
		CreatedAt:   fromMillis(f["created_at"]),
		ScheduledAt: fromMillis(f["scheduled_at"]),
		LeasedUntil: fromMillis(f["leased_until"]),
	}
}

// toString handles Redis replies arriving as []byte or string depending on the
// RESP protocol version in use.
func toString(v any) string {
	switch s := v.(type) {
	case string:
		return s
	case []byte:
		return string(s)
	default:
		return fmt.Sprint(v)
	}
}

func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

// fromMillis returns the zero time for "0" and "", which is how the Lua scripts
// spell "no lease" — distinct from the epoch, and the thing Job.LeasedUntil
// means by its zero value.
func fromMillis(s string) time.Time {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n == 0 {
		return time.Time{}
	}
	return time.UnixMilli(n)
}
