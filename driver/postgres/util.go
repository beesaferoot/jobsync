package postgres

import (
	"crypto/rand"
	"encoding/hex"
	"slices"

	"github.com/beesaferoot/jobsync"
)

func sortByPriority(jobs []*jobsync.Job) {
	slices.SortFunc(jobs, func(a, b *jobsync.Job) int {
		if a.Priority != b.Priority {
			return a.Priority - b.Priority
		}
		return a.ScheduledAt.Compare(b.ScheduledAt)
	})
}

func newToken() string {
	var b [16]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
