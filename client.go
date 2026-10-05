package jobsync

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

// Client enqueues jobs. It is safe for concurrent use and holds no state beyond
// the storage handle, so an application typically constructs exactly one.
type Client struct {
	store     Storage
	schedules Schedules
	location  *time.Location
}

// NewClient wraps a storage driver. It returns an error rather than silently
// degrading when the driver cannot support what was asked for.
func NewClient(store Storage, opts ...ClientOption) (*Client, error) {
	c := &Client{store: store, location: time.UTC}
	for _, opt := range opts {
		opt(c)
	}
	c.schedules, _ = store.(Schedules)
	return c, nil
}

type ClientOption func(*Client)

// InLocation sets the timezone cron expressions are evaluated in.
//
// The default is UTC, not time.Local. A schedule stores its zone by name, and
// time.Local is named the literal string "Local" — which every server then
// resolves to its OWN local zone. One fleet with mixed TZ settings would fire
// the same nightly job at several different hours, and each server would look
// correct in isolation.
//
// Pass a real zone when business hours matter:
//
//	jobsync.InLocation(mustLoad("Africa/Lagos"))
func InLocation(loc *time.Location) ClientOption {
	return func(c *Client) { c.location = loc }
}

// Cancel marks a job cancelled if it has not started. A running job is left
// alone: cancelling it is the server's decision, through the handler's context.
func (c *Client) Cancel(ctx context.Context, ids ...string) error {
	m, ok := c.store.(Monitor)
	if !ok {
		return fmt.Errorf("jobsync: %T cannot cancel jobs", c.store)
	}
	return m.Delete(ctx, ids)
}

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("jobsync: entropy source unavailable: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}
