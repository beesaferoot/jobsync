package postgres_test

import (
	"context"
	"testing"

	"github.com/beesaferoot/jobsync"
	"github.com/beesaferoot/jobsync/driver/postgres"
	"github.com/beesaferoot/jobsync/storagetest"
)

func newTestStore(t *testing.T) jobsync.Storage {
	ctx := context.Background()
	s, err := postgres.Open(ctx, storagetest.DSN(t, "TEST_POSTGRES_URL"))
	if err != nil {
		t.Fatal(err)
	}
	// Each subtest gets an empty table. Truncating beats a fresh database per
	// test: it is fast, and it exercises the real indexes rather than a pristine
	// planner state no production system ever has.
	if err := s.Truncate(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestConformance(t *testing.T)  { storagetest.Run(t, newTestStore) }
func TestDashboardAPI(t *testing.T) { storagetest.RunAPI(t, newTestStore) }
