package redis_test

import (
	"context"
	"testing"

	"github.com/beesaferoot/jobsync"
	redisdriver "github.com/beesaferoot/jobsync/driver/redis"
	"github.com/beesaferoot/jobsync/storagetest"
)

func newTestStore(t *testing.T) jobsync.Storage {
	s, err := redisdriver.Open(storagetest.DSN(t, "TEST_REDIS_URL"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestConformance(t *testing.T)  { storagetest.Run(t, newTestStore) }
func TestDashboardAPI(t *testing.T) { storagetest.RunAPI(t, newTestStore) }
