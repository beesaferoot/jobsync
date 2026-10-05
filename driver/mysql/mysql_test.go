package mysql_test

import (
	"context"
	"testing"

	"github.com/beesaferoot/jobsync"
	"github.com/beesaferoot/jobsync/driver/mysql"
	"github.com/beesaferoot/jobsync/storagetest"
)

func newTestStore(t *testing.T) jobsync.Storage {
	ctx := context.Background()
	s, err := mysql.Open(ctx, storagetest.DSN(t, "TEST_MYSQL_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Truncate(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestConformance(t *testing.T)  { storagetest.Run(t, newTestStore) }
func TestDashboardAPI(t *testing.T) { storagetest.RunAPI(t, newTestStore) }
