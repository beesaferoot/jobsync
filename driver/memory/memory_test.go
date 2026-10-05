package memory_test

import (
	"testing"

	"github.com/beesaferoot/jobsync"
	"github.com/beesaferoot/jobsync/driver/memory"
	"github.com/beesaferoot/jobsync/storagetest"
)

func newTestStore(t *testing.T) jobsync.Storage {
	s := memory.New()
	t.Cleanup(func() { s.Close() })
	return s
}

func TestConformance(t *testing.T)  { storagetest.Run(t, newTestStore) }
func TestDashboardAPI(t *testing.T) { storagetest.RunAPI(t, newTestStore) }
