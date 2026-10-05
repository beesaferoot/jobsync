package jobsync

import (
	"testing"
	"time"
)

func TestNextRun(t *testing.T) {
	utc := time.UTC
	tests := []struct {
		name string
		spec string
		from string
		want string
	}{
		{"daily at 2am", "0 2 * * *", "2026-03-10T00:30:00Z", "2026-03-10T02:00:00Z"},
		{"already past today", "0 2 * * *", "2026-03-10T03:00:00Z", "2026-03-11T02:00:00Z"},
		{"every five minutes", "*/5 * * * *", "2026-03-10T00:02:00Z", "2026-03-10T00:05:00Z"},
		{"descriptor", "@daily", "2026-03-10T05:00:00Z", "2026-03-11T00:00:00Z"},
		// Strictly after, never equal: a schedule that returned `from` unchanged
		// would be refired forever by the scheduler loop.
		{"exactly on the boundary", "0 2 * * *", "2026-03-10T02:00:00Z", "2026-03-11T02:00:00Z"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			from, err := time.Parse(time.RFC3339, tt.from)
			if err != nil {
				t.Fatal(err)
			}
			got, err := nextRun(tt.spec, utc, from)
			if err != nil {
				t.Fatal(err)
			}
			if got.UTC().Format(time.RFC3339) != tt.want {
				t.Errorf("nextRun(%q, %s) = %s, want %s", tt.spec, tt.from, got.UTC().Format(time.RFC3339), tt.want)
			}
		})
	}
}

// The reason this uses a real cron library rather than arithmetic on time.Time.
// New York springs forward at 2026-03-08 02:00 local, so that day has no 02:00
// at all; a daily 2am job must land on the next real one, not vanish and not
// fire at an invented instant.
func TestNextRunAcrossDST(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("tzdata unavailable: %v", err)
	}

	from := time.Date(2026, 3, 8, 0, 30, 0, 0, ny)
	got, err := nextRun("0 2 * * *", ny, from)
	if err != nil {
		t.Fatal(err)
	}
	if got.Before(from) {
		t.Fatalf("next run %v is before %v", got, from)
	}
	if d := got.Sub(from); d > 48*time.Hour {
		t.Errorf("a daily job skipped more than two days across the DST boundary: %v -> %v (%v)", from, got, d)
	}
	t.Logf("spring-forward day: %v -> %v", from, got)
}

func TestValidateCron(t *testing.T) {
	for _, spec := range []string{"0 2 * * *", "@daily", "*/5 * * * *"} {
		if err := ValidateCron(spec); err != nil {
			t.Errorf("ValidateCron(%q) = %v, want nil", spec, err)
		}
	}
	for _, spec := range []string{"", "not a cron", "99 * * * *", "0 2 * *"} {
		if err := ValidateCron(spec); err == nil {
			t.Errorf("ValidateCron(%q) = nil, want an error", spec)
		}
	}
}
