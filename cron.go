package jobsync

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

// Cron parsing lives here, never in a driver: adding a storage backend must
// never mean reimplementing a scheduler. Drivers store the spec as opaque text
// and the computed NextRun as a timestamp.
//
// robfig/cron rather than a hand-rolled parser, for one reason that is easy to
// underestimate: DST. "0 2 * * *" on the night a zone springs forward has no
// 02:00, and on the night it falls back has two. Getting that right by hand is a
// trap, and getting it wrong means a nightly job that silently skips a day or
// fires twice.
var cronParser = cron.NewParser(
	cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
)

// nextRun computes the next firing time of spec strictly after from, evaluated
// in loc.
func nextRun(spec string, loc *time.Location, from time.Time) (time.Time, error) {
	schedule, err := parseCron(spec, loc)
	if err != nil {
		return time.Time{}, err
	}
	return schedule.Next(from.In(loc)), nil
}

func parseCron(spec string, loc *time.Location) (cron.Schedule, error) {
	if spec == "" {
		return nil, fmt.Errorf("jobsync: empty cron spec")
	}
	schedule, err := cronParser.Parse(spec)
	if err != nil {
		return nil, fmt.Errorf("jobsync: bad cron spec %q: %w", spec, err)
	}
	return schedule, nil
}

// ValidateCron reports whether spec is a usable cron expression. Callers that
// build specs from configuration should use it at startup rather than finding
// out at the first fire.
func ValidateCron(spec string) error {
	_, err := parseCron(spec, time.UTC)
	return err
}
