package storagetest

import (
	"os"
	"testing"
)

// DSN returns the connection string for a driver's tests.
//
// Unset locally it skips, naming the command that fixes it. Unset in CI it
// fails: a conformance suite that silently skips is worse than no suite, because
// it reports green while testing nothing. CI is detected by the CI environment
// variable, which GitHub Actions, GitLab and most others set.
func DSN(t *testing.T, env string) string {
	t.Helper()

	if dsn := os.Getenv(env); dsn != "" {
		return dsn
	}
	if os.Getenv("CI") != "" {
		t.Fatalf("%s is unset: the conformance suite must not be skipped in CI", env)
	}
	t.Skipf("%s is unset; start the backing services with: docker compose up -d", env)
	return ""
}
