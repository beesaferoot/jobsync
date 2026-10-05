package mysql

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Migrate brings the schema up to date. Safe to call from every process on
// every boot: GET_LOCK serialises concurrent callers and each file runs once.
//
// Unlike Postgres this cannot wrap the DDL in a transaction — MySQL commits
// implicitly on every DDL statement, so a half-applied migration file stays
// half-applied. That is why each file here holds exactly one statement.
func Migrate(ctx context.Context, db *sql.DB) error {
	if err := checkVersion(ctx, db); err != nil {
		return err
	}

	// GET_LOCK is connection-scoped, so the lock and its release must run on the
	// same connection. A *sql.DB hands out arbitrary ones from the pool.
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	var locked bool
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK('jobsync_migrate', 30)").Scan(&locked); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	if !locked {
		return fmt.Errorf("jobsync: timed out waiting for the migration lock")
	}
	defer conn.ExecContext(ctx, "SELECT RELEASE_LOCK('jobsync_migrate')")

	_, err = conn.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS jobsync_migrations (
			name       VARCHAR(255) NOT NULL PRIMARY KEY,
			applied_at DATETIME(6)  NOT NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
	if err != nil {
		return fmt.Errorf("create migrations table: %w", err)
	}

	entries, err := migrations.ReadDir("migrations")
	if err != nil {
		return err
	}
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	sort.Strings(names)

	for _, name := range names {
		var applied int
		err := conn.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM jobsync_migrations WHERE name = ?", name).Scan(&applied)
		if err != nil {
			return err
		}
		if applied > 0 {
			continue
		}

		stmt, err := migrations.ReadFile("migrations/" + name)
		if err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, string(stmt)); err != nil {
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
		_, err = conn.ExecContext(ctx,
			"INSERT INTO jobsync_migrations (name, applied_at) VALUES (?, UTC_TIMESTAMP(6))", name)
		if err != nil {
			return err
		}
	}
	return nil
}

// checkVersion refuses a server too old for SKIP LOCKED. Failing clearly at boot
// beats the alternative: on an older server the fetch query is a syntax error on
// every poll, which surfaces as a queue that silently never drains.
func checkVersion(ctx context.Context, db *sql.DB) error {
	var version string
	if err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
		return err
	}

	major, minor, err := parseVersion(version)
	if err != nil {
		return err
	}

	if strings.Contains(strings.ToLower(version), "mariadb") {
		if major > 10 || (major == 10 && minor >= 6) {
			return nil
		}
		return fmt.Errorf("jobsync: MariaDB %s is too old; 10.6+ is required for SELECT ... FOR UPDATE SKIP LOCKED", version)
	}
	if major >= 8 {
		return nil
	}
	return fmt.Errorf("jobsync: MySQL %s is too old; 8.0+ is required for SELECT ... FOR UPDATE SKIP LOCKED", version)
}

func parseVersion(v string) (major, minor int, err error) {
	parts := strings.SplitN(v, ".", 3)
	if len(parts) < 2 {
		return 0, 0, fmt.Errorf("jobsync: cannot read server version %q", v)
	}
	if major, err = strconv.Atoi(parts[0]); err != nil {
		return 0, 0, fmt.Errorf("jobsync: cannot read server version %q", v)
	}
	minor, err = strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, fmt.Errorf("jobsync: cannot read server version %q", v)
	}
	return major, minor, nil
}
