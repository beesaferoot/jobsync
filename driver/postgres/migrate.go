package postgres

import (
	"context"
	"embed"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Migrate brings the schema up to date. It is safe to call from every process
// on every boot: the advisory lock serialises concurrent callers, and each file
// runs at most once.
//
// There is no migration framework here on purpose. A library that drags one into
// every consumer's dependency graph, and fights whatever the application already
// uses, is a worse deal than forty lines.
func Migrate(ctx context.Context, conn *pgx.Conn) error {
	// Chosen once, arbitrary, never changed: two processes migrating at the same
	// time must agree on the number or the lock protects nothing.
	const lockID = 0x6a6f6273796e63

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", lockID); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	defer conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", lockID)

	_, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS jobsync_migrations (
			name       TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`)
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
		var applied bool
		err := conn.QueryRow(ctx,
			"SELECT EXISTS (SELECT 1 FROM jobsync_migrations WHERE name = $1)", name).Scan(&applied)
		if err != nil {
			return err
		}
		if applied {
			continue
		}

		sql, err := migrations.ReadFile("migrations/" + name)
		if err != nil {
			return err
		}
		if _, err := conn.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
		if _, err := conn.Exec(ctx, "INSERT INTO jobsync_migrations (name) VALUES ($1)", name); err != nil {
			return err
		}
	}
	return nil
}
