CREATE TABLE IF NOT EXISTS jobsync_schedules (
    id       TEXT        PRIMARY KEY,
    kind     TEXT        NOT NULL,
    queue    TEXT        NOT NULL,
    payload  BYTEA       NOT NULL,
    cron     TEXT        NOT NULL,
    timezone TEXT        NOT NULL,
    last_run TIMESTAMPTZ,
    next_run TIMESTAMPTZ NOT NULL,
    paused   BOOLEAN     NOT NULL DEFAULT false
);

CREATE INDEX IF NOT EXISTS jobsync_schedules_due
    ON jobsync_schedules (next_run) WHERE NOT paused;
