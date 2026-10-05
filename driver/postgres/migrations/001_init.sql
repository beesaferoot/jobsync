CREATE TABLE IF NOT EXISTS jobsync_jobs (
    id           TEXT        PRIMARY KEY,
    kind         TEXT        NOT NULL,
    queue        TEXT        NOT NULL,
    payload      BYTEA       NOT NULL,
    priority     INT         NOT NULL DEFAULT 0,
    attempt      INT         NOT NULL DEFAULT 0,
    max_attempts INT         NOT NULL,
    state        TEXT        NOT NULL,
    unique_key   TEXT,
    tags         TEXT[]      NOT NULL DEFAULT '{}',
    last_error   TEXT        NOT NULL DEFAULT '',
    transitions  JSONB       NOT NULL DEFAULT '[]',
    created_at   TIMESTAMPTZ NOT NULL,
    scheduled_at TIMESTAMPTZ NOT NULL,
    leased_until TIMESTAMPTZ,
    owner        TEXT        NOT NULL DEFAULT ''
);

-- The fetch path. Partial so the index holds only candidate rows: on a queue
-- that is keeping up, that is a handful of rows out of millions of finished
-- ones, which is what keeps this an index scan rather than a growing one.
CREATE INDEX IF NOT EXISTS jobsync_jobs_fetch
    ON jobsync_jobs (queue, priority, scheduled_at)
    WHERE state = 'available';

CREATE INDEX IF NOT EXISTS jobsync_jobs_reclaim
    ON jobsync_jobs (leased_until)
    WHERE state = 'running';

-- Partial on NOT NULL: a terminal job has its unique_key cleared to NULL, and
-- Postgres lets any number of NULLs coexist in a unique index.
CREATE UNIQUE INDEX IF NOT EXISTS jobsync_jobs_unique_key
    ON jobsync_jobs (unique_key)
    WHERE unique_key IS NOT NULL;

-- Dashboard listing, which filters by state and sorts by recency.
CREATE INDEX IF NOT EXISTS jobsync_jobs_browse
    ON jobsync_jobs (state, created_at DESC);

CREATE TABLE IF NOT EXISTS jobsync_locks (
    key        TEXT        PRIMARY KEY,
    token      TEXT        NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);
