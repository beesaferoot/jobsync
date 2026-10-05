ALTER TABLE jobsync_jobs ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ;

-- Throughput reads this; without the index it is a sequential scan over every
-- job ever run, on a page an operator reloads during an incident.
CREATE INDEX IF NOT EXISTS jobsync_jobs_finished
    ON jobsync_jobs (finished_at) WHERE finished_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS jobsync_servers (
    id           TEXT        PRIMARY KEY,
    hostname     TEXT        NOT NULL,
    queues       TEXT[]      NOT NULL DEFAULT '{}',
    concurrency  INT         NOT NULL,
    started_at   TIMESTAMPTZ NOT NULL,
    heartbeat_at TIMESTAMPTZ NOT NULL,
    expires_at   TIMESTAMPTZ NOT NULL
);
