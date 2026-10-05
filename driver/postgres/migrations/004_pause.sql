CREATE TABLE IF NOT EXISTS jobsync_paused_queues (
    name      TEXT        PRIMARY KEY,
    paused_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
