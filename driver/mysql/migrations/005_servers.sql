CREATE TABLE IF NOT EXISTS jobsync_servers (
    id           VARCHAR(255) NOT NULL PRIMARY KEY,
    hostname     VARCHAR(255) NOT NULL,
    queues       JSON         NOT NULL,
    concurrency  INT          NOT NULL,
    started_at   DATETIME(6)  NOT NULL,
    heartbeat_at DATETIME(6)  NOT NULL,
    expires_at   DATETIME(6)  NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
