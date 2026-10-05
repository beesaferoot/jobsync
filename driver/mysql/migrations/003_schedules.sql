CREATE TABLE IF NOT EXISTS jobsync_schedules (
    id       VARCHAR(255) NOT NULL PRIMARY KEY,
    kind     VARCHAR(255) NOT NULL,
    queue    VARCHAR(255) NOT NULL,
    payload  LONGBLOB     NOT NULL,
    cron     VARCHAR(255) NOT NULL,
    timezone VARCHAR(64)  NOT NULL,
    last_run DATETIME(6)  NULL,
    next_run DATETIME(6)  NOT NULL,
    paused   TINYINT(1)   NOT NULL DEFAULT 0,
    INDEX jobsync_schedules_due (paused, next_run)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
