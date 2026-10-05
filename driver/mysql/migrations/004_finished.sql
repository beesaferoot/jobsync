ALTER TABLE jobsync_jobs
    ADD COLUMN finished_at DATETIME(6) NULL,
    ADD INDEX jobsync_jobs_finished (finished_at)
