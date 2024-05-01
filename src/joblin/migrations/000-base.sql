CREATE TABLE IF NOT EXISTS job (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    data BLOB,
    created_at REAL NOT NULL,
    starts_at REAL NOT NULL,
    expires_at REAL,
    completed_at REAL,
    CONSTRAINT job_must_be_created_before_start CHECK (created_at <= starts_at),
    CONSTRAINT job_must_start_before_expiration CHECK (expires_at IS NULL OR starts_at <= expires_at)
);
CREATE INDEX IF NOT EXISTS ix_job_starts_at_expires_at ON job (starts_at, expires_at);
