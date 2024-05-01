DROP INDEX IF EXISTS ix_job_starts_at_expires_at;
CREATE INDEX IF NOT EXISTS ix_job_completed_at_expires_at ON job (completed_at, expires_at);

CREATE TABLE IF NOT EXISTS job_schema (key TEXT PRIMARY KEY, value) WITHOUT ROWID;
INSERT OR IGNORE INTO job_schema (key, value) VALUES ('version', 0);
