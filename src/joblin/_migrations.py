import logging
import sqlite3
from contextlib import contextmanager
from typing import Iterator


log = logging.getLogger(__name__)

SCHEMA_VERSION_0 = """
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
"""
SCHEMA_VERSION_1 = """
DROP INDEX IF EXISTS ix_job_starts_at_expires_at;
CREATE INDEX IF NOT EXISTS ix_job_completed_at_expires_at ON job (completed_at, expires_at);

CREATE TABLE IF NOT EXISTS job_schema (key TEXT PRIMARY KEY, value) WITHOUT ROWID;
INSERT OR IGNORE INTO job_schema (key, value) VALUES ('version', 0);
"""


class Migrator:
    MIGRATIONS = (
        (-1, ""),
        (0, SCHEMA_VERSION_0),
        (1, SCHEMA_VERSION_1),
    )

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def run_migrations(self) -> None:
        version = self.get_version()
        if version >= 0 and not any(version == v for v, _ in self.MIGRATIONS):
            log.warning(
                "Unrecognized database version %s, scheduler may not work as intended"
            )

        with self.begin() as conn:
            for version, script in self.get_migrations(version):
                log.debug("Migrating database to v%s", version)
                conn.executescript(script)

            conn.execute(
                "UPDATE job_schema SET value = ? WHERE key = 'version'",
                (version,),
            )

    def get_version(self) -> int:
        try:
            c = self.conn.execute("SELECT value FROM job_schema WHERE key = 'version'")
        except sqlite3.OperationalError:
            log.debug("job_schema not present, checking for job table")
            c = self.conn.execute(
                "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'job'"
            )

            if c.fetchone() is None:
                log.debug("job table not present, assuming version -1")
                return -1

            log.debug("job table exists, assuming version 0")
            return 0

        row = c.fetchone()
        if row is None:
            raise RuntimeError("missing 'version' key in job_schema table")

        version = int(row[0])
        log.debug("job_schema version returned %s", version)
        return version

    @contextmanager
    def begin(self) -> Iterator[sqlite3.Connection]:
        self.conn.execute("BEGIN")
        try:
            yield self.conn
        except BaseException:
            self.conn.rollback()
            raise
        else:
            self.conn.commit()

    @classmethod
    def get_migrations(cls, version: int) -> tuple[tuple[int, str], ...]:
        i = 0
        for i, (v, _) in enumerate(cls.MIGRATIONS, start=1):
            if version == v:
                break
        return cls.MIGRATIONS[i:]
