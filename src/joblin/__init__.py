from __future__ import annotations

import logging
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Iterator, Self

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

log = logging.getLogger(__name__)


@dataclass
class Job:
    """A job created by the scheduler."""

    scheduler: Scheduler
    id: int
    data: Any
    created_at: float
    starts_at: float
    expires_at: float | None
    completed_at: float | None

    def complete(self, completed_at: float | None) -> bool:
        """Mark the job as completed.

        This is a convenience method for calling :meth:`Scheduler.complete_job()`.

        If the job does not exist, this is a no-op.

        :param completed_at:
            The time at which the job was completed.
            Defaults to the scheduler's current time.
        :returns: True if the job was updated, False otherwise.

        """
        return self.scheduler.complete_job(self.id, completed_at)

    def delete(self) -> bool:
        """Delete the job from the scheduler.

        This is a convenience method for calling :meth:`Scheduler.delete_job()`.

        :returns: True if the job existed, False otherwise.

        """
        return self.scheduler.delete_job(self.id)

    def get_seconds_until_start(self) -> float:
        """Return the amount of time in seconds to wait until the job starts."""
        return self.starts_at - self.scheduler.time()


class Scheduler:
    """A scheduler that persists jobs in an SQLite database. Not thread-safe.

    This class does not directly provide a mechanism for executing jobs,
    but rather expects the caller to retrieve the next job and wait until
    the job can start. As such, it is also the caller's responsibility
    to reschedule whenever a new job is added, potentially from other
    processes.

    The scheduler can be used in a context manager to automatically close
    the database upon exiting.

    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        time_func: Callable[[], float],
    ) -> None:
        self.conn = conn
        self.time_func = time_func

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_val, tb) -> None:
        self.conn.close()

    def add_job(
        self,
        data: Any,
        *,
        created_at: float | None = None,
        starts_at: float | None = None,
        expires_at: float | None = None,
    ) -> Job:
        """Add a job to the scheduler.

        :param data: The payload to be stored with the job.
        :param created_at:
            The time at which the job was created.
            Defaults to the current time.
        :param starts_at:
            The time at which the job should be executed.
            This cannot be lower than the creation time.
            Defaults to the job's creation time.
        :param expires_at:
            The time at which the job will expire.
            This cannot be lower than the start time.
            If None, the job will never expire.
        :returns: The job that was added.
        :raises sqlite3.IntegrityError:
            The start or expiration time was invalid.

        """
        created_at = created_at or self.time()
        starts_at = starts_at or created_at
        job_id: int = self.conn.execute(
            "INSERT INTO job (data, created_at, starts_at, expires_at) "
            "VALUES (?, ?, ?, ?) "
            "RETURNING id",
            (data, created_at, starts_at, expires_at),
        ).fetchone()[0]

        return Job(
            self,
            job_id,
            data,
            created_at,
            starts_at,
            expires_at,
            completed_at=None,
        )

    def add_job_from_now(
        self,
        data: Any,
        *,
        starts_after: float,
        expires_after: float | None = None,
        created_at: float | None = None,
    ) -> Job:
        """A convenience method to add a job relative to the current time.

        :param data: The payload to be stored with the job.
        :param starts_after:
            The amount of time in seconds after the creation time.
            This cannot be a negative value.
        :param expires_at:
            The amount of time in seconds after which the job will expire.
            This cannot be a negative value.
            If None, the job will never expire.
        :param created_at:
            The time at which the job was created.
            Defaults to the current time.
        :returns: The job that was added.
        :raises sqlite3.IntegrityError:
            The start or expiration time was invalid.

        """
        created_at = created_at or self.time()
        starts_at = created_at + starts_after
        expires_at = None if expires_after is None else created_at + expires_after

        return self.add_job(
            data,
            created_at=created_at,
            starts_at=starts_at,
            expires_at=expires_at,
        )

    def get_job_by_id(self, job_id: int) -> Job | None:
        """Get a job from the scheduler by ID."""
        c = self.conn.execute("SELECT * FROM job WHERE id = ?", (job_id,))
        row = c.fetchone()
        if row is not None:
            return Job(self, **row)

    def get_next_job(self, now: float | None = None) -> Job | None:
        """Get the next job in the scheduler.

        :param now:
            The current time.
            Defaults to the current time.
        :returns: The next job to be completed, if any.

        """
        now = now or self.time()
        c = self.conn.execute(
            "SELECT * FROM job WHERE completed_at IS NULL "
            "AND (expires_at IS NULL OR ? < expires_at)"
            "ORDER BY starts_at LIMIT 1",
            (now,),
        )
        row = c.fetchone()
        if row is not None:
            return Job(self, **row)

    def get_seconds_until_next_job(
        self,
        now: float | None = None,
    ) -> tuple[int, float] | None:
        """Get the next job's ID and the amount of time in seconds
        to wait until it starts.

        This reduces unnecessary I/O compared to :meth:`get_next_job()`
        when only the time is needed.

        Note that the returned duration may be negative if the job's start
        time is overdue.

        To avoid race conditions in cases where the job's start and
        expiration time are equal, the job object should be retrieved with
        :meth:`get_job_by_id()` rather than calling :meth:`get_next_job()`.

        :param now:
            The current time.
            Defaults to the current time.
        :returns: The next job's ID and delay, or None if no job is pending.

        """
        now = now or self.time()
        c = self.conn.execute(
            "SELECT id, starts_at FROM job WHERE completed_at IS NULL "
            "AND (expires_at IS NULL OR ? < expires_at)"
            "ORDER BY starts_at LIMIT 1",
            (now,),
        )
        row = c.fetchone()
        if row is not None:
            return row[0], row[1] - now

    def count_pending_jobs(self, now: float | None = None) -> int:
        """Count the number of jobs that need to run.

        :param now:
            The current time.
            Defaults to the current time.
        :returns: The number of pending jobs.

        """
        # Similar query to get_next_job()
        now = now or self.time()
        c = self.conn.execute(
            "SELECT COUNT(*) FROM job WHERE completed_at IS NULL "
            "AND (expires_at IS NULL OR ? < expires_at)",
            (now,),
        )
        return c.fetchone()[0]

    def complete_job(self, job_id: int, completed_at: float | None = None) -> bool:
        """Mark the given job as completed.

        If the job does not exist, this is a no-op.

        :param job_id: The ID of the job.
        :param completed_at:
            The time at which the job was completed.
            Defaults to the current time.
        :returns: True if the job was updated, False otherwise.

        """
        completed_at = completed_at or self.time()
        c = self.conn.execute(
            "UPDATE job SET completed_at = ? WHERE id = ?",
            (completed_at, job_id),
        )
        return c.rowcount > 0

    def delete_job(self, job_id: int) -> bool:
        """Delete a job from the scheduler by ID.

        :param job_id: The ID of the job.
        :returns: True if the job existed, False otherwise.

        """
        c = self.conn.execute("DELETE FROM job WHERE id = ?", (job_id,))
        return c.rowcount > 0

    def delete_completed_jobs(self) -> int:
        """Delete all completed jobs.

        :param now:
            The current time.
            Defaults to the current time.

        """
        c = self.conn.execute("DELETE FROM job WHERE completed_at IS NOT NULL")
        return c.rowcount

    def delete_expired_jobs(self, now: float | None = None) -> int:
        """Delete all expired jobs.

        Jobs marked as completed will not be considered as expired.

        :param now:
            The current time.
            Defaults to the current time.

        """
        now = now or self.time()
        c = self.conn.execute(
            "DELETE FROM job WHERE completed_at IS NULL "
            "AND expires_at IS NOT NULL AND ? >= expires_at",
            (now,),
        )
        return c.rowcount

    @classmethod
    def connect(
        cls,
        path: str,
        time_func: Callable[[], float] = time.time,
        **kwargs,
    ) -> Self:
        """Connect and set up an SQLite database at the given path.

        ``:memory:`` can be used instead of a file path to create an
        in-memory scheduler. In this case, it may be desirable to provide
        a monotonic time function to avoid abnormal behaviour from
        the system time changing.

        Extra arguments will be passed to :func:`sqlite3.connect()`.

        """
        conn = sqlite3.connect(path, isolation_level=None, **kwargs)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL")
        _SchedulerMigrator(conn).run_migrations()
        return cls(conn, time_func=time_func)

    # NOTE: defined here to avoid shadowing the time module
    def time(self) -> float:
        """Get the current time as returned by :attr:`time_func`."""
        return self.time_func()


class _SchedulerMigrator:
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
