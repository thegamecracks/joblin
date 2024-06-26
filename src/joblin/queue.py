import logging
import sqlite3
import time
from contextlib import contextmanager
from typing import Any, Callable, Iterator, Literal, Self

from .job import Job
from ._migrations import run_default_migrations

log = logging.getLogger(__name__)


class Queue:
    """A queue that persists jobs in an SQLite database. Not thread-safe.

    ::

        queue = Queue.connect("job.db")
        job = queue.add_job("Some payload")
        job = queue.get_next_job()
        if job is not None:
            job.complete()
        queue.close()

    This class does not directly provide a mechanism for executing jobs,
    but rather expects the caller to retrieve the next job and wait until
    the job can start. As such, it is also the caller's responsibility
    to reschedule whenever a new job is added, potentially from other
    processes.

    The queue can be used in a context manager to automatically close
    the database upon exiting. For example::

        with Queue.connect("job.db") as queue:
            ...

    """

    conn: sqlite3.Connection
    """The SQLite conection used to query for jobs."""
    time_func: Callable[[], float]
    """The function used to get the current time."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        time_func: Callable[[], float],
    ) -> None:
        """
        :param conn: The SQLite conection used to query for jobs.
        :param time_func: The function used to get the current time.
        """
        self.conn = conn
        self.time_func = time_func

    @classmethod
    def connect(
        cls,
        path: str,
        time_func: Callable[[], float] = time.time,
        **kwargs,
    ) -> Self:
        """Connect and set up an SQLite database at the given path.

        ::

            with Queue.connect("job.db") as queue:
                ...

        ``:memory:`` can be used instead of a file path to create an
        in-memory queue. In this case, it may be desirable to provide
        a monotonic time function to avoid abnormal behaviour from
        the system time changing.

        Extra arguments will be passed to :func:`sqlite3.connect()`.

        :param path: The database path to open.
        :param time_func: The function used to get the current time.
        :returns: A new queue instance.

        """
        conn = sqlite3.connect(path, isolation_level=None, **kwargs)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL")
        run_default_migrations(conn)
        return cls(conn, time_func=time_func)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_val, tb) -> None:
        self.close()

    def add_job(
        self,
        data: Any,
        *,
        created_at: float | None = None,
        starts_at: float | None = None,
        expires_at: float | None = None,
    ) -> Job:
        """Add a job to the queue.

        ::

            job = queue.add_job("Hello world!", created_at=123.45, starts_at=130, expires_at=140)

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
        if created_at is None:
            created_at = self.time()
        if starts_at is None:
            starts_at = created_at

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
            locked_at=None,
        )

    def add_job_from_now(
        self,
        data: Any,
        *,
        starts_after: float = 0.0,
        expires_after: float | None = None,
        created_at: float | None = None,
    ) -> Job:
        """A convenience method to add a job relative to the current time.

        ::

            job = queue.add_job_from_now("Hello world!", starts_after=10, expires_after=20)

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
        if created_at is None:
            created_at = self.time()

        starts_at = created_at + starts_after
        expires_at = None if expires_after is None else created_at + expires_after

        return self.add_job(
            data,
            created_at=created_at,
            starts_at=starts_at,
            expires_at=expires_at,
        )

    def get_job_by_id(self, job_id: int) -> Job | None:
        """Get a job from the queue by ID.

        ::

            job = queue.get_job_by_id(1234)

        :param job_id: The ID of the job.
        :returns: A job object, or None if not found.

        """
        c = self.conn.execute("SELECT * FROM job WHERE id = ?", (job_id,))
        row = c.fetchone()
        if row is not None:
            return Job(self, **row)

    def get_next_job(self, now: float | None = None) -> Job | None:
        """Get the next job in the queue.

        ::

            job = queue.get_next_job()

        If two jobs start at the same time, the job with the
        lower ID gets priority.

        :param now:
            The current time.
            Defaults to the current time.
        :returns: The next job to be completed, if any.

        """
        if now is None:
            now = self.time()

        c = self.conn.execute(
            "SELECT * FROM job WHERE completed_at IS NULL "
            "AND (expires_at IS NULL OR ? < expires_at) "
            "AND locked_at IS NULL "
            "ORDER BY starts_at, id LIMIT 1",
            (now,),
        )
        row = c.fetchone()
        if row is not None:
            return Job(self, **row)

    def get_next_job_delay(
        self,
        now: float | None = None,
    ) -> tuple[int, float] | None:
        """Get the next job's ID and the amount of time in seconds
        to wait until it starts.

        ::

            job_delay = queue.get_next_job_delay()
            if job_delay is not None:
                job_id, delay = job_delay

                time.sleep(delay)

                job = queue.get_job_by_id(job_id)
                if job is not None:
                    ...

        This reduces unnecessary I/O compared to :meth:`get_next_job()`
        when only the time is needed.

        If the job's start time is overdue, the delay will be 0.

        To avoid race conditions in cases where the job's start and
        expiration time are equal, the job object should be retrieved with
        :meth:`get_job_by_id()` rather than calling :meth:`get_next_job()`.

        :param now:
            The current time.
            Defaults to the current time.
        :returns: The next job's ID and delay, or None if no job is pending.

        .. versionadded:: 0.3.0

        """
        if now is None:
            now = self.time()

        c = self.conn.execute(
            "SELECT id, starts_at FROM job WHERE completed_at IS NULL "
            "AND (expires_at IS NULL OR ? < expires_at) "
            "AND locked_at IS NULL "
            "ORDER BY starts_at, id LIMIT 1",
            (now,),
        )
        row = c.fetchone()
        if row is not None:
            return row[0], max(0.0, row[1] - now)

    def count_pending_jobs(self, now: float | None = None) -> int:
        """Count the number of jobs that need to run.

        ::

            pending = queue.count_pending_jobs()

        :param now:
            The current time.
            Defaults to the current time.
        :returns: The number of pending jobs.

        """
        # Similar query to get_next_job()
        if now is None:
            now = self.time()

        c = self.conn.execute(
            "SELECT COUNT(*) FROM job WHERE completed_at IS NULL "
            "AND (expires_at IS NULL OR ? < expires_at)",
            (now,),
        )
        return c.fetchone()[0]

    def lock_job(self, job_id: int, *, locked_at: float | None = None) -> bool:
        """Attempt to lock the given job.

        ::

            success = queue.lock_job(1234)

        This prevents the job from showing up in subsequent
        :meth:`get_next_job()` calls.

        If the job is already locked or does not exist, this returns ``False``.

        :param job_id: The ID of the job.
        :param locked_at:
            The time at which the job was locked.
            Defaults to the current time.
        :returns: ``True`` if the job was locked, ``False`` otherwise.

        .. versionadded:: 0.3.0

        """
        if locked_at is None:
            locked_at = self.time()

        with self._begin("IMMEDIATE"):
            c = self.conn.execute("SELECT locked_at FROM job WHERE id = ?", (job_id,))
            row = c.fetchone()

            if row is None:  # Job was deleted
                return False

            if row[0] is not None:  # Job is already locked
                return False

            c.execute("UPDATE job SET locked_at = ? WHERE id = ?", (locked_at, job_id))
            return True

    def lock_next_job(self, now: float | None = None) -> Job | None:
        """Get and lock the next job in the queue.

        ::

            job = queue.lock_next_job()
            if job is not None:
                time.sleep(job.delay)

                # In case job got updated or deleted, check again
                job = queue.get_job_by_id(job.id)
                if job is not None:
                    job.complete()
                    job.unlock()

        This should be preferred over manually calling :meth:`get_next_job()`
        and :meth:`lock_job()` as this method will do both in a single
        transaction, reducing the chance of other connections trying
        to lock the same job.

        If two jobs start at the same time, the job with the
        lower ID gets priority.

        :param now:
            The current time.
            Defaults to the current time.
        :returns: The next job to be completed, if any.

        .. versionadded:: 0.3.0

        """
        if now is None:
            now = self.time()

        with self._begin("IMMEDIATE"):
            c = self.conn.execute(
                "SELECT * FROM job WHERE completed_at IS NULL "
                "AND (expires_at IS NULL OR ? < expires_at) "
                "AND locked_at IS NULL "
                "ORDER BY starts_at, id LIMIT 1",
                (now,),
            )
            row = c.fetchone()
            if row is None:
                return None

            job = Job(self, **row)
            job.locked_at = now

            c.execute("UPDATE job SET locked_at = ? WHERE id = ?", (now, job.id))
            return job

    def lock_next_job_delay(
        self,
        now: float | None = None,
    ) -> tuple[int, float] | None:
        """Lock the next job and return its ID and the amount of time
        in seconds to wait until it starts.

        ::

            job_delay = queue.lock_next_job_delay()
            if job_delay is not None:
                job_id, delay = job_delay

                time.sleep(delay)

                job = queue.get_job_by_id(job_id)
                if job is not None:
                    job.complete()
                    job.unlock()

        This should be preferred over manually calling :meth:`get_next_job()`
        and :meth:`lock_job()` as this method will do both in a single
        transaction, reducing the chance of other connections trying
        to lock the same job.

        This reduces unnecessary I/O compared to :meth:`lock_next_job()`
        when only the time is needed.

        If the job's start time is overdue, the delay will be 0.

        To avoid race conditions in cases where the job's start and
        expiration time are equal, the job object should be retrieved with
        :meth:`get_job_by_id()` rather than calling :meth:`get_next_job()`.

        :param now:
            The current time.
            Defaults to the current time.
        :returns: The next job's ID and delay, or None if no job is pending.

        .. versionadded:: 0.3.0

        """
        if now is None:
            now = self.time()

        with self._begin("IMMEDIATE"):
            c = self.conn.execute(
                "SELECT id, starts_at FROM job WHERE completed_at IS NULL "
                "AND (expires_at IS NULL OR ? < expires_at) "
                "AND locked_at IS NULL "
                "ORDER BY starts_at, id LIMIT 1",
                (now,),
            )
            row = c.fetchone()
            if row is None:
                return None

            c.execute("UPDATE job SET locked_at = ? WHERE id = ?", (now, row[0]))
            return row[0], max(0.0, row[1] - now)

    def unlock_job(self, job_id: int) -> bool:
        """Attempt to unlock the given job.

        ::

            success = queue.unlock_job(1234)

        Unlike :meth:`lock_job()`, this method returns ``True``
        if job is already unlocked.

        If the job does not exist, this returns ``False``.

        :param job_id: The ID of the job.
        :returns: ``True`` if the job was unlocked, ``False`` otherwise.

        .. versionadded:: 0.3.0

        """
        c = self.conn.execute(
            "UPDATE job SET locked_at = ? WHERE id = ? RETURNING 1",
            (None, job_id),
        )
        return c.fetchone() is not None

    def complete_job(self, job_id: int, completed_at: float | None = None) -> bool:
        """Mark the given job as completed.

        ::

            success = queue.complete_job(1234)

        If the job does not exist, this is a no-op.

        :param job_id: The ID of the job.
        :param completed_at:
            The time at which the job was completed.
            Defaults to the current time.
        :returns: ``True`` if the job was updated, ``False`` otherwise.

        """
        if completed_at is None:
            completed_at = self.time()

        c = self.conn.execute(
            "UPDATE job SET completed_at = ? WHERE id = ?",
            (completed_at, job_id),
        )
        return c.rowcount > 0

    def delete_job(self, job_id: int) -> bool:
        """Delete a job from the queue by ID.

        ::

            success = queue.delete_job(1234)

        :param job_id: The ID of the job.
        :returns: ``True`` if the job existed, ``False`` otherwise.

        """
        c = self.conn.execute("DELETE FROM job WHERE id = ?", (job_id,))
        return c.rowcount > 0

    def delete_completed_jobs(self) -> int:
        """Delete all completed jobs.

        ::

            deleted = queue.delete_completed_jobs()

        :param now:
            The current time.
            Defaults to the current time.
        :returns: The number of jobs that were deleted.

        """
        c = self.conn.execute("DELETE FROM job WHERE completed_at IS NOT NULL")
        return c.rowcount

    def delete_expired_jobs(self, now: float | None = None) -> int:
        """Delete all expired jobs.

        ::

            deleted = queue.delete_expired_jobs()

        Jobs marked as completed will not be considered as expired.

        :param now:
            The current time.
            Defaults to the current time.
        :returns: The number of jobs that were deleted.

        """
        if now is None:
            now = self.time()

        c = self.conn.execute(
            "DELETE FROM job WHERE completed_at IS NULL "
            "AND expires_at IS NOT NULL AND ? >= expires_at",
            (now,),
        )
        return c.rowcount

    def close(self) -> None:
        """Close the queue.

        .. versionadded:: 0.2.0

        """
        self.conn.close()

    # NOTE: defined here to avoid shadowing the time module
    def time(self) -> float:
        """Get the current time as returned by :attr:`time_func`."""
        return self.time_func()

    @contextmanager
    def _begin(
        self,
        mode: Literal["DEFERRED", "IMMEDIATE", "EXCLUSIVE"] = "DEFERRED",
    ) -> Iterator[sqlite3.Connection]:
        self.conn.execute(f"BEGIN {mode}")
        try:
            yield self.conn
        except BaseException:
            self.conn.rollback()
            raise
        else:
            self.conn.commit()
