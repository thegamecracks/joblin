from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .queue import Queue


@dataclass
class Job:
    """A job created by the queue.

    Any methods here that call the queue are not thread-safe.
    If querying from another thread is desired, you must create a
    new connection and queue.

    """

    queue: Queue
    """The queue associated with this job."""
    id: int
    """The job's ID stored in the queue."""
    data: Any
    """The payload stored with this job."""
    created_at: float
    """The time at which this job was created."""
    starts_at: float
    """The time at which this job should be executed."""
    expires_at: float | None
    """The time at which this job will expire.

    If None, the job will never expire.

    """
    completed_at: float | None
    """The time at which this job was completed, or None if not completed."""
    locked_at: float | None
    """The time at which this job was locked, or None if not locked.

    .. versionadded:: 0.3.0

    """

    def complete(self, completed_at: float | None = None) -> bool:
        """Mark the job as completed.

        This is a convenience method for calling :meth:`Queue.complete_job()`.

        If the job does not exist, this is a no-op.

        Note that calling this method does not change the
        :attr:`completed_at` attribute.

        :param completed_at:
            The time at which the job was completed.
            Defaults to the queue's current time.
        :returns: ``True`` if the job was updated, ``False`` otherwise.

        .. versionchanged:: 0.1.1
            ``completed_at`` now defaults to ``None`` as originally implied,
            rather than being a required parameter.

        """
        return self.queue.complete_job(self.id, completed_at)

    def delete(self) -> bool:
        """Delete the job from the queue.

        This is a convenience method for calling :meth:`Queue.delete_job()`.

        :returns: ``True`` if the job existed, ``False`` otherwise.

        """
        return self.queue.delete_job(self.id)

    def lock(self, locked_at: float | None = None) -> bool:
        """Attempt to lock this job.

        This is a convenience method for calling :meth:`Queue.lock_job()`.

        This prevents the job from showing up in subsequent
        :meth:`Queue.get_next_job()` calls.

        If the job is already locked or does not exist, this returns ``False``.

        :param locked_at:
            The time at which the job was locked.
            Defaults to the current time.
        :returns: ``True`` if the job was locked, ``False`` otherwise.

        .. versionadded:: 0.3.0

        """
        return self.queue.lock_job(self.id, locked_at=locked_at)

    def unlock(self) -> bool:
        """Attempt to unlock this job.

        This is a convenience method for calling :meth:`Queue.unlock_job()`.

        Unlike :meth:`lock()`, this method returns ``True``
        if job is already unlocked.

        If the job does not exist, this returns ``False``.

        :param job_id: The ID of the job.
        :returns: ``True`` if the job was unlocked, ``False`` otherwise.

        .. versionadded:: 0.3.0

        """
        return self.queue.unlock_job(self.id)

    @property
    def delay(self) -> float:
        """The amount of time in seconds until the job starts.

        If the job's start time is overdue, this returns 0.

        .. versionadded:: 0.3.0

        """
        return max(0.0, self.starts_at - self.queue.time())
