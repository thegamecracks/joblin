from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .scheduler import Scheduler


@dataclass
class Job:
    """A job created by the scheduler."""

    scheduler: Scheduler
    """The scheduler associated with this job."""
    id: int
    """The job's ID stored in the scheduler."""
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
    """The time at which this job was locked, or None if not locked."""

    def complete(self, completed_at: float | None = None) -> bool:
        """Mark the job as completed.

        This is a convenience method for calling :meth:`Scheduler.complete_job()`.

        If the job does not exist, this is a no-op.

        Note that calling this method does not change the
        :attr:`completed_at` attribute.

        :param completed_at:
            The time at which the job was completed.
            Defaults to the scheduler's current time.
        :returns: ``True`` if the job was updated, ``False`` otherwise.

        .. versionchanged:: 0.1.1
            ``completed_at`` now defaults to ``None`` as originally implied,
            rather than being a required parameter.

        """
        return self.scheduler.complete_job(self.id, completed_at)

    def delete(self) -> bool:
        """Delete the job from the scheduler.

        This is a convenience method for calling :meth:`Scheduler.delete_job()`.

        :returns: ``True`` if the job existed, ``False`` otherwise.

        """
        return self.scheduler.delete_job(self.id)

    def lock(self, locked_at: float | None = None) -> bool:
        """Attempt to lock this job.

        This is a convenience method for calling :meth:`Scheduler.lock_job()`.

        This prevents the job from showing up in subsequent
        :meth:`Scheduler.get_next_job()` calls.

        If the job is already locked or does not exist, this returns ``False``.

        :param locked_at:
            The time at which the job was locked.
            Defaults to the current time.
        :returns: ``True`` if the job was locked, ``False`` otherwise.

        """
        return self.scheduler.lock_job(self.id, locked_at=locked_at)

    def unlock(self) -> bool:
        """Attempt to unlock this job.

        This is a convenience method for calling :meth:`Scheduler.unlock_job()`.

        Unlike :meth:`lock()`, this method returns ``True``
        if job is already unlocked.

        If the job does not exist, this returns ``False``.

        :param job_id: The ID of the job.
        :returns: ``True`` if the job was unlocked, ``False`` otherwise.

        """
        return self.scheduler.unlock_job(self.id)

    def get_seconds_until_start(self) -> float:
        """Return the amount of time in seconds to wait until the job starts.

        Note that the returned duration may be negative if the job's start
        time is overdue.

        """
        return self.starts_at - self.scheduler.time()
