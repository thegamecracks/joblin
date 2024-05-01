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

    def complete(self, completed_at: float | None = None) -> bool:
        """Mark the job as completed.

        This is a convenience method for calling :meth:`Scheduler.complete_job()`.

        If the job does not exist, this is a no-op.

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

    def get_seconds_until_start(self) -> float:
        """Return the amount of time in seconds to wait until the job starts.

        Note that the returned duration may be negative if the job's start
        time is overdue.

        """
        return self.starts_at - self.scheduler.time()
