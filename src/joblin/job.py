from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .scheduler import Scheduler


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
