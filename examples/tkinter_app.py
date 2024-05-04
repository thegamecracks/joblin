"""
Start a tkinter GUI and event loop to submit and complete jobs found in job.db.
"""

import logging
from tkinter import Text, Tk
from tkinter.ttk import Button, Frame
from typing import Any, Callable

from joblin import Job, Scheduler


def main() -> None:
    fmt = "%(levelname)s: %(message)-50s (%(name)s#L%(lineno)d)"
    logging.basicConfig(format=fmt, level=logging.DEBUG)

    enable_windows_dpi_awareness()

    def job_callback(job: Job) -> None:
        log_callback(f"Completed job #{job.id}")

    def log_callback(message: object) -> None:
        text_log.configure(state="normal")
        text_log.insert("end", f"{message}\n")
        text_log.configure(state="disabled")
        text_log.see("end")

    with Scheduler.connect("job.db") as scheduler:
        app = Tk()
        app.geometry("600x300")

        runner = Runner(app, scheduler, job_callback)
        # Have our runner check if there's a job to run
        runner.reschedule()

        controls = SchedulerControls(app, scheduler, runner, log_callback)
        controls.pack()
        text_log = Text(app, font="TkDefaultFont", state="disabled")
        text_log.pack(fill="both", expand=True, padx=10, pady=10)

        app.mainloop()


def enable_windows_dpi_awareness() -> None:
    import sys

    if sys.platform == "win32":
        from ctypes import windll

        windll.shcore.SetProcessDpiAwareness(2)


class Runner:
    """Run jobs from a scheduler using a tkinter event loop."""

    _job_id: int | None
    """The current job ID scheduled to be run."""
    _callback_id: str | None
    """The ID of the command scheduled to be called by tkinter.

    This is used for cancellation if the runner needs to be rescheduled.

    """

    def __init__(
        self,
        app: Tk,
        scheduler: Scheduler,
        job_callback: Callable[[Job], Any],
    ) -> None:
        self.app = app
        self.scheduler = scheduler
        self.job_callback = job_callback

        self._job_id = None
        self._callback_id = None

    def reschedule(self) -> None:
        """(Re)schedule the runner.

        This should be called once when the runner is first created
        to check for any existing jobs to run.

        When a new job gets queued in the scheduler, this method should
        be called to reschedule the runner in case the new job has a
        higher priority over the previous job.

        """
        if self._callback_id is not None:
            self.app.after_cancel(self._callback_id)
            self._callback_id = None

        job_delay = self.scheduler.get_next_job_delay()
        if job_delay is None:
            return

        job_id, delay = job_delay
        delay_ms = int(delay * 1000)

        self._job_id = job_id
        self._callback_id = self.app.after(delay_ms, self._run_job)

    def _run_job(self) -> None:
        """Call :attr:`job_callback` with the last scheduled job,
        mark the job as completed, and reschedule.

        If the job no longer exists, the runner will be rescheduled
        for the next job.

        """
        self._callback_id = None
        if self._job_id is None:
            return

        job = self.scheduler.get_job_by_id(self._job_id)
        if job is not None and job.completed_at is None:
            self.job_callback(job)
            self.scheduler.complete_job(job.id)

        self.reschedule()


class SchedulerControls(Frame):
    """Various controls for the user to interact with the scheduler."""

    def __init__(
        self,
        parent: Tk,
        scheduler: Scheduler,
        runner: Runner,
        log_callback: Callable[[object], Any],
    ) -> None:
        super().__init__(parent)

        self.scheduler = scheduler
        self.runner = runner
        self.log_callback = log_callback

        self.next = Button(self, text="Check Next Job", command=self.check_next_job)
        self.next.pack(side="left")
        self.count = Button(
            self, text="Count Pending Jobs", command=self.count_pending_jobs
        )
        self.count.pack(side="left")
        self.clear = Button(self, text="Cleanup Jobs", command=self.cleanup_jobs)
        self.clear.pack(side="left")
        self.submit = Button(self, text="Submit Job", command=self.submit_job)
        self.submit.pack(side="left")

    def check_next_job(self) -> None:
        """Log the time remaining until the next job runs."""
        job_delay = self.scheduler.get_next_job_delay()
        if job_delay is None:
            return self.log("No job is pending completion.")

        job_id, delay = job_delay
        self.log(f"The next job is #{job_id}, due in {delay:.2f} seconds.")

    def count_pending_jobs(self) -> None:
        """Log the number of jobs remaining in the scheduler."""
        n = self.scheduler.count_pending_jobs()
        self.log(f"{n} job(s) need to be completed.")

    def cleanup_jobs(self) -> None:
        """Clean up any completed and expired jobs from the scheduler."""
        n_completed = self.scheduler.delete_completed_jobs()
        n_expired = self.scheduler.delete_expired_jobs()
        self.log(
            f"{n_completed} completed job(s) and "
            f"{n_expired} expired job(s) were cleaned up."
        )

    def submit_job(self) -> None:
        """Submit a new job to the scheduler."""
        job = self.scheduler.add_job_from_now(None, starts_after=3, expires_after=10)
        self.runner.reschedule()
        self.log(f"Submitted job #{job.id}, expected completion in 3 seconds.")

    def log(self, message: object) -> None:
        """Call :attr:`log_callback` with the given message and end suffix."""
        self.log_callback(message)


if __name__ == "__main__":
    main()
