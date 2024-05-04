"""Starts multiple threads to complete jobs concurrently.

Each thread will concurrently read jobs from the queue.

"""

import random
import threading
import time
from contextlib import contextmanager
from typing import Callable, Iterator

from joblin import Job, Scheduler


def main() -> None:
    scheduler_factory = lambda: Scheduler.connect("job.db")

    with scheduler_factory() as scheduler:
        submit_jobs(scheduler)

    threads = [
        threading.Thread(target=run_pending_jobs, args=(scheduler_factory,))
        for _ in range(3)
    ]

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    with scheduler_factory() as scheduler:
        pending = scheduler.count_pending_jobs()
        print(f"Finished with {pending} job(s) pending")


def submit_jobs(scheduler: Scheduler) -> None:
    n_jobs = 15
    data = "Hello world!"

    for n in range(n_jobs):
        scheduler.add_job_from_now(data, starts_after=n / n_jobs * 2)

    pending = scheduler.count_pending_jobs()
    print(f"{n_jobs} jobs submitted, {pending} jobs pending")


def run_pending_jobs(scheduler_factory: Callable[[], Scheduler]) -> None:
    runner = Runner(scheduler_factory())
    runner.run_pending_jobs()


class Runner:
    def __init__(self, scheduler: Scheduler) -> None:
        self.scheduler = scheduler

    def run_pending_jobs(self) -> None:
        # For this example, we'll stop checking for jobs once the scheduler is empty.
        # Feel free to make this run indefinitely with a stop event.
        while True:
            job_delay = self.scheduler.lock_next_job_delay()
            if job_delay is None:
                break

            job_id, delay = job_delay

            with self._failsafe_unlock(job_id):
                self._wait_to_run_job(job_id, delay)

    @contextmanager
    def _failsafe_unlock(self, job_id: int) -> Iterator[None]:
        # In case an error occurs, unlock the job.
        #
        # Yes, this is a bit confusing to think about and is still
        # not entirely fool-proof since it allows for a gap between
        # the lock call and when this context manager is reached.
        # In other words, the API limits us from being fully failsafe.
        try:
            yield
        except BaseException:
            print(f"Failsafe! Unlocking job #{job_id}")
            self.scheduler.unlock_job(job_id)
            raise

    def _wait_to_run_job(self, job_id: int, delay: float) -> None:
        if delay > 0:
            print(f"Waiting {delay:.2f}s for job #{job_id}...")
            time.sleep(delay)

        job = self.scheduler.get_job_by_id(job_id)
        if job is None:
            return print(f"Skipping now-deleted job #{job_id}")

        self._run_job(job)

    def _run_job(self, job: Job) -> None:
        print(f"Running job #{job.id}...")

        time.sleep(random.uniform(1, 4))
        self.scheduler.complete_job(job.id)

        print(f"Completed job #{job.id}")


_print = print


def print(*args, sep: str = " ", end: str = "\n", **kwargs) -> None:
    # thread-safe print
    if threading.current_thread() is threading.main_thread():
        tid = " main"
    else:
        tid = format(threading.get_ident(), "05d")

    message = sep.join(map(str, args)) + end
    message = f"{tid}: {message}"
    _print(message, sep="", end="", **kwargs)


if __name__ == "__main__":
    main()
