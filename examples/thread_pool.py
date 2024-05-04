"""Starts a thread pool executor to complete jobs concurrently."""
import concurrent.futures
import random
import threading
import time
from concurrent.futures import Executor, Future, ThreadPoolExecutor
from typing import Callable

from joblin import Job, Scheduler


def main() -> None:
    scheduler_factory = lambda: Scheduler.connect("job.db")
    executor = ThreadPoolExecutor(max_workers=3)
    with scheduler_factory() as main_scheduler, executor:
        submit_jobs(main_scheduler)
        run_pending_jobs(main_scheduler, executor, scheduler_factory)


def submit_jobs(scheduler: Scheduler) -> None:
    n_jobs = 15
    data = "Hello world!"

    for n in range(n_jobs):
        scheduler.add_job_from_now(data, starts_after=n / n_jobs * 2)

    pending = scheduler.count_pending_jobs()
    print(f"{n_jobs} jobs submitted, {pending} jobs pending")


def run_pending_jobs(
    scheduler: Scheduler,
    executor: Executor,
    scheduler_factory: Callable[[], Scheduler],
) -> None:
    futures: list[Future] = []

    # For this example, we'll stop checking for jobs once the scheduler is empty.
    # Feel free to make this run indefinitely.
    while (job_delay := scheduler.get_seconds_until_next_job()) is not None:
        job_id, job_delay = job_delay
        job_delay = max(0, job_delay)

        if job_delay > 0:
            print(f"Waiting {job_delay:.2f}s for job #{job_id}...")
            time.sleep(job_delay)

        # FIXME: Below checks are NOT atomic. This requires a scheduler
        #        method to get and lock a job in the same transaction.
        #        Until this is implemented, it may be possible for
        #        another process to complete/delete the job before
        #        the job is locked by this process.
        job = scheduler.get_job_by_id(job_id)
        if job is None:
            print(f"Skipping now-deleted job #{job_id}")
            continue
        if job.completed_at is not None:
            print(f"Skipping now-completed job #{job_id}")
            continue
        if not job.lock():
            print(f"Unable to lock job #{job_id}, skipping")
            continue

        print(f"Submitting job #{job_id} to executor")
        fut = executor.submit(run_job, scheduler_factory, job)
        fut.add_done_callback(make_job_unlocker(scheduler_factory, job.id))
        futures.append(fut)

    # Wait for any remaining futures to complete. This is to avoid
    # shutting down the executor early and unnecessarily cancelling jobs.
    concurrent.futures.wait(futures)


def run_job(scheduler_factory: Callable[[], Scheduler], job: Job) -> None:
    tid = threading.get_ident()
    print(f"  Thread {tid:05d} running job #{job.id}...")
    time.sleep(random.uniform(1, 4))

    # The scheduler associated with the job isn't thread-safe,
    # so we need to create a new scheduler to complete the job.
    with scheduler_factory() as scheduler:
        scheduler.complete_job(job.id)

    print(f"  Thread {tid:05d} completed job #{job.id}")


def make_job_unlocker(
    scheduler_factory: Callable[[], Scheduler],
    job_id: int,
) -> Callable[[Future], None]:
    # Instead of unlocking the job directly in run_job(), we'll use
    # a future callback.
    #
    # This simplifies run_job() by not needing a try/finally clause,
    # and also guarantees jobs will be unlocked if the executor shuts
    # down with jobs that haven't been given to a worker thread yet.

    def callback(fut: Future) -> None:
        with scheduler_factory() as scheduler:
            scheduler.unlock_job(job_id)

    return callback


_print = print


def print(*args, sep: str = " ", end: str = "\n", **kwargs) -> None:
    # thread-safe print
    message = sep.join(map(str, args)) + end
    _print(message, sep="", end="", **kwargs)


if __name__ == "__main__":
    main()
