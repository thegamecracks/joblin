"""Start multiple threads to complete jobs concurrently.

Here, each thread concurrently reads jobs from the queue
For reading jobs only from the main thread, see thread_pool.py.

"""

from __future__ import annotations

import random
import threading
import time
from contextlib import contextmanager
from typing import Callable, Iterator

from joblin import Job, Queue


def main() -> None:
    queue_factory = lambda: Queue.connect("job.db")

    with queue_factory() as queue:
        submit_jobs(queue)

    runners = [Runner(queue_factory) for _ in range(3)]
    threads = [threading.Thread(target=runner.run_pending_jobs) for runner in runners]

    start_and_join(runners, threads)

    with queue_factory() as queue:
        pending = queue.count_pending_jobs()
        print(f"Finished with {pending} job(s) pending")


def submit_jobs(queue: Queue) -> None:
    n_jobs = 15
    data = "Hello world!"

    for n in range(n_jobs):
        queue.add_job_from_now(data, starts_after=n / n_jobs * 2)

    pending = queue.count_pending_jobs()
    print(f"{n_jobs} jobs submitted, {pending} jobs pending")


def start_and_join(runners: list[Runner], threads: list[threading.Thread]) -> None:
    try:
        for t in threads:
            t.start()

        join(threads)
    except BaseException as e:
        # This is how we propagate signals like KeyboardInterrupt
        # from the main thread to the runners.
        print(f"{type(e).__name__} received, stopping runners")
        for runner in runners:
            runner.stop()
        raise
    finally:
        join(threads)


def join(threads: list[threading.Thread]) -> None:
    # For whatever reason, a bare .join() call seems to prevent
    # signals from coming through.
    while any(t.is_alive() for t in threads):
        time.sleep(0.25)


class Runner:
    def __init__(self, queue_factory: Callable[[], Queue]) -> None:
        self.queue_factory = queue_factory
        self._stop_ev = threading.Event()

    def run_pending_jobs(self) -> None:
        queue = self.queue_factory()

        # For this example, we'll stop checking for jobs once
        # the stop event is set or the queue is empty.
        # Feel free to make this run indefinitely.
        while not self._stop_ev.is_set():
            job_delay = queue.lock_next_job_delay()
            if job_delay is None:
                break

            job_id, delay = job_delay
            with self._unlock_on_exit(queue, job_id):
                self._wait_to_run_job(queue, job_id, delay)

        print("Done")

    def stop(self) -> None:
        self._stop_ev.set()

    @contextmanager
    def _unlock_on_exit(self, queue: Queue, job_id: int) -> Iterator[None]:
        # Once the job is done or an error occurs, unlock the job.
        #
        # This is not entirely fool-proof since it allows for a gap between
        # the lock call and when this context manager is reached.
        # In other words, the API limits us from being fully failsafe.
        try:
            yield
        finally:
            queue.unlock_job(job_id)

    def _wait_to_run_job(self, queue: Queue, job_id: int, delay: float) -> None:
        if delay > 0:
            print(f"Waiting {delay:.2f}s for job #{job_id}...")

            if self._stop_ev.wait(delay):
                return print("Stop requested while waiting on job")

        job = queue.get_job_by_id(job_id)
        if job is None:
            return print(f"Skipping now-deleted job #{job_id}")

        # At this point, the runner can't check the stop signal
        # and the job must be run to completion.
        self._run_job(queue, job)

    def _run_job(self, queue: Queue, job: Job) -> None:
        print(f"Running job #{job.id}...")

        time.sleep(random.uniform(1, 4))
        queue.complete_job(job.id)

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
