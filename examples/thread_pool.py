"""Start a thread pool executor to complete jobs concurrently.

Here, the main thread reads from the queue and submits jobs to worker
threads. For reading from multiple threads, see threads.py.

"""

import concurrent.futures
import random
import threading
import time
from concurrent.futures import Executor, Future, ThreadPoolExecutor
from contextlib import contextmanager
from typing import Callable, Iterator

from joblin import Job, Queue


def main() -> None:
    queue_factory = lambda: Queue.connect("job.db")
    executor = ThreadPoolExecutor(max_workers=3)

    with queue_factory() as main_queue, fail_fast_shutdown(executor):
        submit_jobs(main_queue)

        runner = Runner(main_queue, queue_factory, executor)
        runner.run_pending_jobs()

        pending = main_queue.count_pending_jobs()
        print(f"Finished with {pending} job(s) pending")


@contextmanager
def fail_fast_shutdown(executor: Executor) -> Iterator[Executor]:
    # The default exception behaviour for executors is to shut down
    # without cancelling futures. Since our jobs are persistent,
    # it's fine to cancel pending futures and shut down more quickly.
    with executor:
        try:
            yield executor
        except BaseException as e:
            print(f"{type(e).__name__} raised, shutting down executor")
            executor.shutdown(wait=True, cancel_futures=True)
            raise


def submit_jobs(queue: Queue) -> None:
    n_jobs = 15
    data = "Hello world!"

    for n in range(n_jobs):
        queue.add_job_from_now(data, starts_after=n / n_jobs * 2)

    pending = queue.count_pending_jobs()
    print(f"{n_jobs} jobs submitted, {pending} jobs pending")


class Runner:
    def __init__(
        self,
        queue: Queue,
        queue_factory: Callable[[], Queue],
        executor: Executor,
    ) -> None:
        self.queue = queue
        self.queue_factory = queue_factory
        self.executor = executor

    def run_pending_jobs(self) -> None:
        futures: list[Future] = []

        # For this example, we'll stop checking for jobs once the queue is empty.
        # Feel free to make this run indefinitely.
        while True:
            job_delay = self.queue.lock_next_job_delay()
            if job_delay is None:
                break

            job_id, delay = job_delay
            with self._failsafe_unlock(job_id):
                fut = self._wait_to_run_job(job_id, delay)

            if fut is not None:
                futures.append(fut)

        # Don't return until all jobs are completed.
        # This is what the caller would expect, rather than
        # returning while workers are still running.
        print("No more pending jobs, waiting on executor")
        print("(KeyboardInterrupt signal may be blocked)")
        concurrent.futures.wait(futures)

    @contextmanager
    def _failsafe_unlock(self, job_id: int) -> Iterator[None]:
        # In case an error occurs before the callback gets registered,
        # we have this context manager to unlock the job.
        #
        # Yes, this is a bit confusing to think about and is still
        # not entirely fool-proof since it allows for a gap between
        # the lock call and when this context manager is reached.
        # In other words, the API limits us from being fully failsafe.
        #
        # Try to Ctrl+C while the runner is waiting for jobs!
        try:
            yield
        except BaseException:
            print(f"Failsafe! Unlocking job #{job_id}")
            self.queue.unlock_job(job_id)
            raise

    def _wait_to_run_job(self, job_id: int, delay: float) -> Future | None:
        if delay > 0:
            print(f"Waiting {delay:.2f}s for job #{job_id}...")
            time.sleep(delay)

        job = self.queue.get_job_by_id(job_id)
        if job is None:
            return print(f"Skipping now-deleted job #{job_id}")

        print(f"Submitting job #{job_id} to executor")
        fut = self.executor.submit(self._run_job, job)
        fut.add_done_callback(self._make_job_unlocker(job.id))
        return fut

    def _run_job(self, job: Job) -> None:
        tid = threading.get_ident()
        print(f"  Thread {tid:05d} running job #{job.id}...")
        time.sleep(random.uniform(1, 4))

        # The queue associated with the job isn't thread-safe,
        # so we need to create a new queue to complete the job.
        with self.queue_factory() as queue:
            queue.complete_job(job.id)

        print(f"  Thread {tid:05d} completed job #{job.id}")

    def _make_job_unlocker(self, job_id: int) -> Callable[[Future], None]:
        # Instead of unlocking the job directly in run_job(), we'll use
        # a future callback.
        #
        # This simplifies run_job() by not needing a try/finally clause,
        # and also guarantees jobs will be unlocked if the executor shuts
        # down with jobs that haven't been given to a worker thread yet.

        def callback(fut: Future) -> None:
            # Careful! Future callbacks run in the worker's thread.
            # We need a new queue here.
            with self.queue_factory() as queue:
                queue.unlock_job(job_id)

        return callback


_print = print


def print(*args, sep: str = " ", end: str = "\n", **kwargs) -> None:
    # thread-safe print
    message = sep.join(map(str, args)) + end
    _print(message, sep="", end="", **kwargs)


if __name__ == "__main__":
    main()
