# TODO: write concurrency tests
from joblin import Queue

DATA = 123


def test_lock_job(queue: Queue):
    job = queue.add_job(DATA)
    assert queue.lock_job(job.id, locked_at=1) is True

    assert queue.get_next_job() is None

    job = queue.get_job_by_id(job.id)
    assert job is not None
    assert job.locked_at == 1

    assert queue.lock_job(job.id, locked_at=2) is False
    job = queue.get_job_by_id(job.id)
    assert job is not None
    assert job.locked_at == 1


def test_unlock_job(queue: Queue):
    job = queue.add_job(DATA)
    assert queue.lock_job(job.id) is True
    assert queue.unlock_job(job.id) is True
    assert queue.unlock_job(job.id) is True

    assert queue.get_next_job() == job

    job = queue.get_job_by_id(job.id)
    assert job is not None
    assert job.locked_at is None


def test_lock_next_job(queue: Queue):
    jobs = [queue.add_job(DATA) for _ in range(100)]

    for job in jobs:
        locked = queue.lock_next_job(now=1)
        assert locked is not None
        assert locked.id == job.id
        assert locked.locked_at == 1

    assert queue.lock_next_job() is None

    for job in reversed(jobs):
        assert queue.unlock_job(job.id) is True
        assert queue.get_next_job() == job


def test_lock_next_job_delay(queue: Queue):
    jobs = [queue.add_job(DATA) for _ in range(100)]

    for job in jobs:
        locked = queue.lock_next_job_delay()
        assert locked is not None
        assert locked == (job.id, 0)

    assert queue.lock_next_job_delay() is None

    for job in reversed(jobs):
        assert queue.unlock_job(job.id) is True
        assert queue.get_next_job() == job
