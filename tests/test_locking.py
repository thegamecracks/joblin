# TODO: write concurrency tests
from joblin import Scheduler

DATA = 123


def test_lock_job(scheduler: Scheduler):
    job = scheduler.add_job(DATA)
    assert scheduler.lock_job(job.id, locked_at=1) is True

    assert scheduler.get_next_job() is None

    job = scheduler.get_job_by_id(job.id)
    assert job is not None
    assert job.locked_at == 1

    assert scheduler.lock_job(job.id, locked_at=2) is False
    job = scheduler.get_job_by_id(job.id)
    assert job is not None
    assert job.locked_at == 1


def test_unlock_job(scheduler: Scheduler):
    job = scheduler.add_job(DATA)
    assert scheduler.lock_job(job.id) is True
    assert scheduler.unlock_job(job.id) is True
    assert scheduler.unlock_job(job.id) is True

    assert scheduler.get_next_job() == job

    job = scheduler.get_job_by_id(job.id)
    assert job is not None
    assert job.locked_at is None


def test_lock_next_job(scheduler: Scheduler):
    jobs = [scheduler.add_job(DATA) for _ in range(100)]

    for job in jobs:
        locked = scheduler.lock_next_job(now=1)
        assert locked is not None
        assert locked.id == job.id
        assert locked.locked_at == 1

    assert scheduler.lock_next_job() is None

    for job in reversed(jobs):
        assert scheduler.unlock_job(job.id) is True
        assert scheduler.get_next_job() == job


def test_lock_next_job_delay(scheduler: Scheduler):
    jobs = [scheduler.add_job(DATA) for _ in range(100)]

    for job in jobs:
        locked = scheduler.lock_next_job_delay()
        assert locked is not None
        assert locked == (job.id, 0)

    assert scheduler.lock_next_job_delay() is None

    for job in reversed(jobs):
        assert scheduler.unlock_job(job.id) is True
        assert scheduler.get_next_job() == job
