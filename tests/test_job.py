from joblin import Scheduler

DATA = 123


def test_job_complete(scheduler: Scheduler):
    job = scheduler.add_job(DATA)
    job.complete()

    # assert job.completed_at == 0

    fetched = scheduler.get_job_by_id(job.id)
    assert fetched is not None
    assert fetched.completed_at == 0


def test_job_delete(scheduler: Scheduler):
    job = scheduler.add_job(DATA)
    job.delete()
    assert scheduler.get_job_by_id(job.id) is None


def test_job_lock(scheduler: Scheduler):
    job = scheduler.add_job(DATA)
    assert job.lock(locked_at=1) is True

    job = scheduler.get_job_by_id(job.id)
    assert job is not None
    assert job.locked_at == 1


def test_job_unlock(scheduler: Scheduler):
    job = scheduler.add_job(DATA)
    assert job.lock() is True
    assert job.unlock() is True

    job = scheduler.get_job_by_id(job.id)
    assert job is not None
    assert job.locked_at is None


def test_job_get_seconds_until_start(scheduler: Scheduler):
    job = scheduler.add_job(DATA, starts_at=3)
    assert job.get_seconds_until_start() == 3
