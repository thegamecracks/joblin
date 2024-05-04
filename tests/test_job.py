from joblin import Queue

DATA = 123


def test_job_complete(queue: Queue):
    job = queue.add_job(DATA)
    job.complete()

    # assert job.completed_at == 0

    fetched = queue.get_job_by_id(job.id)
    assert fetched is not None
    assert fetched.completed_at == 0


def test_job_delete(queue: Queue):
    job = queue.add_job(DATA)
    job.delete()
    assert queue.get_job_by_id(job.id) is None


def test_job_lock(queue: Queue):
    job = queue.add_job(DATA)
    assert job.lock(locked_at=1) is True

    job = queue.get_job_by_id(job.id)
    assert job is not None
    assert job.locked_at == 1


def test_job_unlock(queue: Queue):
    job = queue.add_job(DATA)
    assert job.lock() is True
    assert job.unlock() is True

    job = queue.get_job_by_id(job.id)
    assert job is not None
    assert job.locked_at is None


def test_job_delay(queue: Queue):
    job = queue.add_job(DATA, starts_at=3)
    assert job.delay == 3
