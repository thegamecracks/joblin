import sqlite3

import pytest

from joblin import Job, Queue

DATA = 123


def test_queue_add_job(queue: Queue):
    expected = Job(
        queue=queue,
        id=1,
        data=DATA,
        created_at=1,
        starts_at=2,
        expires_at=3,
        completed_at=None,
        locked_at=None,
    )

    job = queue.add_job(DATA, created_at=1, starts_at=2, expires_at=3)
    assert job == expected
    assert queue.get_job_by_id(job.id) == expected


def test_queue_add_job_defaults(queue: Queue):
    expected = Job(
        queue=queue,
        id=1,
        data=DATA,
        created_at=0,
        starts_at=0,
        expires_at=None,
        completed_at=None,
        locked_at=None,
    )

    job = queue.add_job(DATA)
    assert job == expected
    assert queue.get_job_by_id(job.id) == expected


def test_queue_add_job_from_now(queue: Queue):
    expected = Job(
        queue=queue,
        id=1,
        data=DATA,
        created_at=1,
        starts_at=2,
        expires_at=3,
        completed_at=None,
        locked_at=None,
    )

    job = queue.add_job_from_now(DATA, created_at=1, starts_after=1, expires_after=2)
    assert job == expected
    assert queue.get_job_by_id(job.id) == expected


def test_queue_add_job_from_now_defaults(queue: Queue):
    expected = Job(
        queue=queue,
        id=1,
        data=DATA,
        created_at=0,
        starts_at=0,
        expires_at=None,
        completed_at=None,
        locked_at=None,
    )

    job = queue.add_job_from_now(DATA)
    assert job == expected
    assert queue.get_job_by_id(job.id) == expected


def test_queue_add_job_with_same_creation_and_start(queue: Queue):
    job = queue.add_job(DATA, created_at=1, starts_at=1)
    assert queue.get_next_job(now=1) == job


def test_queue_add_job_with_same_start_and_expiry(queue: Queue):
    job = queue.add_job(DATA, starts_at=1, expires_at=1)
    assert queue.get_next_job(now=0) == job
    assert queue.get_next_job(now=1) is None


def test_queue_add_job_with_invalid_start(queue: Queue):
    with pytest.raises(sqlite3.IntegrityError):
        queue.add_job(DATA, created_at=1, starts_at=0)


def test_queue_add_job_with_invalid_expiry(queue: Queue):
    with pytest.raises(sqlite3.IntegrityError):
        queue.add_job(DATA, starts_at=1, expires_at=0)


def test_queue_get_next_job(queue: Queue):
    assert queue.get_next_job(now=0) is None

    job1 = queue.add_job(DATA, created_at=0, starts_at=2, expires_at=4)
    for i in range(4):
        assert queue.get_next_job(now=i) == job1
    assert queue.get_next_job(now=4) is None

    job2 = queue.add_job(DATA, created_at=0, starts_at=1, expires_at=None)
    for i in range(5):
        assert queue.get_next_job(now=i) == job2

    queue.delete_job(job1.id)
    queue.delete_job(job2.id)

    assert queue.get_next_job(now=0) is None


def test_queue_get_next_job_delay(queue: Queue):
    assert queue.get_next_job_delay(now=0) is None

    job1 = queue.add_job(DATA, created_at=0, starts_at=2, expires_at=4)
    for i in range(4):
        assert queue.get_next_job_delay(now=i) == (job1.id, max(0.0, 2 - i))
    assert queue.get_next_job_delay(now=4) is None

    job2 = queue.add_job(DATA, created_at=0, starts_at=1, expires_at=None)
    for i in range(5):
        assert queue.get_next_job_delay(now=i) == (job2.id, max(0.0, 1 - i))

    queue.delete_job(job1.id)
    queue.delete_job(job2.id)

    assert queue.get_next_job_delay(now=0) is None


def test_queue_count_pending_jobs(queue: Queue):
    for i in range(1, 101):
        queue.add_job(DATA, created_at=0, starts_at=i, expires_at=i)

        # Completed jobs should be ignored
        job = queue.add_job(DATA, created_at=0, starts_at=i, expires_at=i)
        queue.complete_job(job.id)

    for i in range(101):
        assert queue.count_pending_jobs(now=i) == 100 - i


def test_queue_complete_job(queue: Queue):
    job = queue.add_job(None)
    assert queue.get_next_job() == job
    assert queue.get_next_job_delay() == (job.id, 0)
    assert queue.count_pending_jobs() == 1

    job_updated = queue.complete_job(job.id)
    assert job_updated is True

    assert queue.get_next_job() is None
    assert queue.get_next_job_delay() is None
    assert queue.count_pending_jobs() == 0

    fetched = queue.get_job_by_id(job.id)
    assert fetched is not None
    assert fetched.completed_at == 0

    assert queue.complete_job(job.id) is True
    queue.delete_job(job.id)
    assert queue.complete_job(job.id) is False


def test_queue_delete_job(queue: Queue):
    job = queue.add_job(DATA)
    assert queue.get_job_by_id(job.id) == job

    queue.delete_job(job.id)
    assert queue.get_job_by_id(job.id) is None

    queue.delete_job(job.id)  # Should be idempotent


def test_queue_delete_completed_jobs(queue: Queue):
    for _ in range(50):
        queue.add_job(DATA)

    for _ in range(50):
        job = queue.add_job(DATA)
        queue.complete_job(job.id)

    assert queue.conn.execute("SELECT COUNT(*) FROM job").fetchone()[0] == 100
    queue.delete_completed_jobs()
    assert queue.conn.execute("SELECT COUNT(*) FROM job").fetchone()[0] == 50


def test_queue_delete_expired_jobs(queue: Queue):
    for _ in range(50):
        queue.add_job(DATA, expires_at=1)

    for _ in range(50):
        queue.add_job(DATA, expires_at=2)

    queue.delete_expired_jobs(now=0)
    assert queue.conn.execute("SELECT COUNT(*) FROM job").fetchone()[0] == 100
    queue.delete_expired_jobs(now=1)
    assert queue.conn.execute("SELECT COUNT(*) FROM job").fetchone()[0] == 50
    queue.delete_expired_jobs(now=2)
    assert queue.conn.execute("SELECT COUNT(*) FROM job").fetchone()[0] == 0


def test_queue_job_ids_not_reused(queue: Queue):
    # Relies on INTEGER PRIMARY KEY AUTOINCREMENT
    job1 = queue.add_job(DATA)
    job2 = queue.add_job(DATA)
    assert job1.id == 1
    assert job2.id == 2

    queue.delete_job(job1.id)
    queue.delete_job(job2.id)
    job3 = queue.add_job(DATA)
    job4 = queue.add_job(DATA)
    assert job3.id == 3
    assert job4.id == 4


def test_queue_next_job_ordered_by_id(queue: Queue):
    jobs = [queue.add_job(DATA) for _ in range(100)]
    for job in jobs:
        assert queue.get_next_job() == job
        assert queue.get_next_job_delay() == (job.id, 0)
        queue.delete_job(job.id)


def test_queue_next_job_ordered_by_id_reversed(queue: Queue):
    for id_ in range(100, 0, -1):
        queue.conn.execute(
            "INSERT INTO job (id, created_at, starts_at) VALUES (?, 0, 0)",
            (id_,),
        )

    for id_ in range(1, 101):
        job = queue.get_next_job()
        assert job is not None
        assert job.id == id_
        assert queue.get_next_job_delay() == (id_, 0)
        queue.delete_job(id_)


def test_queue_time_func(queue: Queue):
    def time_func() -> float:
        return current_time

    queue.time_func = time_func

    for current_time in range(100):
        assert queue.time() == current_time
        assert queue.add_job(123).created_at == current_time
