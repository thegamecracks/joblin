import sqlite3

import pytest

from joblin import Job, Scheduler

DATA = 123


def test_scheduler_add_job(scheduler: Scheduler):
    expected = Job(
        scheduler=scheduler,
        id=1,
        data=DATA,
        created_at=1,
        starts_at=2,
        expires_at=3,
        completed_at=None,
        locked_at=None,
    )

    job = scheduler.add_job(DATA, created_at=1, starts_at=2, expires_at=3)
    assert job == expected
    assert scheduler.get_job_by_id(job.id) == expected


def test_scheduler_add_job_defaults(scheduler: Scheduler):
    expected = Job(
        scheduler=scheduler,
        id=1,
        data=DATA,
        created_at=0,
        starts_at=0,
        expires_at=None,
        completed_at=None,
        locked_at=None,
    )

    job = scheduler.add_job(DATA)
    assert job == expected
    assert scheduler.get_job_by_id(job.id) == expected


def test_scheduler_add_job_from_now(scheduler: Scheduler):
    expected = Job(
        scheduler=scheduler,
        id=1,
        data=DATA,
        created_at=1,
        starts_at=2,
        expires_at=3,
        completed_at=None,
        locked_at=None,
    )

    job = scheduler.add_job_from_now(DATA, created_at=1, starts_after=1, expires_after=2)
    assert job == expected
    assert scheduler.get_job_by_id(job.id) == expected


def test_scheduler_add_job_from_now_defaults(scheduler: Scheduler):
    expected = Job(
        scheduler=scheduler,
        id=1,
        data=DATA,
        created_at=0,
        starts_at=0,
        expires_at=None,
        completed_at=None,
        locked_at=None,
    )

    job = scheduler.add_job_from_now(DATA)
    assert job == expected
    assert scheduler.get_job_by_id(job.id) == expected


def test_scheduler_add_job_with_same_creation_and_start(scheduler: Scheduler):
    job = scheduler.add_job(DATA, created_at=1, starts_at=1)
    assert scheduler.get_next_job(now=1) == job


def test_scheduler_add_job_with_same_start_and_expiry(scheduler: Scheduler):
    job = scheduler.add_job(DATA, starts_at=1, expires_at=1)
    assert scheduler.get_next_job(now=0) == job
    assert scheduler.get_next_job(now=1) is None


def test_scheduler_add_job_with_invalid_start(scheduler: Scheduler):
    with pytest.raises(sqlite3.IntegrityError):
        scheduler.add_job(DATA, created_at=1, starts_at=0)


def test_scheduler_add_job_with_invalid_expiry(scheduler: Scheduler):
    with pytest.raises(sqlite3.IntegrityError):
        scheduler.add_job(DATA, starts_at=1, expires_at=0)


def test_scheduler_get_next_job(scheduler: Scheduler):
    assert scheduler.get_next_job(now=0) is None

    job1 = scheduler.add_job(DATA, created_at=0, starts_at=2, expires_at=4)
    for i in range(4):
        assert scheduler.get_next_job(now=i) == job1
    assert scheduler.get_next_job(now=4) is None

    job2 = scheduler.add_job(DATA, created_at=0, starts_at=1, expires_at=None)
    for i in range(5):
        assert scheduler.get_next_job(now=i) == job2

    scheduler.delete_job(job1.id)
    scheduler.delete_job(job2.id)

    assert scheduler.get_next_job(now=0) is None


def test_scheduler_get_next_job_delay(scheduler: Scheduler):
    assert scheduler.get_next_job_delay(now=0) is None

    job1 = scheduler.add_job(DATA, created_at=0, starts_at=2, expires_at=4)
    for i in range(4):
        assert scheduler.get_next_job_delay(now=i) == (job1.id, max(0.0, 2 - i))
    assert scheduler.get_next_job_delay(now=4) is None

    job2 = scheduler.add_job(DATA, created_at=0, starts_at=1, expires_at=None)
    for i in range(5):
        assert scheduler.get_next_job_delay(now=i) == (job2.id, max(0.0, 1 - i))

    scheduler.delete_job(job1.id)
    scheduler.delete_job(job2.id)

    assert scheduler.get_next_job_delay(now=0) is None


def test_scheduler_count_pending_jobs(scheduler: Scheduler):
    for i in range(1, 101):
        scheduler.add_job(DATA, created_at=0, starts_at=i, expires_at=i)

        # Completed jobs should be ignored
        job = scheduler.add_job(DATA, created_at=0, starts_at=i, expires_at=i)
        scheduler.complete_job(job.id)

    for i in range(101):
        assert scheduler.count_pending_jobs(now=i) == 100 - i


def test_scheduler_complete_job(scheduler: Scheduler):
    job = scheduler.add_job(None)
    assert scheduler.get_next_job() == job
    assert scheduler.get_next_job_delay() == (job.id, 0)
    assert scheduler.count_pending_jobs() == 1

    job_updated = scheduler.complete_job(job.id)
    assert job_updated is True

    assert scheduler.get_next_job() is None
    assert scheduler.get_next_job_delay() is None
    assert scheduler.count_pending_jobs() == 0

    fetched = scheduler.get_job_by_id(job.id)
    assert fetched is not None
    assert fetched.completed_at == 0

    assert scheduler.complete_job(job.id) is True
    scheduler.delete_job(job.id)
    assert scheduler.complete_job(job.id) is False


def test_scheduler_delete_job(scheduler: Scheduler):
    job = scheduler.add_job(DATA)
    assert scheduler.get_job_by_id(job.id) == job

    scheduler.delete_job(job.id)
    assert scheduler.get_job_by_id(job.id) is None

    scheduler.delete_job(job.id)  # Should be idempotent


def test_scheduler_delete_completed_jobs(scheduler: Scheduler):
    for _ in range(50):
        scheduler.add_job(DATA)

    for _ in range(50):
        job = scheduler.add_job(DATA)
        scheduler.complete_job(job.id)

    assert scheduler.conn.execute("SELECT COUNT(*) FROM job").fetchone()[0] == 100
    scheduler.delete_completed_jobs()
    assert scheduler.conn.execute("SELECT COUNT(*) FROM job").fetchone()[0] == 50


def test_scheduler_delete_expired_jobs(scheduler: Scheduler):
    for _ in range(50):
        scheduler.add_job(DATA, expires_at=1)

    for _ in range(50):
        scheduler.add_job(DATA, expires_at=2)

    scheduler.delete_expired_jobs(now=0)
    assert scheduler.conn.execute("SELECT COUNT(*) FROM job").fetchone()[0] == 100
    scheduler.delete_expired_jobs(now=1)
    assert scheduler.conn.execute("SELECT COUNT(*) FROM job").fetchone()[0] == 50
    scheduler.delete_expired_jobs(now=2)
    assert scheduler.conn.execute("SELECT COUNT(*) FROM job").fetchone()[0] == 0


def test_scheduler_job_ids_not_reused(scheduler: Scheduler):
    # Relies on INTEGER PRIMARY KEY AUTOINCREMENT
    job1 = scheduler.add_job(DATA)
    job2 = scheduler.add_job(DATA)
    assert job1.id == 1
    assert job2.id == 2

    scheduler.delete_job(job1.id)
    scheduler.delete_job(job2.id)
    job3 = scheduler.add_job(DATA)
    job4 = scheduler.add_job(DATA)
    assert job3.id == 3
    assert job4.id == 4


def test_scheduler_next_job_ordered_by_id(scheduler: Scheduler):
    jobs = [scheduler.add_job(DATA) for _ in range(100)]
    for job in jobs:
        assert scheduler.get_next_job() == job
        assert scheduler.get_next_job_delay() == (job.id, 0)
        scheduler.delete_job(job.id)


def test_scheduler_next_job_ordered_by_id_reversed(scheduler: Scheduler):
    for id_ in range(100, 0, -1):
        scheduler.conn.execute(
            "INSERT INTO job (id, created_at, starts_at) VALUES (?, 0, 0)",
            (id_,),
        )

    for id_ in range(1, 101):
        job = scheduler.get_next_job()
        assert job is not None
        assert job.id == id_
        assert scheduler.get_next_job_delay() == (id_, 0)
        scheduler.delete_job(id_)


def test_scheduler_time_func(scheduler: Scheduler):
    def time_func() -> float:
        return current_time

    scheduler.time_func = time_func

    for current_time in range(100):
        assert scheduler.time() == current_time
        assert scheduler.add_job(123).created_at == current_time
