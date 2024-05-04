Changelog
=========

All notable changes to this project will be documented in this file.

The format is based on `Keep a Changelog`_.

.. _Keep a Changelog: https://keepachangelog.com/en/1.1.0/

Unreleased
----------

This release provides preliminary support for locking jobs so that
multiple workers can concurrently consume jobs from the database.
However, users must opt into locking with the new methods provided.

This locking mechanism is entirely experimental. For now, the scheduler
does not provide a context manager to lock and unlock jobs, which poses
a risk of leaving jobs deadlocked.

See `examples/thread_pool.py`_ for a demonstration of the new API.

.. _examples/thread_pool.py: https://github.com/thegamecracks/joblin/blob/main/examples/thread_pool.py

Added
^^^^^

- :meth:`Scheduler.get_next_job_delay() <joblin.Scheduler.get_next_job_delay>`
  (replaces ``Scheduler.get_seconds_until_next_job()``)
- :meth:`Scheduler.lock_job() <joblin.Scheduler.lock_job>`
- :meth:`Scheduler.lock_next_job() <joblin.Scheduler.lock_next_job>`
- :meth:`Scheduler.lock_next_job_delay() <joblin.Scheduler.lock_next_job_delay>`
- :meth:`Scheduler.unlock_job() <joblin.Scheduler.unlock_job>`
- :attr:`Job.locked_at <joblin.Job.locked_at>`
- :meth:`Job.lock() <joblin.Job.lock>`
- :meth:`Job.unlock() <joblin.Job.unlock>`
- :attr:`Job.delay <joblin.Job.delay>` (replaces ``Job.get_seconds_until_start()``)

Changed
^^^^^^^

- BREAKING CHANGE:
  Job and Scheduler delay methods can no longer return negative delays.
  This is to simplify usage for end users. If a negative delay is still
  desired, users will have to do ``job.starts_at - scheduler.time()``.
- :meth:`Scheduler.get_next_job() <joblin.Scheduler.get_next_job>`
  and :meth:`Scheduler.get_seconds_until_next_job() <joblin.Scheduler.get_seconds_until_next_job>`
  are now guaranteed to return the job with the smaller ID if two jobs
  started at the same time. Previously this was not part of the database
  query, making the ordering reliant on SQLite's implementation details.

Removed
^^^^^^^

- ``Scheduler.get_seconds_until_next_job()`` in favour of
  :meth:`Scheduler.get_next_job_delay() <joblin.Scheduler.get_next_job_delay>`
- ``Job.get_seconds_until_start()`` in favour of :attr:`Job.delay <joblin.Job.delay>`

v0.2.1 - 2024-05-02
-------------------

This release includes more documentation enhancements and test coverage.

Fixed
^^^^^

- Don't apply default values when ``0`` is passed for any time parameter
  in Job / Scheduler methods

  This fix mainly applies to users that provide their own time functions
  for the scheduler.

v0.2.0 - 2024-05-01
-------------------

This release provides this documentation site along with a few minor changes.

Added
^^^^^

- More inline documentation to source code
- :meth:`Scheduler.close() <joblin.Scheduler.close>`
  alternative to the context manager protocol

v0.1.1 - 2024-05-01
-------------------

Fixed
^^^^^

- Make :meth:`Job.complete(completed_at=) <joblin.Job.complete>` parameter
  optional as implied by documentation
- Fix readme example passing a negative delay to :func:`time.sleep()`

v0.1.0 - 2024-05-01
-------------------

This marks the first release of the joblin library, rewritten from the
`original gist`_.

.. _original gist: https://gist.github.com/thegamecracks/f9e8cafc350fa8296e4e2de7cb529046
