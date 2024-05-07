Changelog
=========

All notable changes to this project will be documented in this file.

The format is based on `Keep a Changelog`_.

.. _Keep a Changelog: https://keepachangelog.com/en/1.1.0/

Unreleased
----------

v0.3.0.post2 (2024-05-07)
-------------------------

This post-release contains minor documentation improvements.

v0.3.0.post1 (2024-05-05)
-------------------------

This post-release contains minor documentation improvements.

v0.3.0 (2024-05-05)
-------------------

Several breaking changes have been introduced in this release.

``Scheduler`` has been renamed to :class:`~joblin.Queue` to better reflect its purpose.

Preliminary support for locking jobs has been added to allow for
multiple workers concurrently consuming jobs from the database.
Users must call the new locking methods to take advantage of this.

This locking mechanism is entirely experimental. For now, the queue
does not provide a context manager to lock and unlock jobs, which poses
a risk of leaving jobs deadlocked.

See `examples/thread_pool.py`_ and `examples/threads.py`_ for demonstrations
of the new API.

.. _examples/thread_pool.py: https://github.com/thegamecracks/joblin/blob/main/examples/thread_pool.py
.. _examples/threads.py: https://github.com/thegamecracks/joblin/blob/main/examples/threads.py

Added
^^^^^

- :class:`Queue <joblin.Queue>` (renamed from ``Scheduler``)
- :meth:`Queue.get_next_job_delay() <joblin.Queue.get_next_job_delay>`
  (replaces ``Queue.get_seconds_until_next_job()``)
- :meth:`Queue.lock_job() <joblin.Queue.lock_job>`
- :meth:`Queue.lock_next_job() <joblin.Queue.lock_next_job>`
- :meth:`Queue.lock_next_job_delay() <joblin.Queue.lock_next_job_delay>`
- :meth:`Queue.unlock_job() <joblin.Queue.unlock_job>`
- :attr:`Job.queue <joblin.Job.queue>` (renamed from ``Job.scheduler``)
- :attr:`Job.locked_at <joblin.Job.locked_at>`
- :meth:`Job.lock() <joblin.Job.lock>`
- :meth:`Job.unlock() <joblin.Job.unlock>`
- :attr:`Job.delay <joblin.Job.delay>` (replaces ``Job.get_seconds_until_start()``)

Changed
^^^^^^^

- BREAKING CHANGE:
  Job and Queue delay methods no longer return negative delays
  to simplify usage for end users. If a negative delay is still
  desired to know how overdue a job is, users will have to do
  ``job.starts_at - queue.time()`` instead.
- :meth:`Queue.get_next_job() <joblin.Queue.get_next_job>`
  and :meth:`Queue.get_next_job_delay() <joblin.Queue.get_next_job_delay>`
  are now guaranteed to return the job with the smaller ID if two jobs
  started at the same time. Previously this was not part of the database
  query, making the ordering reliant on SQLite's implementation details.

Removed
^^^^^^^

- ``Scheduler`` (renamed to :class:`~joblin.Queue`)
- ``Queue.get_seconds_until_next_job()`` in favour of
  :meth:`Queue.get_next_job_delay() <joblin.Queue.get_next_job_delay>`
- ``Job.scheduler`` (renamed to :attr:`Job.queue <joblin.Job.queue>`)
- ``Job.get_seconds_until_start()`` in favour of :attr:`Job.delay <joblin.Job.delay>`

v0.2.1 (2024-05-02)
-------------------

This release includes more documentation enhancements and test coverage.

Fixed
^^^^^

- Don't apply default values when ``0`` is passed for any time parameter
  in Job / Scheduler methods

  This fix mainly applies to users that provide their own time functions
  for the scheduler.

v0.2.0 (2024-05-01)
-------------------

This release provides this documentation site along with a few minor changes.

Added
^^^^^

- More inline documentation to source code
- :meth:`Scheduler.close() <joblin.Scheduler.close>`
  alternative to the context manager protocol

v0.1.1 (2024-05-01)
-------------------

Fixed
^^^^^

- Make :meth:`Job.complete(completed_at=) <joblin.Job.complete>` parameter
  optional as implied by documentation
- Fix readme example passing a negative delay to :func:`time.sleep()`

v0.1.0 (2024-05-01)
-------------------

This marks the first release of the joblin library, rewritten from the
`original gist`_.

.. _original gist: https://gist.github.com/thegamecracks/f9e8cafc350fa8296e4e2de7cb529046
