Changelog
=========

All notable changes to this project will be documented in this file.

The format is based on `Keep a Changelog`_.

.. _Keep a Changelog: https://keepachangelog.com/en/1.1.0/

Unreleased
----------

Changed
^^^^^^^

- :meth:`Scheduler.get_next_job() <joblin.Scheduler.get_next_job>`
  and :meth:`Scheduler.get_seconds_until_next_job() <joblin.Scheduler.get_seconds_until_next_job>`
  are now guaranteed to return the job with the smaller ID if two jobs
  started at the same time. Previously this was not part of the database
  query, making the ordering reliant on SQLite's implementation details.

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