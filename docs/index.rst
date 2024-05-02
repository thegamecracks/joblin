.. joblin documentation master file, created by
   sphinx-quickstart on Wed May  1 16:08:48 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Joblin!
==================

|pyright-workflow| |pytest-workflow| |docs-workflow|

.. |pyright-workflow| image:: https://img.shields.io/github/actions/workflow/status/thegamecracks/joblin/pyright-lint.yml?style=flat-square&label=pyright
   :target: https://microsoft.github.io/pyright/#/

.. |pytest-workflow| image:: https://img.shields.io/github/actions/workflow/status/thegamecracks/joblin/python-test.yml?style=flat-square&logo=pytest&label=tests
   :target: https://docs.pytest.org/en/stable/

.. |docs-workflow| image:: https://img.shields.io/github/actions/workflow/status/thegamecracks/joblin/publish-docs.yml?style=flat-square&logo=github&label=docs
   :target: https://thegamecracks.github.io/joblin/

Joblin is a simple, SQLite-based, synchronous, Python job scheduler library.

.. code-block:: python

   import time
   from joblin import Scheduler

   with Scheduler.connect("job.db") as scheduler:
      data = '{"type": "my-type", "message": "Hello world!"}'
      scheduler.add_job_from_now(data, starts_after=3.0, expires_after=10.0)

      while (job := scheduler.get_next_job()) is not None:
         time.sleep(max(0, job.get_seconds_until_start()))
         print(f"Received job {job.id} with data: {job.data}")
         job.complete()

.. toctree::
   :maxdepth: 2
   :caption: Contents:

Usage
-----

With Python 3.11+ and Git, this library can be installed using:

.. code-block:: shell

   pip install git+https://github.com/thegamecracks/joblin

Afterwards, you can import ``joblin`` and use its :class:`~joblin.Scheduler`
class to start storing jobs.

Examples
--------

Check out the `examples/ <https://github.com/thegamecracks/joblin/tree/main/examples/>`_
for reference on using the scheduler:

.. image:: https://raw.githubusercontent.com/thegamecracks/joblin/main/examples/tkinter_app.png
   :target: https://github.com/thegamecracks/joblin/tree/main/examples/tkinter_app.py

License
-------

This project is written under the MIT license.

API Reference
-------------

.. autoclass:: joblin.Scheduler

   .. autoclasstoc::

.. autoclass:: joblin.Job
