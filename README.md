# joblin

[![](https://img.shields.io/github/actions/workflow/status/thegamecracks/joblin/pyright-lint.yml?style=flat-square&label=pyright)](https://microsoft.github.io/pyright/#/)
[![](https://img.shields.io/github/actions/workflow/status/thegamecracks/joblin/python-test.yml?style=flat-square&logo=pytest&label=tests)](https://docs.pytest.org/en/stable/)
[![](https://img.shields.io/github/actions/workflow/status/thegamecracks/joblin/publish-docs.yml?style=flat-square&logo=github&label=docs)](https://thegamecracks.github.io/joblin/)

A simple, SQLite-based, synchronous, Python job queue library.

```py
import time
from joblin import Queue

with Queue.connect("job.db") as queue:
    data = '{"type": "my-type", "message": "Hello world!"}'
    queue.add_job_from_now(data, starts_after=3.0, expires_after=10.0)

    while (job := queue.get_next_job()) is not None:
        time.sleep(job.delay)
        print(f"Received job {job.id} with data: {job.data}")
        job.complete()
```

## Usage

With Python 3.11+ and Git, this library can be installed using:

```sh
pip install git+https://github.com/thegamecracks/joblin@v0.3.0
```

Afterwards you can import `joblin` and use the `Queue` class
to start storing jobs. See the [documentation] for more details.

[documentation]: https://thegamecracks.github.io/joblin/

## Examples

Check out the [examples/] for reference on using the queue:

[![](https://raw.githubusercontent.com/thegamecracks/joblin/main/examples/tkinter_app.png)](https://github.com/thegamecracks/joblin/tree/main/examples/tkinter_app.py)

[examples/]: https://github.com/thegamecracks/joblin/tree/main/examples/

## TODO

- [ ] Add more examples (in-memory scheduling, concurrency, asyncio, ...)

## License

This project is written under the MIT license.
