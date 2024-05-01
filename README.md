# joblin

A simple, SQLite-based, synchronous, Python job scheduler library.

```py
import time
from joblin import Scheduler

with Scheduler.connect("job.db") as scheduler:
    data = '{"type": "my-type", "message": "Hello world!"}'
    scheduler.add_job_from_now(data, expires_after=5.0)

    while (job := scheduler.get_next_job()) is not None:
        time.sleep(job.get_seconds_until_start())
        print(f"Received job {job.id} with data: {job.data}")
        job.complete()
```

## Usage

With Python 3.11+ and Git, this library can be installed using:

```sh
pip install git+https://github.com/thegamecracks/dum-dum-irc
```

Afterwards, the `joblin` package should be available to your scripts.

## Examples

Check out the [examples/] for reference on using the scheduler:

![](https://raw.githubusercontent.com/thegamecracks/joblin/main/examples/tkinter_app.png)

[examples/]: https://github.com/thegamecracks/joblin/tree/main/examples/

## License

This project is written under the MIT license.
