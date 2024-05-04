import sqlite3
from pathlib import Path
from typing import Iterator

import pytest
from pytest import FixtureRequest

from joblin import Queue


@pytest.fixture(params=["memory", "file"])
def queue(request: FixtureRequest, tmp_path: Path) -> Iterator[Queue]:
    if request.param == "memory":
        queue = Queue.connect(":memory:", time_func=lambda: 0)
    elif request.param == "file":
        db = str(tmp_path / "job.db")
        queue = Queue.connect(db, time_func=lambda: 0)
    else:
        raise ValueError(f"unexpected param {request.param!r}")

    try:
        with queue:
            yield queue
    finally:
        with pytest.raises(sqlite3.ProgrammingError):
            queue.conn.execute("SELECT 1")
