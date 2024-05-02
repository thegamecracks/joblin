import sqlite3
from pathlib import Path
from typing import Iterator

import pytest
from pytest import FixtureRequest

from joblin import Scheduler


@pytest.fixture(params=["memory", "file"])
def scheduler(request: FixtureRequest, tmp_path: Path) -> Iterator[Scheduler]:
    if request.param == "memory":
        scheduler = Scheduler.connect(":memory:", time_func=lambda: 0)
    elif request.param == "file":
        db = str(tmp_path / "job.db")
        scheduler = Scheduler.connect(db, time_func=lambda: 0)
    else:
        raise ValueError(f"unexpected param {request.param!r}")

    try:
        with scheduler:
            yield scheduler
    finally:
        with pytest.raises(sqlite3.ProgrammingError):
            scheduler.conn.execute("SELECT 1")
