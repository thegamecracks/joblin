import importlib.resources
import logging
import re
import sqlite3
from contextlib import contextmanager
from typing import Iterable, Iterator, NamedTuple, Protocol, Self


log = logging.getLogger(__name__)


class Migration(NamedTuple):
    version: int
    sql: str


class Migrations(tuple[Migration, ...]):
    def after_version(self, version: int) -> Self:
        """Return a copy of self with only migrations after the given version."""
        return type(self)(m for m in self if m.version > version)

    def version_exists(self, version: int) -> bool:
        return any(m.version == version for m in self)

    @classmethod
    def from_iterable_unsorted(cls, it: Iterable[Migration]) -> Self:
        return cls(sorted(it, key=lambda m: m.version))


class MigrationFinder(Protocol):
    def discover(self) -> Migrations: ...


class JoblinMigrationFinder(MigrationFinder):
    _FILE_PATTERN = re.compile(r"(\d+)-(.+)\.sql")

    def discover(self) -> Migrations:
        migrations: list[Migration] = [Migration(version=-1, sql="")]

        assert __package__ is not None
        path = importlib.resources.files(__package__).joinpath("migrations/")

        for file in path.iterdir():
            if not file.is_file():
                continue

            m = self._FILE_PATTERN.fullmatch(file.name)
            if m is None:
                continue

            version = int(m[1])
            sql = file.read_text("utf-8")
            migrations.append(Migration(version=version, sql=sql))

        return Migrations.from_iterable_unsorted(migrations)


class Migrator:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def run_migrations(self, migrations: Migrations) -> None:
        version = self.get_version()
        if version >= 0 and not migrations.version_exists(version):
            log.warning(
                "Unrecognized database version %s, skipping migrations",
                version,
            )
            return

        with self.begin() as conn:
            for version, script in migrations.after_version(version):
                log.debug("Migrating database to v%s", version)
                conn.executescript(script)

            conn.execute(
                "UPDATE job_schema SET value = ? WHERE key = 'version'",
                (version,),
            )

    def get_version(self) -> int:
        try:
            c = self.conn.execute("SELECT value FROM job_schema WHERE key = 'version'")
        except sqlite3.OperationalError:
            log.debug("job_schema not present, checking for job table")
            c = self.conn.execute(
                "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'job'"
            )

            if c.fetchone() is None:
                log.debug("job table not present, assuming version -1")
                return -1

            log.debug("job table exists, assuming version 0")
            return 0

        row = c.fetchone()
        if row is None:
            raise RuntimeError("missing 'version' key in job_schema table")

        version = int(row[0])
        log.debug("job_schema version returned %s", version)
        return version

    @contextmanager
    def begin(self) -> Iterator[sqlite3.Connection]:
        self.conn.execute("BEGIN")
        try:
            yield self.conn
        except BaseException:
            self.conn.rollback()
            raise
        else:
            self.conn.commit()


def run_default_migrations(conn: sqlite3.Connection) -> None:
    migrations = JoblinMigrationFinder().discover()
    migrator = Migrator(conn)
    migrator.run_migrations(migrations)
