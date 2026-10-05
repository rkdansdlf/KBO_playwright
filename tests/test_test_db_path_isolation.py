"""The test runtime database must be unique per invocation.

The xdist branch of the path resolution included the controller PID "so
concurrent xdist invocations cannot share a DB". The plain branch had no
discriminator at all, so two concurrent ``pytest`` runs both received
``data/test_runtime.db``.

Observed directly, two plain runs at the same time:

    [TESTDB] pid=57410 DATABASE_URL=sqlite:///.../data/test_runtime.db
    [TESTDB] pid=57416 DATABASE_URL=sqlite:///.../data/test_runtime.db

That does not visibly slow anything down, and the reason matters. The session
fixture unlinks the file at startup, so the second run orphans the first run's
database instead of locking against it -- the two runs diverge onto separate
inodes and never see each other. The isolation holds, but by accident of the
cleanup rather than by construction, and it stops holding the moment a run
survives long enough to be reopened by path.

The bare ``test_runtime.db`` name is pinned below so the discriminator cannot be
dropped again quietly.
"""

from __future__ import annotations

import os

import tests.conftest as conftest
from tests.conftest import _test_db_path


def test_the_resolved_path_carries_a_discriminator() -> None:
    """Direct guard: the shared name must not come back."""
    if conftest.TEST_DB_PATH is None:
        # A non-SQLite DATABASE_URL was supplied, so the SQLite path is unused.
        return
    assert conftest.TEST_DB_PATH.name != "test_runtime.db", "every concurrent plain pytest run would share one database"


def test_two_plain_runs_from_the_same_shell_do_not_share_a_database() -> None:
    """The case that collided: same parent shell, different pytest process."""
    parent = os.getppid()

    first = _test_db_path("", os.getpid(), parent)
    second = _test_db_path("", os.getpid() + 1, parent)

    assert first != second
    assert first.parent == second.parent


def test_xdist_workers_of_one_controller_stay_distinct() -> None:
    """The branch that already had a discriminator must keep it."""
    controller = os.getppid()

    assert _test_db_path("gw0", os.getpid(), controller) != _test_db_path("gw1", os.getpid(), controller)


def test_two_xdist_controllers_do_not_share_workers() -> None:
    """Different controllers, same worker name, still different databases."""
    mine = os.getpid()

    assert _test_db_path("gw0", mine, 1000) != _test_db_path("gw0", mine, 2000)


def test_the_path_stays_under_the_data_directory() -> None:
    """CI removes ``data/test_runtime*.db*`` before a run; keep that glob true."""
    path = _test_db_path("", os.getpid(), os.getppid())

    assert path.parent.name == "data"
    assert path.name.startswith("test_runtime")
    assert path.suffix == ".db"
