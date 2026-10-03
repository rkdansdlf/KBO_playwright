"""Value types shared by the per-game run ledgers.

Game detail and relay both record one run per game, opened before the fetch and
closed after the write. Only the value types are shared: each ledger owns its own
transitions and its own state machine, because what counts as a finished relay and
what counts as a finished box score are different questions and sharing those
decisions would be how one crawler's terminal starts meaning another's.

Deliberately not a framework. There is no base class, no registry and no hook:
these are frozen dataclasses, so a ledger that forgets one is a type error rather
than a silent behaviour change.
"""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field as dataclasses_field


@dataclass(frozen=True)
class RunCounts:
    """Rows moved by one game, as the run ledger records them.

    `written` counts rows actually stored, which includes a degraded partial:
    the payload was persisted. A "full detail" flag must not be used here,
    because it is false for exactly the partial case that did write.
    """

    read: int = 0
    written: int = 0
    failed: int = 0


#: Shared empty counts, so a default is not a fresh object per call.
_NO_ROWS = RunCounts()


@dataclass(frozen=True)
class TerminalOutcome:
    """What became of one game after its fetch and its write.

    Returned only when the ledger transition actually committed. A caller must
    not treat a run it could not record as finished, and must not report a
    success the ledger never accepted.

    For a replay, the persisted run row is still the authority: this is how the
    collection service reports back to its own caller, not a substitute for
    reading the run back.
    """

    status: str
    error_code: str | None = None
    error_message: str | None = None
    counts: RunCounts = _NO_ROWS
    run_id: str | None = None


@dataclass(frozen=True)
class RunOpenResult:
    """Which runs could be started, and why the rest could not.

    Failures are reported rather than absorbed because an unopened run is not a
    bookkeeping detail: it is the one case where a game's data would otherwise be
    written with nothing to attribute it to. The caller decides what to do with a
    game that got no run, and it cannot do that if the answer was discarded here.
    """

    started: dict[str, str] = dataclasses_field(default_factory=dict)
    failures: dict[str, tuple[str, str]] = dataclasses_field(default_factory=dict)

    def run_id_for(self, game_id: str) -> str | None:
        """Return the run started for a game, or None when it got none."""
        return self.started.get(game_id)

    def failure_for(self, game_id: str) -> tuple[str, str] | None:
        """Return the classified ``(code, message)`` for a game that got no run."""
        return self.failures.get(game_id)
