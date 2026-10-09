"""Typed-page-outcome vocabulary: three modules that must not drift apart.

`kbo_event_outcome`, `player_movement_outcome` and `team_history_outcome` exist
because a browser crawler cannot know what a page *meant* from an HTTP status.
All three map a page-level reason onto (taxonomy code, terminal?) so that an
unparseable document is never retried five times and a connection blip is never
written off as drift.

BH1 found the three modules disagreeing on shape:

* ``team_history`` and ``player_movement`` carry a 3-tuple ``(code, text, terminal)``
* ``kbo_event`` carries a 4-tuple with a fourth field named ``absence`` whose value
  is ``False`` in every entry and which no caller ever reads

That fourth field looks like a feature and is not one. The module docstring
argues at length that an empty standing page and an unreadable page "look
identical from outside" -- then names the distinguishing evidence and never reads
it. A field that is constant is not evidence, and leaving it in the table invites
the next reader to believe absence is being tracked when it is not.

These tests pin the agreement rather than the asymmetry: whichever way the
`absence` field is resolved, all three modules must expose the same two facts
per reason, and no module may carry a field that is constant across every entry.
"""

from __future__ import annotations

import pytest

from src.crawlers import kbo_event_outcome, player_movement_outcome, team_history_outcome

#: Every module that owns a page-level reason table, paired with the name it
#: gave its classifier. The names differ (`classify_page_failure` vs
#: `classify_history_failure`), which is itself part of the drift this module
#: records -- one call site cannot assume a common name.
OUTCOME_MODULES = (
    (kbo_event_outcome, "classify_page_failure"),
    (player_movement_outcome, "classify_page_failure"),
    (team_history_outcome, "classify_history_failure"),
)


def _reason_tables() -> list[tuple[object, dict]]:
    tables = []
    for module, _classifier_name in OUTCOME_MODULES:
        table = module._REASON_FAILURES
        assert table, f"{module.__name__} has an empty reason table"
        tables.append((module, table))
    return tables


def _constant_columns(table: dict) -> list[int]:
    """Return the indices of tuple columns that hold the same value everywhere.

    Index 0 (the code) and the terminal flag are expected to vary. Any *other*
    column that never varies is carrying no information.
    """
    width = len(next(iter(table.values())))
    columns: list[int] = []
    for index in range(width):
        values = {entry[index] for entry in table.values()}
        if len(values) == 1:
            columns.append(index)
    return columns


class TestTheThreeModulesAgreeOnShape:
    def test_every_reason_carries_a_code_and_a_terminal_flag(self) -> None:
        for module, table in _reason_tables():
            for reason, entry in table.items():
                assert entry[0] is not None, f"{module.__name__}[{reason}] has no code"
                assert isinstance(entry[-1], bool), (
                    f"{module.__name__}[{reason}] must end with a terminality flag, got {entry[-1]!r}"
                )

    def test_terminality_is_read_from_the_last_column(self) -> None:
        """``entry[-1]`` must be the terminality flag in every module.

        The cross-module tests above lean on ``entry[-1]``, so if a module ever
        pins terminality at some other index while a later column holds the flag,
        those tests would read the wrong field and quietly mean something else.
        """
        for module, table in _reason_tables():
            width = len(next(iter(table.values())))
            constant = _constant_columns(table)
            if len({entry[-1] for entry in table.values()}) == 1:
                assert width - 1 in constant, f"{module.__name__} pins terminality somewhere unexpected"

    def test_every_reason_in_a_table_is_reachable_from_its_module(self) -> None:
        """A module must recognise the reason keys it defines.

        The classifiers return UNKNOWN for an unrecognised reason, so a typo in
        a caller would degrade to UNKNOWN rather than raise. This is the cheap
        guard against that: every key is looked up through the public classifier
        and must come back as a real taxonomy code.
        """
        for module, classifier_name in OUTCOME_MODULES:
            classifier = getattr(module, classifier_name)
            for reason in module._REASON_FAILURES:
                code, terminal = classifier(reason)
                assert code != "UNKNOWN", f"{module.__name__} cannot classify its own reason {reason!r}"
                assert isinstance(terminal, bool)


class TestNoModuleCarriesAConstantColumn:
    """BUG-005: `kbo_event` carried an `absence` column that was always False.

    The other two modules were migrated to a 3-tuple; this one was not. Nothing
    read the field, so it was dead weight that documented an intention the code
    did not implement -- and the docstring actively argued the distinction
    matters. Fixed by dropping the column.

    The assertion is deliberately about *width* rather than about the absence
    field specifically: a fourth column of any kind would reintroduce the same
    gap between what the module appears to track and what it actually reads. The
    module's docstring still explains why an empty guide page and an unreadable
    one look identical from outside, because that reasoning is real and still
    decides between `EMPTY` and `SCHEMA_CHANGED`. It just no longer carries a
    column that says so without meaning it.
    """

    def test_kbo_event_has_no_dead_fourth_column(self) -> None:
        table = kbo_event_outcome._REASON_FAILURES
        width = len(next(iter(table.values())))

        assert width == 3, (
            f"BUG-005: kbo_event_outcome._REASON_FAILURES has a {width}-tuple, but the other two "
            "outcome modules use 3. A constant column is not evidence: it makes the module read as "
            "though it tracks a distinction it discards at the point of use."
        )

    def test_every_entry_has_the_declared_width(self) -> None:
        """A table can be 3-wide overall and still carry one ragged entry."""
        widths = {len(entry) for entry in kbo_event_outcome._REASON_FAILURES.values()}

        assert widths == {3}, widths

    @pytest.mark.parametrize(
        ("module", "_classifier_name"),
        OUTCOME_MODULES,
        ids=[m.__name__.rsplit(".", 1)[-1] for m, _ in OUTCOME_MODULES],
    )
    def test_no_module_carries_an_all_constant_column(self, module, _classifier_name) -> None:
        """A column constant across every entry records nothing.

        Terminality is allowed to be constant (a module may decide every reason
        it knows is terminal); the code column is checked separately. This
        catches a *newly added* informational column that was filled with a
        placeholder.
        """
        table = module._REASON_FAILURES
        width = len(next(iter(table.values())))
        constant = set(_constant_columns(table))

        dead = {index for index in constant if index not in {0, width - 1}}
        assert not dead, (
            f"{module.__name__}._REASON_FAILURES has column(s) {sorted(dead)} that hold one value "
            "across every reason, so they carry no information."
        )
