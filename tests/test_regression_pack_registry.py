"""The check registry is counted, so a duplicate id is not a cosmetic slip.

``run_regression_pack`` produces one result row per entry in ``_CHECKS``, and the
quality score deducts ``failure_count * 7.0`` from that count. A check defined
twice therefore fails twice and takes fourteen points off a run that only had
one thing wrong with it -- a number that cannot be traced back to any invariant,
because there is no second invariant.

The duplicate arrived the way these do: a merge brought in a check that was
already present. The two entries are byte-identical, so the count is the only
thing that notices.

These compare the registry against itself rather than against a hand-written
list, so a registry that grows is checked without the list being updated.
"""

from __future__ import annotations

from collections import Counter

from src.validators.data_quality_regression_pack import _CHECKS


class TestTheRegistryHasOneEntryPerCheck:
    def test_no_check_id_is_defined_twice(self) -> None:
        """Two entries with one id means one failure is reported -- and charged -- twice.

        The score deducts per failing check, so the second copy is not a duplicate
        row in a report. It is seven points removed from a run that had one
        problem, and no invariant in the repository corresponds to those points.
        """
        counts = Counter(check.check_id for check in _CHECKS)
        duplicated = {check_id: n for check_id, n in counts.items() if n > 1}

        assert not duplicated, f"these checks are defined more than once and are charged more than once: {duplicated}"

    def test_two_entries_sharing_an_id_would_be_indistinguishable_in_a_report(self) -> None:
        """Stated separately because the charge is the harm, not the duplication.

        A reader of a failure list sees one failing check and looks for the one
        thing that explains it. Two identical rows under one id give them nothing
        to find, which is what makes the extra deduction hard to argue with.
        """
        by_id: dict[str, list[str]] = {}
        for check in _CHECKS:
            by_id.setdefault(check.check_id, []).append(check.table)

        offenders = {check_id: tables for check_id, tables in by_id.items() if len(tables) > 1}

        assert not offenders, f"one id reporting more than one result row: {offenders}"


class TestTheCountIsOfChecks:
    def test_the_reported_count_is_the_number_of_distinct_checks(self) -> None:
        """``check_count`` is displayed beside ``failure_count`` in every summary.

        A reader comparing them assumes the same unit. Reporting 15 checks when
        there are 14 makes a failure rate computed from them wrong by a
        percentage point without anything looking wrong.
        """
        assert len(_CHECKS) == len({check.check_id for check in _CHECKS})

    def test_every_check_names_the_table_it_reads(self) -> None:
        """A check without a table cannot be skipped when the table is absent.

        Missing tables are how a fresh or partially migrated database differs from
        production, and the skip decision reads this field. An empty one leaves
        the check to raise instead of reporting "not measured".
        """
        unnamed = [check.check_id for check in _CHECKS if not check.table]

        assert not unnamed, f"checks with no table to scope them: {unnamed}"
