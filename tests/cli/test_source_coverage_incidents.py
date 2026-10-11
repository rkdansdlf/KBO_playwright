"""Tests for the blocked-source incident.

The defect this exists to catch is a pair of individually-correct facts: a policy
refusal is not an error, and a run that collected nothing is still `success`.
Together they left 82 `source_limited` runs and one `PARSE_EMPTY` run with no
incident anywhere -- a queue that stays empty while the tables stop moving.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.cli.reports.source_coverage_incidents import (
    INCIDENT_KEY,
    build_source_coverage_incidents,
    incident_crawler_names,
)
from src.reporting.source_inventory import (
    Freshness,
    PolicyStatus,
    RunSummary,
    SourceInventory,
    SourceRow,
    TableMeasurement,
)

_KST = timezone(timedelta(hours=9))


def _measurement(table: str, *, age_days: int, freshness: Freshness = Freshness.STALE) -> TableMeasurement:
    return TableMeasurement(
        table=table,
        column="updated_at",
        rows=10,
        last_updated_at=datetime.now(_KST) - timedelta(days=age_days),
        age_days=age_days,
        freshness=freshness,
    )


def _run(*, succeeded: int, zero_yield: int) -> RunSummary:
    return RunSummary(
        crawler="x",
        window_hours=72,
        total=succeeded,
        succeeded=succeeded,
        zero_yield=zero_yield,
        latest_at=datetime.now(_KST),
    )


def _row(
    crawler: str,
    *,
    policy: PolicyStatus = PolicyStatus.BLOCKED,
    measurements: tuple[TableMeasurement, ...] = (),
    runs: RunSummary | None = None,
) -> SourceRow:
    return SourceRow(
        crawler=crawler,
        source_domains=("www.koreabaseball.com",),
        policy_status=policy,
        measurements=measurements,
        runs=runs,
    )


class TestBlockedAndSilentOpensTheIncident:
    def test_a_blocked_crawler_with_a_stale_table_opens_it(self) -> None:
        inventory = SourceInventory(
            rows=(_row("preview_crawler", measurements=(_measurement("game_summary", age_days=56),)),)
        )

        events, resolve_keys = build_source_coverage_incidents(inventory)

        assert resolve_keys == []
        assert len(events) == 1
        assert events[0].incident_key == INCIDENT_KEY
        assert incident_crawler_names(events) == ("preview_crawler",)
        assert "game_summary 56d" in events[0].message

    def test_success_without_a_yield_is_enough_on_its_own(self) -> None:
        """A table with no age column still gives itself away through the ledger."""
        inventory = SourceInventory(rows=(_row("preview_crawler", runs=_run(succeeded=20, zero_yield=20)),))

        events, _ = build_source_coverage_incidents(inventory)

        assert len(events) == 1
        assert incident_crawler_names(events) == ("preview_crawler",)

    def test_the_whole_set_travels_in_one_incident(self) -> None:
        """Twenty-four crawlers take the same host down at once.

        One notification, because a channel that fires twenty-four times for a
        single policy change is a channel people stop reading.
        """
        inventory = SourceInventory(
            rows=(
                _row("a_crawler", measurements=(_measurement("t1", age_days=56),)),
                _row("b_crawler", measurements=(_measurement("t2", age_days=111),)),
                _row("c_crawler", runs=_run(succeeded=5, zero_yield=5)),
            ),
        )

        events, _ = build_source_coverage_incidents(inventory)

        assert len(events) == 1
        assert incident_crawler_names(events) == ("a_crawler", "b_crawler", "c_crawler")
        assert events[0].metadata["blocked_total"] == 3


class TestTheIncidentClosesOnlyWhenTheDataMoves:
    def test_a_blocked_crawler_that_still_collects_resolves_it(self) -> None:
        """Blocked is not itself a fault; a blocked crawler still filling tables is fine."""
        inventory = SourceInventory(
            rows=(
                _row(
                    "pbp_crawler",
                    measurements=(_measurement("game_play_by_play", age_days=0, freshness=Freshness.CURRENT),),
                ),
            ),
        )

        events, resolve_keys = build_source_coverage_incidents(inventory)

        assert events == []
        assert resolve_keys == [INCIDENT_KEY]

    def test_partial_recovery_keeps_it_open(self) -> None:
        """One crawler still stale is one crawler still not collecting.

        Closing on a fraction would be the quiet version of the same bug: the
        number next to the key falls, the operator reads it as progress, and the
        remaining gap needs a second notice to survive.
        """
        inventory = SourceInventory(
            rows=(
                _row("silent_crawler", measurements=(_measurement("t1", age_days=56),)),
                _row(
                    "healthy_crawler",
                    measurements=(_measurement("t2", age_days=1, freshness=Freshness.CURRENT),),
                ),
            ),
        )

        events, resolve_keys = build_source_coverage_incidents(inventory)

        assert resolve_keys == []
        assert len(events) == 1
        assert incident_crawler_names(events) == ("silent_crawler",)
        assert events[0].metadata["blocked_total"] == 2

    def test_nothing_blocked_and_nothing_silent_is_not_a_state(self) -> None:
        """With no blocked crawler there is no decision pending, so nothing resolves."""
        inventory = SourceInventory(
            rows=(
                _row(
                    "parking_crawler",
                    policy=PolicyStatus.UNKNOWN,
                    measurements=(_measurement("parking_lots", age_days=56),),
                ),
            ),
        )

        events, resolve_keys = build_source_coverage_incidents(inventory)

        assert events == []
        assert resolve_keys == []


class TestUnknowingThePolicyIsNotBeingBlocked:
    def test_an_unrecorded_policy_is_reported_somewhere_else(self) -> None:
        """``parking`` and ``seat`` are stale without asking any policy.

        Folding them in here would say the robots policy stopped them, which is
        a claim this code cannot support -- and would bury them under the real
        blocked ones in the same message.
        """
        inventory = SourceInventory(
            rows=(
                _row(
                    "parking_crawler",
                    policy=PolicyStatus.UNKNOWN,
                    measurements=(_measurement("parking_lots", age_days=56),),
                ),
            ),
        )

        events, resolve_keys = build_source_coverage_incidents(inventory)

        assert events == []
        assert resolve_keys == []

    def test_an_allowed_crawler_that_is_stale_is_not_a_policy_incident(self) -> None:
        inventory = SourceInventory(
            rows=(
                _row(
                    "game_detail_crawler",
                    policy=PolicyStatus.OK,
                    measurements=(_measurement("game_summary", age_days=90),),
                ),
            ),
        )

        events, resolve_keys = build_source_coverage_incidents(inventory)

        assert events == []
        assert resolve_keys == []
