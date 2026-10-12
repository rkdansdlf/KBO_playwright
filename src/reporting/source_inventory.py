"""What each crawler reads, what it writes, and whether that data is current.

Three questions this exists to answer, none of which any single existing report
could:

* **Which data is actually old?** A crawler succeeding and a table being current
  are different facts. `preview` ran 459 times and collected nothing for eight
  weeks, and the freshness gauges stayed green throughout (BUG-014). This reads
  the tables instead of the run outcomes.
* **Which service is reading the old data?** Measured, not asserted: the
  ``rag_chunks`` table records the ``source_table`` every chunk came from, so the
  AI-visible surface is a query rather than a guess.
* **What should be repaired first?** The columns that need judgement are declared
  once and verified against reality, so a wrong declaration fails a test instead
  of quietly reporting the wrong table's freshness.

Two design choices are deliberate and were arrived at by trying the alternative.

**Crawler-to-table is declared, not derived.** Resolving it through repository
imports looked promising and is not reliable: ``roster_transaction_repository``
imports several models and the first one resolves to ``player_basic``. A wrong
mapping reports the freshness of a table nobody asked about, which is worse than
reporting nothing. Every declaration is checked against
``information_schema`` so a typo is a test failure.

**Coverage is reported as a gap rather than implied.** Not every crawler is
declared yet. The undeclared ones are listed in the report, because an inventory
that quietly omits a third of its subjects reads as complete.
"""

from __future__ import annotations

import ast
import logging
from dataclasses import dataclass
from enum import StrEnum
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING

from src.utils.host_policy import HostVerdict, read_policy

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence
    from datetime import datetime

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

CRAWLER_DIR = Path("src/crawlers")
SOURCE_DIR = Path("src")
ROBOTS_DIR = Path("docs/robots")
SNAPSHOT_HEADER_LINES = 3

#: How many undeclared crawlers an advisory names before summarising.
ADVISORY_NAME_LIMIT = 8

#: The host the compliance layer is scoped to.
#:
#: Named rather than derived: `_is_same_site` in the compliance module is the
#: authority, and a report that worked it out a second time could disagree with
#: the crawlers about whether a URL was even subject to the policy.
KBO_HOST = "www.koreabaseball.com"

#: Modules in the crawler directory that are not crawlers.
#:
#: Bases, legacy copies and the vocabulary/shared modules. Including them would
#: pad the report with rows that can never collect anything; excluding them is
#: safe because the declared design names what is left.
NON_CRAWLER_PREFIXES = ("base_", "legacy_")


@dataclass(frozen=True)
class Cadence:
    """What a scheduled job promises, and how late it may reasonably run."""

    job_id: str
    summary: str
    period_days: int

    @property
    def max_age_days(self) -> int:
        """Return the age at which a table stops being plausibly current.

        Three missed cycles, floored at two days. One missed cycle is a retry; two
        can be a bad afternoon; three is a pattern. The floor matters for the
        intraday jobs, where ``period_days`` is 1 and a two-day allowance would
        let a daily crawler sit silent for a weekend unnoticed.
        """
        return max(2, self.period_days * 3)


def _fields_of(trigger: object) -> dict[str, str]:
    """Read a cron trigger's non-wildcard fields, as strings."""
    fields: dict[str, str] = {}
    for field in getattr(trigger, "fields", ()):  # APScheduler BaseTrigger
        value = str(field)
        if value != "*":
            fields[field.name] = value
    return fields


def _cadence_from(trigger: object, job_id: str) -> Cadence:
    """Describe how often a trigger fires, in the coarsest unit that fits.

    Coarse on purpose: the inventory compares weeks and months, so an hour field
    only has to distinguish "daily" from "not daily".
    """
    fields = _fields_of(trigger)
    hour = fields.get("hour", "0")
    minute = fields.get("minute", "0")
    if "day" in fields:
        return Cadence(job_id, f"monthly (day {fields['day']} {hour}:{minute} KST)", 31)
    if "day_of_week" in fields:
        return Cadence(job_id, f"weekly ({fields['day_of_week']} {hour}:{minute} KST)", 7)
    if "hour" in fields:
        return Cadence(job_id, f"daily ({hour}:{minute} KST)", 1)
    return Cadence(job_id, "intraday", 1)


def scheduled_cadence() -> dict[str, Cadence]:
    """Return every scheduled job's cadence, keyed by job id.

    Read from the registry's own job list rather than restated here: a cadence
    copied into a report is a cadence that goes stale the next time the schedule
    moves, and this report exists because stale contracts are expensive.
    """
    from apscheduler.triggers.cron import CronTrigger

    from src.scheduler.registry import job_specs

    return {job_id: _cadence_from(trigger, job_id) for _fn, trigger, job_id, _name, _grace in job_specs(CronTrigger)}


class PolicyStatus(StrEnum):
    """Whether the crawler's source may be consulted at all."""

    OK = "ok"
    """The source is allowed, or the crawler does not use a controlled host."""

    BLOCKED = "blocked"
    """The source's robots policy refuses generic agents."""

    UNKNOWN = "unknown"
    """The crawler does not consult the policy, so no verdict can be given."""


class Freshness(StrEnum):
    """How the measured age compares against what the crawler promises."""

    CURRENT = "current"
    STALE = "stale"
    EMPTY = "empty"
    """The table exists and holds no rows, so it has no age to judge."""

    UNKNOWN = "unknown"
    """No timestamp column could be read, so nothing was compared."""


@dataclass(frozen=True)
class SourceDeclaration:
    """The curated half of a row: what the code cannot be trusted to tell us.

    Kept small on purpose. Each field is either verified by a test or reported as
    a gap, so the declaration cannot silently rot into fiction.
    """

    tables: tuple[str, ...]
    repository: str | None = None
    """The repository module that persists these tables, used to find readers."""

    freshness_column: str | None = None
    """Which column means "current" for this table.

    ``None`` falls back to ``updated_at`` then ``created_at``. A domain date is
    sometimes the honest answer instead -- ``roster_transactions`` is fresh
    because of when the transaction happened, not when the row was written.
    """

    ledger_name: str | None = None
    """The crawler's name in the run ledger, when it differs from its module.

    The ledger and the filesystem use different vocabularies -- the ledger writes
    ``roster_transactions`` where the module is ``roster_transaction_crawler`` --
    so the default is a guess and this is its correction. Guessing was tried
    first and matched seven of the eight crawlers observed; the eighth needed
    this field, and without it the run measurement silently attached to nothing.
    """

    job: str | None = None
    """The scheduler job that runs this crawler, for its expected cadence.

    Declared rather than derived. Jobs delegate to CLI entrypoints which call the
    crawlers, so the chain is two hops deep (``crawl_p0_non_game`` ->
    ``crawl_p0_data`` -> ``RosterTransactionCrawler``) and a name match against
    the job bodies finds nothing. ``None`` means the crawler runs on demand, and
    its tables are then judged by the default threshold rather than a cadence.
    """

    expected_max_age_days: int | None = None
    """For data with no scheduled job, how old it may be before it is a problem.

    Declared because the job cadence cannot answer it. Awards are announced once
    a season, so the 2026 list legitimately does not exist yet and a 54-day-old
    table is not stale -- but a flat threshold calls it stale, and a report that
    flags a healthy table teaches its reader to ignore the column. Taken in
    preference to the job cadence when set.
    """

    alternative_source: str | None = None
    """Where else the data could come from. ``None`` means nobody has looked."""

    priority: str = "unset"


#: The honest value while the permission review (6단계) has not happened.
#:
#: A specific string rather than an empty field so the report can count how many
#: alternatives are actually known, and so a reader can tell "nobody has looked"
#: from "someone looked and there is nowhere else".
PENDING_PERMISSION_REVIEW = "pending: permission review (6단계)"

#: Crawlers whose data reaches answers, plus every crawler a robots policy
#: currently refuses. The rest are reported as undeclared.
#:
#: Deliberately partial. Declaring fifty-three rows from inspection would invent
#: most of them; the report names the gap instead, and each pass fills a few more.
DECLARED: dict[str, SourceDeclaration] = {
    "award_crawler": SourceDeclaration(
        tables=("awards",),
        expected_max_age_days=400,
        repository="award_repository",
        alternative_source="Wikipedia and yagoonara; not a KBO-controlled host",
        priority="normal",
    ),
    "game_detail_crawler": SourceDeclaration(
        job="crawl_daily_games",
        tables=("game_batting_stats", "game_pitching_stats", "game_summary", "game_metadata"),
        alternative_source="Naver relay API (already the primary path)",
        priority="normal",
    ),
    "kbo_event_crawler": SourceDeclaration(
        tables=("team_events",),
        freshness_column="updated_at",
        alternative_source="none identified; KBO publishes these pages only",
        priority="high",
    ),
    "player_movement_crawler": SourceDeclaration(
        tables=("player_movements",),
        repository="player_movement_repository",
        alternative_source="none identified; KBO Player/Trade.aspx only",
        priority="high",
    ),
    "player_profile_crawler": SourceDeclaration(
        tables=("player_basic",),
        alternative_source="pending: permission review (6단계)",
        priority="high",
    ),
    "press_release_crawler": SourceDeclaration(
        job="crawl_kbo_press_releases",
        tables=("kbo_press_releases",),
        alternative_source="pending: permission review (6단계)",
        priority="normal",
    ),
    "preview_crawler": SourceDeclaration(
        tables=(),
        alternative_source="none identified; the preview batch owns the write",
        priority="high",
    ),
    "roster_transaction_crawler": SourceDeclaration(
        ledger_name="roster_transactions",
        job="crawl_p0_non_game",
        tables=("roster_transactions",),
        repository="roster_transaction_repository",
        freshness_column="transaction_date",
        alternative_source="none identified; KBO roster page only",
        priority="high",
    ),
    "schedule_crawler": SourceDeclaration(
        job="crawl_daily_games",
        tables=("game",),
        freshness_column="game_date",
        repository="game_repository",
        alternative_source="Naver schedule API (already the primary path)",
        priority="normal",
    ),
    "team_history_crawler": SourceDeclaration(
        tables=("team_history",),
        repository="team_history_repository",
        alternative_source="pending: permission review (6단계)",
        priority="high",
    ),
    "ticket_crawler": SourceDeclaration(
        tables=("ticket_prices", "ticket_open_rules"),
        expected_max_age_days=400,
        alternative_source="pending: permission review (6단계)",
        priority="normal",
    ),
    "broadcast_crawler": SourceDeclaration(
        tables=("game_broadcasts",),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
    "congestion_crawler": SourceDeclaration(
        tables=("stadium_congestion",),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
    "fan_culture_crawler": SourceDeclaration(
        tables=(
            "cheer_chants",
            "cheer_songs",
            "team_rivalries",
        ),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
    "foreign_player_crawler": SourceDeclaration(
        tables=("foreign_player_changes",),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
    "game_mvp_crawler": SourceDeclaration(
        tables=("game_mvps",),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
    "injury_crawler": SourceDeclaration(
        tables=("injury_entries",),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
    "manager_change_crawler": SourceDeclaration(
        tables=("manager_changes",),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
    "operation_notice_doosan_crawler": SourceDeclaration(
        tables=("stadium_operation_notices",),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
    "operation_notice_lg_crawler": SourceDeclaration(
        tables=("stadium_operation_notices",),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
    "operation_notice_naver_crawler": SourceDeclaration(
        tables=("stadium_operation_notices",),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
    "parking_crawler": SourceDeclaration(
        tables=(
            "parking_lots",
            "parking_fee_rules",
            "parking_fee_kinds",
        ),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
    "player_search_crawler": SourceDeclaration(
        tables=("player_basic",),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
    "seat_crawler": SourceDeclaration(
        tables=("stadium_seat_sections",),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
    "staff_register_crawler": SourceDeclaration(
        tables=("player_basic",),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
    "team_event_crawler": SourceDeclaration(
        tables=("team_events",),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
    "transit_time_crawler": SourceDeclaration(
        tables=("stadium_transit_times",),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
    "team_batting_stats_crawler": SourceDeclaration(
        tables=("team_season_batting",),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
    "team_pitching_stats_crawler": SourceDeclaration(
        tables=("team_season_pitching",),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
    "fielding_stats_crawler": SourceDeclaration(
        tables=("player_season_fielding",),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
    "player_pitching_all_series_crawler": SourceDeclaration(
        tables=("player_season_pitching",),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
    "futures_schedule_crawler": SourceDeclaration(
        tables=("futures_game_schedules",),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
    "daily_roster_crawler": SourceDeclaration(
        tables=("team_daily_roster",),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
    "draft_history_crawler": SourceDeclaration(
        tables=("player_draft_histories",),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
    "milestone_crawler": SourceDeclaration(
        tables=("player_milestones",),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
    "pbp_crawler": SourceDeclaration(
        tables=("game_play_by_play",),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
    "baserunning_stats_crawler": SourceDeclaration(
        tables=("player_season_baserunning",),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
    "draft_wiki_crawler": SourceDeclaration(
        tables=("player_draft_histories",),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
    "dynamic_data_crawler": SourceDeclaration(
        tables=("ticket_schedules",),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
    "external_stats_crawler": SourceDeclaration(
        tables=("external_season_stats",),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
    "food_crawler": SourceDeclaration(
        tables=("stadium_food_menu_items", "stadium_food_vendors"),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
    "historical_season_crawler": SourceDeclaration(
        tables=("game",),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
    "naver_relay_crawler": SourceDeclaration(
        tables=("game_events",),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
    "player_batting_all_series_crawler": SourceDeclaration(
        tables=("player_season_batting",),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
    "player_list_crawler": SourceDeclaration(
        tables=("player_basic",),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
    "player_splits_crawler": SourceDeclaration(
        tables=("player_splits_stats",),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
    "relay_crawler": SourceDeclaration(
        tables=("game_events",),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
    "team_info_crawler": SourceDeclaration(
        tables=("team_franchises",),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
    "realtime_issue_crawler": SourceDeclaration(
        tables=(),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
    "static_text_crawler": SourceDeclaration(
        tables=(),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
    "text_relay_crawler": SourceDeclaration(
        tables=(),
        alternative_source=PENDING_PERMISSION_REVIEW,
    ),
}


#: How far back to read the run ledger when judging whether a crawler yields.
#:
#: Three days rather than a day: a daily crawler that yields on the seventh day
#: would look broken in a 24-hour window and healthy in a year, and neither
#: window can tell those apart. Three days also spans a weekend, which is when a
#: genuinely absent collection is most likely to be explained away.
RUN_WINDOW_HOURS = 72


@dataclass(frozen=True)
class RunSummary:
    """What a crawler's recent runs actually did, as opposed to reported.

    The ledger distinguishes a run that failed from a run that succeeded. It does
    not distinguish a run that succeeded *and collected something* from one that
    succeeded having fetched nothing at all, which is the difference between a
    crawler working and a crawler blocked by a robots policy while reporting
    `success`.
    """

    crawler: str
    window_hours: int
    total: int
    succeeded: int
    zero_yield: int
    latest_at: datetime | None = None
    policy_blocked: int = 0
    """Runs whose outcome was a refusal rather than an unreachable host."""

    @property
    def yields(self) -> bool:
        """Return whether any success in the window actually collected data.

        False means every reported success handed back nothing. One such run can be
        an off-day; a window of them is the signature of a source that is not
        being consulted.

        A crawler with no successes at all does not count. Every run failing is
        already visible in the ledger, and folding it in here would report a
        loud, already-known failure as if it were the quiet one this section
        exists to find.
        """
        return self.succeeded == 0 or self.zero_yield < self.succeeded


class CollectionState(StrEnum):
    """What the run ledger and the table together say about a crawler.

    Neither signal can answer alone, which is the whole reason this exists. A run
    that succeeded having collected nothing and a table whose contents have not
    changed in two months both look fine on their own; together they mean the
    collection has stopped, and the opposite pair means the opposite thing. An
    age-based verdict reads "old" as "broken", which called a crawler that had
    written three records every day for four days a table that had not changed
    since August.
    """

    COLLECTING = "collecting"
    """Recent runs collected data and the tables are within their threshold."""

    STABLE = "stable"
    """Recent runs collected data; the tables have not changed past the threshold.

    Not a defect. This is what a working collector looks like when the source
    stops publishing, and calling it stale is the false positive that teaches a
    reader to ignore the column.
    """

    NOT_COLLECTED = "not_collected"
    """Every reported success collected nothing.

    The quiet failure: the run did what it was asked and handed back no data.
    """

    FAILING = "failing"
    """Runs exist and none succeeded. Loud, and already visible in the ledger."""

    UNEPLAINED = "unexplained"
    """The tables are old and no run in the window can corroborate either way.

    Most crawlers are weekly or on demand and simply did not run. Naming this
    state is what keeps it from being read as either of the two above.
    """

    EMPTY = "empty"
    """The tables exist and hold nothing."""

    UNKNOWN = "unknown"
    """Nothing was measured."""


#: Run outcomes that mean a source was refused rather than unreachable.
#:
#: Named here because the disagreement between this module's own verdict and the
#: ledger is worth reporting: the inventory judges the hosts it can see in the
#: source, and a runtime-assembled URL is invisible to it, so `ok` and
#: `FETCH_BLOCKED` can coexist. Reporting the disagreement is honest; deciding
#: which side is right without evidence is not.
POLICY_OUTCOME_CODES = frozenset({"FETCH_BLOCKED"})


@dataclass(frozen=True)
class TableMeasurement:
    """One table's real state, read from the database."""

    table: str
    column: str | None
    rows: int
    last_updated_at: datetime | None
    age_days: int | None
    freshness: Freshness

    def render_age(self) -> str:
        """Return the age for a human, or why there is none."""
        if self.freshness is Freshness.EMPTY:
            return "empty"
        if self.age_days is None:
            return "unknown"
        return f"{self.age_days}d"


@dataclass(frozen=True)
class SourceRow:
    """One crawler, its source, and what that source feeds."""

    crawler: str
    source_domains: tuple[str, ...]
    policy_status: PolicyStatus
    enforced: bool = False
    """Whether the crawler asks the compliance layer before fetching.

    Separate from the status because the two answer different questions. A host
    that permits reading while the crawler never asks is the state that turns a
    policy change into a surprise.
    """

    declaration: SourceDeclaration | None = None
    measurements: tuple[TableMeasurement, ...] = ()
    rag_chunks: int = 0
    consumers: tuple[str, ...] = ()
    runs: RunSummary | None = None

    @property
    def stale(self) -> bool:
        """Return whether any measured table is past its threshold."""
        return any(m.freshness is Freshness.STALE for m in self.measurements)

    @property
    def collection_state(self) -> CollectionState:
        """Return what the ledger and the tables together say.

        A property rather than a stored field so it cannot disagree with the
        measurements it is derived from -- the previous age-only verdict was
        wrong for exactly the crawlers whose runs contradicted their table.

        Returns:
            The joint verdict.

        """
        runs = self.runs
        if runs is not None and runs.total:
            if runs.succeeded == 0:
                return CollectionState.FAILING
            if not runs.yields:
                # Zero-yield successes, stale tables or not: the source was
                # consulted and produced nothing, and that is the finding whether
                # or not a table happens to corroborate it.
                return CollectionState.NOT_COLLECTED
            return CollectionState.STABLE if self.stale else CollectionState.COLLECTING
        if self.stale:
            return CollectionState.UNEPLAINED
        if any(m.freshness is Freshness.EMPTY for m in self.measurements):
            return CollectionState.EMPTY
        return CollectionState.UNKNOWN

    @property
    def declared(self) -> bool:
        """Return whether anyone has recorded what this crawler writes."""
        return self.declaration is not None

    @property
    def stalest(self) -> TableMeasurement | None:
        """Return the oldest measured table, which is what a reader looks for."""
        aged = [m for m in self.measurements if m.age_days is not None]
        return max(aged, key=lambda m: m.age_days or 0, default=None)

    def to_dict(self) -> dict[str, object]:
        """Return the row as a plain mapping for JSON output."""
        return {
            "crawler": self.crawler,
            "source_domains": list(self.source_domains),
            "policy_status": self.policy_status.value,
            "enforced": self.enforced,
            "declared": self.declared,
            "alternative_source": (self.declaration.alternative_source if self.declaration else None),
            "priority": (self.declaration.priority if self.declaration else None),
            "rag_chunks": self.rag_chunks,
            "consumers": list(self.consumers),
            "runs": (
                {
                    "window_hours": self.runs.window_hours,
                    "total": self.runs.total,
                    "succeeded": self.runs.succeeded,
                    "zero_yield": self.runs.zero_yield,
                    "latest_at": (self.runs.latest_at.isoformat() if self.runs.latest_at else None),
                }
                if self.runs
                else None
            ),
            "tables": [
                {
                    "table": m.table,
                    "column": m.column,
                    "rows": m.rows,
                    "age_days": m.age_days,
                    "freshness": m.freshness.value,
                    "last_updated_at": (m.last_updated_at.isoformat() if m.last_updated_at else None),
                }
                for m in self.measurements
            ],
        }


@dataclass(frozen=True)
class SourceInventory:
    """The report: rows, the gaps in it, and what deserves attention."""

    rows: tuple[SourceRow, ...]
    drift: tuple[str, ...] = ()
    advisories: tuple[str, ...] = ()
    generated_at: datetime | None = None

    def rows_by_crawler(self) -> dict[str, SourceRow]:
        """Index the rows by crawler name."""
        return {row.crawler: row for row in self.rows}

    def by_state(self, state: CollectionState) -> tuple[SourceRow, ...]:
        """Return the rows in one collection state.

        Args:
            state: The state to select.

        Returns:
            The matching rows, in report order.

        """
        return tuple(row for row in self.rows if row.collection_state is state)

    def not_collected(self) -> tuple[SourceRow, ...]:
        """Return crawlers whose successes collected nothing.

        The only finding here that is both silent in the ledger and actionable
        without a judgement call about what the data should look like.
        """
        return self.by_state(CollectionState.NOT_COLLECTED)

    def policy_disagreements(self) -> tuple[SourceRow, ...]:
        """Return crawlers this module calls permitted but the ledger calls refused.

        The two come from different places: the verdict is read off the hosts
        visible in the crawler source, the outcome was recorded at fetch time. A
        runtime-assembled URL is invisible to the first, so both can be true at
        once. Which one is wrong cannot be settled from here, so the
        disagreement is reported instead of resolved by preferring one.
        """
        return tuple(
            row
            for row in self.rows
            if row.policy_status is PolicyStatus.OK and row.runs is not None and row.runs.policy_blocked
        )

    def succeeding_and_stale(self) -> tuple[SourceRow, ...]:
        """Return crawlers reporting success while their tables go empty.

        The two facts are collected from different places and neither contradicts
        the other on its own. A crawler that succeeded did what it was asked; a
        table that is old was not updated. Put together they mean the run is not
        evidence of collection at all -- which is how a blocked source stays
        invisible: every run is `success`, and nothing in the ledger is wrong.
        """
        return tuple(
            row
            for row in self.rows
            if row.runs is not None
            and not row.runs.yields
            and any(m.freshness is Freshness.STALE for m in row.measurements)
        )

    def zero_yield_runs(self) -> tuple[SourceRow, ...]:
        """Return crawlers whose every recent success collected nothing.

        Broader than :meth:`succeeding_and_stale`, which needs a stale table as
        corroboration. A pure reader has no table of its own -- the preview
        crawler is the clearest case, and it is the one that read nothing fifty
        times in a row -- so for those the ledger is the only evidence there is,
        and withholding it would hide the most alarming case in the report.
        """
        return tuple(row for row in self.rows if row.runs is not None and not row.runs.yields)

    def ai_exposure(self) -> tuple[SourceRow, ...]:
        """Return rows that feed answers *and* are not current.

        The reason this report exists. A stale table is a data problem; a stale
        table the assistant quotes from is a correctness problem, and the second
        is what an operator cannot see anywhere else.
        """
        return tuple(
            row
            for row in self.rows
            if row.rag_chunks > 0
            and row.collection_state in (CollectionState.NOT_COLLECTED, CollectionState.UNEPLAINED)
        )


def discover_crawler_modules(*, crawler_dir: Path = CRAWLER_DIR) -> tuple[str, ...]:
    """Return the crawler modules worth reporting on.

    Excludes the bases and the legacy copies: they are not crawl units and would
    pad the report with rows that can never collect anything.
    """
    names = sorted(path.stem for path in crawler_dir.glob("*_crawler.py"))
    return tuple(name for name in names if not name.startswith(NON_CRAWLER_PREFIXES))


@cache
def _crawler_tree(path: Path) -> ast.Module:
    """Parse a crawler module once.

    `source_domains` and `policy_status_of` both need it, and the files run to a
    few thousand lines each -- parsing every one twice was measurable.
    """
    return ast.parse(path.read_text(encoding="utf-8"))


def source_domains(module: str, *, crawler_dir: Path = CRAWLER_DIR) -> tuple[str, ...]:
    """Return the hosts a crawler's source mentions.

    Args:
        module: Crawler module stem, without ``.py``.
        crawler_dir: Directory holding the crawler modules.

    Returns:
        Sorted, de-duplicated host names.

    """
    path = crawler_dir / f"{module}.py"
    if not path.exists():
        return ()
    hosts: set[str] = set()
    for node in ast.walk(_crawler_tree(path)):
        # Only string literals: a host assembled at runtime is not a claim this
        # report can check against a robots policy, and guessing it would be the
        # unreliable derivation the module docstring rejects.
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            for piece in node.value.split():
                if piece.startswith(("http://", "https://")):
                    hosts.add(piece.split("//", 1)[1].split("/", 1)[0])
    return tuple(sorted(hosts))


def consults_policy(module: str, *, crawler_dir: Path = CRAWLER_DIR) -> bool:
    """Return whether a crawler asks the compliance policy before fetching."""
    path = crawler_dir / f"{module}.py"
    if not path.exists():
        return False
    source = path.read_text(encoding="utf-8")
    return "compliance.is_allowed" in source or "is_allowed_sync" in source


@cache
def host_verdicts(*, policy_dir: Path = ROBOTS_DIR) -> dict[str, HostVerdict]:
    """Return the recorded policy verdict for every host the crawlers use.

    Cached because `policy_status_of` is called once per crawler and each call
    otherwise re-reads the snapshot files; the archive holds thousands of them.

    Args:
        policy_dir: Where the per-host snapshots live.

    Returns:
        Host to verdict. Hosts with no record are absent rather than permissive.

    """
    verdicts: dict[str, HostVerdict] = {}
    for module in discover_crawler_modules():
        for domain in source_domains(module):
            if domain in verdicts:
                continue
            verdicts[domain] = read_policy(domain, policy_dir=policy_dir).verdict
    return verdicts


def policy_status_of(
    module: str,
    verdicts: Mapping[str, HostVerdict],
    *,
    crawler_dir: Path = CRAWLER_DIR,
) -> PolicyStatus:
    """Return what a crawler's sources permit.

    Judges the host's recorded policy, not whether the crawler asks about it.
    Those are separate facts and conflating them is what made 27 crawlers read as
    `unknown`: they were unknown because no policy existed for their host, and a
    crawler that never consults a policy is a different problem from one whose
    host has none.

    Args:
        module: Crawler module stem, without ``.py``.
        verdicts: Recorded verdicts by host.
        crawler_dir: Directory holding the crawler modules.

    Returns:
        The worst verdict among the crawler's hosts.

    """
    domains = source_domains(module, crawler_dir=crawler_dir)
    if not domains:
        return PolicyStatus.UNKNOWN
    seen = [verdicts.get(domain) for domain in domains]
    if any(verdict is HostVerdict.DISALLOWED for verdict in seen):
        return PolicyStatus.BLOCKED
    if seen and all(verdict in (HostVerdict.ALLOWED, HostVerdict.NO_POLICY) for verdict in seen):
        return PolicyStatus.OK
    return PolicyStatus.UNKNOWN


def _table_columns(session: Session, table: str) -> set[str] | None:
    """Return a table's columns, or None when the table does not exist.

    Through the SQLAlchemy inspector rather than `information_schema`: the
    catalogue query is PostgreSQL-only, and the measurement has no reason to be.
    Reading the columns rather than issuing a probe `SELECT` also means a missing
    table is an answer instead of an exception to catch.
    """
    from sqlalchemy import inspect

    try:
        return {column["name"] for column in inspect(session.get_bind()).get_columns(table)}
    except Exception:
        # Any reflection failure means the same thing here: this table cannot be
        # measured. The driver's exception types differ by dialect, and a report
        # that raised would lose every row after the one it could not read.
        logger.info("Could not read columns for %s", table, exc_info=True)
        return None


def measure_table(
    session: Session,
    table: str,
    *,
    column: str | None = None,
    stale_after_days: int = 30,
    now: datetime | None = None,
) -> TableMeasurement:
    """Read one table's row count and age.

    Args:
        session: An open session. Read-only; this never writes.
        table: The table to measure.
        column: The column that means "current" for this table, or None to use
            ``updated_at`` then ``created_at``.
        stale_after_days: Age at which the table is reported stale.
        now: The instant to measure against. Defaults to the current UTC time.

    Returns:
        The measurement, marked ``UNKNOWN`` when no timestamp could be read.

    """
    from datetime import UTC
    from datetime import datetime as _datetime

    from sqlalchemy import text

    columns = _table_columns(session, table)
    if columns is None:
        return TableMeasurement(table, column, 0, None, None, Freshness.UNKNOWN)

    chosen = column if column in columns else next((c for c in ("updated_at", "created_at") if c in columns), None)
    if chosen is None:
        rows = session.execute(text(f"SELECT count(*) FROM {table}")).scalar() or 0  # noqa: S608 - fixed name
        return TableMeasurement(table, None, int(rows), None, None, Freshness.UNKNOWN)

    rows, latest = session.execute(
        text(f"SELECT count(*), max({chosen}) FROM {table}"),  # noqa: S608 - fixed names
    ).one()
    rows = int(rows or 0)
    if not rows or latest is None:
        return TableMeasurement(table, chosen, rows, None, None, Freshness.EMPTY)

    # The age is computed here rather than in SQL. `now() - max(col)` is a
    # PostgreSQL expression, and the measurement is worth more portable than that
    # saves: the same code then runs against a test database.
    if not isinstance(latest, _datetime):
        latest = _datetime.fromisoformat(str(latest))
    if latest.tzinfo is None:
        latest = latest.replace(tzinfo=UTC)
    reference = now or _datetime.now(UTC)
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=UTC)

    # Clamped at zero: a domain date can legitimately be in the future -- the
    # `game` table holds scheduled fixtures, so its newest date is days ahead --
    # and a negative age is not a fact about staleness. Reporting one would make
    # a reader wonder what a "-2d old table" could mean; clamped, the verdict is
    # the honest one: nothing is behind.
    age_days = max(0, (reference - latest).days)
    freshness = Freshness.STALE if age_days > stale_after_days else Freshness.CURRENT
    return TableMeasurement(table, chosen, rows, latest, age_days, freshness)


def run_summaries(session: Session, *, window_hours: int = RUN_WINDOW_HOURS) -> dict[str, RunSummary]:
    """Read the run ledger and count how many successes actually collected data.

    A zero-yield success is one where neither a record was read nor a record was
    written. Both columns are consulted because a crawler can read a page and
    write nothing legitimately -- an unchanged page -- and counting only writes
    would call that a failure.

    Args:
        session: An open session. Read-only.
        window_hours: How far back to look.

    Returns:
        A summary per crawler that has runs in the window.

    """
    from datetime import UTC, timedelta
    from datetime import datetime as _datetime

    from sqlalchemy import text

    since = _datetime.now(UTC).replace(tzinfo=None) - timedelta(hours=window_hours)
    try:
        rows = session.execute(
            text(
                "SELECT crawler, status, records_read, records_written, started_at, error_code "
                "FROM crawl_execution_runs WHERE started_at > :since"
            ),
            {"since": since},
        ).all()
    except Exception:
        # A deployment without the reliability tables still deserves an inventory.
        logger.info("crawl_execution_runs is not readable; skipping the run measurement", exc_info=True)
        return {}

    # Aggregated here rather than in SQL for the same reason the table age is:
    # `count(*) FILTER (...)` and `make_interval` are PostgreSQL, and a
    # measurement that cannot run against a test database is a measurement whose
    # error handling silently replaces it with nothing.
    summaries: dict[str, list[int]] = {}
    latest_at: dict[str, datetime] = {}
    for crawler, status, records_read, records_written, started_at, error_code in rows:
        counts = summaries.setdefault(str(crawler), [0, 0, 0, 0])
        counts[0] += 1
        if status == "success":
            counts[1] += 1
            if not (records_read or 0) and not (records_written or 0):
                counts[2] += 1
        if error_code in POLICY_OUTCOME_CODES:
            counts[3] += 1
        if started_at is not None:
            seen = latest_at.get(str(crawler))
            latest_at[str(crawler)] = started_at if seen is None or started_at > seen else seen
    return {
        crawler: RunSummary(
            crawler=crawler,
            window_hours=window_hours,
            total=counts[0],
            succeeded=counts[1],
            zero_yield=counts[2],
            latest_at=latest_at.get(crawler),
            policy_blocked=counts[3],
        )
        for crawler, counts in summaries.items()
    }


def rag_chunk_counts(session: Session) -> dict[str, int]:
    """Return how many answer chunks each table contributes.

    This is the measurement that turns "the table is old" into "the assistant may
    answer from it".
    """
    from sqlalchemy import text

    try:
        rows = session.execute(
            text("SELECT source_table, count(*) FROM rag_chunks GROUP BY source_table"),
        ).all()
    except Exception:
        # A deployment without the RAG tables is still a valid inventory; the
        # AI-exposure section is then empty rather than the report failing.
        logger.info("rag_chunks is not readable; skipping the AI-exposure measurement", exc_info=True)
        return {}
    return {str(table): int(count) for table, count in rows if table}


@cache
def _import_index(source_dir: Path) -> dict[str, tuple[str, ...]]:
    """Map every module under a tree to the dotted names it imports.

    Built once and cached because the alternative is what the first version did:
    re-parse the whole tree per repository, which measured 108 seconds for five
    repositories and made the report too slow to use as a CI gate.

    A per-file parse failure is skipped rather than raised: one unparseable file
    must not hide every consumer after it.

    Args:
        source_dir: Root of the tree to index.

    Returns:
        File path to the imported dotted names, each entry sorted.

    """
    index: dict[str, tuple[str, ...]] = {}
    for path in sorted(source_dir.rglob("*.py")):
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            logger.info("Skipping unreadable file while indexing imports: %s", path)
            continue
        # Cheap pre-filter before the parse. Every way of reaching a repository
        # names its package -- `from src.repositories.x import Y`, `import
        # src.repositories.x`, even `import_module("src.repositories.x")` -- so a
        # file without the substring cannot be a consumer. Without this the index
        # costs a full AST parse of the tree, which measured 19 seconds.
        if "repositories" not in text:
            continue
        try:
            tree = ast.parse(text)
        except SyntaxError:
            logger.info("Skipping unparseable file while indexing imports: %s", path)
            continue
        names: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                names.add(node.module or "")
            elif isinstance(node, ast.Import):
                names.update(alias.name for alias in node.names)
        index[str(path)] = tuple(sorted(names))
    return index


def consumers_of(repository: str, *, source_dir: Path = SOURCE_DIR) -> tuple[str, ...]:
    """Return the modules that import a repository.

    Import-based on purpose: it is a syntactic fact. Reading the repository's own
    model imports to infer its table was the derivation that proved unreliable,
    and the same reasoning applies here.

    Args:
        repository: Repository module name, without a package prefix.
        source_dir: Root of the tree to search.

    Returns:
        Sorted paths of the importing modules.

    """
    if not repository:
        return ()
    return tuple(
        path for path, names in _import_index(source_dir).items() if any(name.endswith(repository) for name in names)
    )


def _threshold_for(declaration: SourceDeclaration, cadence: dict[str, Cadence], default: int) -> int:
    """Return the age at which this crawler's tables stop being plausible.

    Derived from the job that runs it rather than fixed: a table refreshed daily
    and one refreshed monthly mean different things at thirty days old, and a
    single threshold would call the first healthy or the second broken.
    """
    if declaration.expected_max_age_days is not None:
        return declaration.expected_max_age_days
    if declaration.job and declaration.job in cadence:
        return cadence[declaration.job].max_age_days
    return default


def _measure_declared(
    session_factory: Callable[[], Session] | None,
    *,
    stale_after_days: int,
    cadence: dict[str, Cadence] | None = None,
    window_hours: int = RUN_WINDOW_HOURS,
) -> tuple[dict[str, tuple[TableMeasurement, ...]], dict[str, int], dict[str, RunSummary], list[str]]:
    """Read every declared table once, returning measurements, chunks, runs and drift.

    One session for all of it: the readings are independent, and reconnecting per
    table would make the report's own runtime the dominant cost.
    """
    measurements: dict[str, tuple[TableMeasurement, ...]] = {}
    chunks: dict[str, int] = {}
    runs: dict[str, RunSummary] = {}
    drift: list[str] = []
    if session_factory is None:
        return measurements, chunks, runs, drift

    schedules = cadence if cadence is not None else scheduled_cadence()
    with session_factory() as session:
        chunks = rag_chunk_counts(session)
        runs = run_summaries(session, window_hours=window_hours)
        for module, declaration in DECLARED.items():
            threshold = _threshold_for(declaration, schedules, stale_after_days)
            measurements[module] = tuple(
                measure_table(
                    session,
                    table,
                    column=declaration.freshness_column,
                    stale_after_days=threshold,
                )
                for table in declaration.tables
            )
            # A declared table that cannot be measured is either a typo or a
            # table with no timestamp. Both make the row claim less than the
            # declaration promises, so the declaration is what gets reported.
            drift.extend(
                f"{module}: declared table '{m.table}' could not be measured"
                for m in measurements[module]
                if m.freshness is Freshness.UNKNOWN and m.rows == 0
            )
    return measurements, chunks, runs, drift


def _build_rows(
    *,
    verdicts: Mapping[str, HostVerdict],
    measurements: dict[str, tuple[TableMeasurement, ...]],
    chunks: dict[str, int],
    runs: dict[str, RunSummary],
    crawler_dir: Path,
) -> list[SourceRow]:
    """Assemble one row per crawler, declared or not."""
    rows: list[SourceRow] = []
    for module in discover_crawler_modules(crawler_dir=crawler_dir):
        declaration = DECLARED.get(module)
        table_measurements = measurements.get(module, ())
        rows.append(
            SourceRow(
                crawler=module,
                source_domains=source_domains(module, crawler_dir=crawler_dir),
                policy_status=policy_status_of(module, verdicts, crawler_dir=crawler_dir),
                enforced=consults_policy(module, crawler_dir=crawler_dir),
                declaration=declaration,
                measurements=table_measurements,
                rag_chunks=sum(chunks.get(m.table, 0) for m in table_measurements),
                runs=runs.get(declaration.ledger_name or module.removesuffix("_crawler")) if declaration else None,
                consumers=(consumers_of(declaration.repository) if declaration and declaration.repository else ()),
            ),
        )
    return rows


def _collect_findings(
    rows: Sequence[SourceRow],
    *,
    crawler_dir: Path,
) -> tuple[list[str], list[str]]:
    """Return the drift and advisories the rows imply.

    Both are about the report's own trustworthiness as much as about the data:
    a declaration for a crawler that no longer exists, a blocked crawler with no
    alternative recorded, and the coverage gap are all reasons to doubt a row
    rather than read it.
    """
    drift: list[str] = []
    advisories: list[str] = []

    present = set(discover_crawler_modules(crawler_dir=crawler_dir))
    drift.extend(
        f"{module}: declared for a crawler module that does not exist" for module in DECLARED if module not in present
    )
    drift.extend(
        f"{row.crawler}: blocked by policy with no alternative_source recorded"
        for row in rows
        if row.policy_status is PolicyStatus.BLOCKED and row.declaration and not row.declaration.alternative_source
    )

    stale_tables = [row for row in rows if any(m.freshness is Freshness.STALE for m in row.measurements)]
    bare = [row for row in rows if row.runs is not None and not row.runs.yields]
    undeclared = sorted(row.crawler for row in rows if not row.declared)
    if undeclared:
        named = ", ".join(undeclared[:ADVISORY_NAME_LIMIT])
        more = " ..." if len(undeclared) > ADVISORY_NAME_LIMIT else ""
        advisories.append(
            f"{len(undeclared)} of {len(rows)} crawlers have no declaration yet, so their tables are "
            f"not measured: {named}{more}",
        )
    unguarded = sorted(row.crawler for row in rows if row.policy_status is PolicyStatus.OK and not row.enforced)
    if unguarded:
        advisories.append(
            f"{len(unguarded)} crawlers may read their source but never consult the policy, so a policy "
            f"change would not stop them: {', '.join(unguarded[:ADVISORY_NAME_LIMIT])}"
            f"{' ...' if len(unguarded) > ADVISORY_NAME_LIMIT else ''}"
        )
    advisories.extend(
        f"{row.crawler} reported {row.runs.succeeded} success"
        f"{'' if row.runs.succeeded == 1 else 'es'} in {row.runs.window_hours}h, none of which read or "
        f"wrote a record"
        + (
            f"; its table is {row.stalest.render_age()} old"
            if row in stale_tables
            else "; it writes no table of its own"
        )
        for row in bare
        if row.runs is not None
    )
    advisories.extend(
        f"{row.crawler}: its sources read as permitted but {row.runs.policy_blocked} run(s) were "
        f"recorded as refused -- the URL is probably built at runtime, which this report cannot see"
        for row in rows
        if row.policy_status is PolicyStatus.OK and row.runs is not None and row.runs.policy_blocked
    )
    advisories.extend(
        f"AI-visible stale data: {row.crawler} feeds {row.rag_chunks} chunks from a table "
        f"last current {row.stalest.render_age() if row.stalest else 'never'}"
        for row in rows
        if row.rag_chunks > 0 and row.collection_state in (CollectionState.NOT_COLLECTED, CollectionState.UNEPLAINED)
    )
    return drift, advisories


def build_inventory(
    *,
    session_factory: Callable[[], Session] | None = None,
    crawler_dir: Path = CRAWLER_DIR,
    robots_dir: Path = ROBOTS_DIR,
    stale_after_days: int = 30,
    window_hours: int = RUN_WINDOW_HOURS,
) -> SourceInventory:
    """Build the inventory, reading the database when one is reachable.

    Args:
        session_factory: Opens a session for the measurements. When omitted, no
            table is measured and every row reports its declaration alone.
        crawler_dir: Directory holding crawler modules (tests override this).
        robots_dir: Directory holding robots snapshots.
        stale_after_days: Default age at which a table is reported stale.
        window_hours: How far back to read the run ledger.

    Returns:
        The inventory, with its own coverage gaps attached.

    """
    verdicts = host_verdicts(policy_dir=robots_dir)
    measurements, chunks, runs, drift = _measure_declared(
        session_factory,
        stale_after_days=stale_after_days,
        window_hours=window_hours,
    )
    rows = _build_rows(
        verdicts=verdicts,
        measurements=measurements,
        chunks=chunks,
        runs=runs,
        crawler_dir=crawler_dir,
    )
    more_drift, advisories = _collect_findings(rows, crawler_dir=crawler_dir)
    return SourceInventory(
        rows=tuple(rows),
        drift=tuple(drift + more_drift),
        advisories=tuple(advisories),
    )


__all__ = [
    "CRAWLER_DIR",
    "DECLARED",
    "Cadence",
    "Freshness",
    "PolicyStatus",
    "SourceDeclaration",
    "SourceInventory",
    "SourceRow",
    "TableMeasurement",
    "build_inventory",
    "consumers_of",
    "discover_crawler_modules",
    "measure_table",
    "policy_status_of",
    "rag_chunk_counts",
    "run_summaries",
    "scheduled_cadence",
    "source_domains",
]
