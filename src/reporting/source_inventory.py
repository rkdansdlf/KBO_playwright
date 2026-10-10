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
from urllib import robotparser

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
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
}


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
    declaration: SourceDeclaration | None
    measurements: tuple[TableMeasurement, ...] = ()
    rag_chunks: int = 0
    consumers: tuple[str, ...] = ()

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
            "declared": self.declared,
            "alternative_source": (self.declaration.alternative_source if self.declaration else None),
            "priority": (self.declaration.priority if self.declaration else None),
            "rag_chunks": self.rag_chunks,
            "consumers": list(self.consumers),
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

    def ai_exposure(self) -> tuple[SourceRow, ...]:
        """Return rows that feed answers *and* are not current.

        The reason this report exists. A stale table is a data problem; a stale
        table the assistant quotes from is a correctness problem, and the second
        is what an operator cannot see anywhere else.
        """
        return tuple(
            row
            for row in self.rows
            if row.rag_chunks > 0 and any(m.freshness is Freshness.STALE for m in row.measurements)
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


def blocked_hosts(*, robots_dir: Path = ROBOTS_DIR) -> tuple[frozenset[str], str | None]:
    """Return the hosts the newest robots snapshot refuses, and its filename.

    Parsed with the same stdlib parser the compliance layer uses, so this report
    cannot disagree with the crawlers about what the policy says.

    Args:
        robots_dir: Directory holding the saved robots.txt snapshots.

    Returns:
        The set of hosts whose policy refuses ``*``, and the snapshot's name.

    """
    snapshots = sorted(robots_dir.glob("robots_*.txt"))
    if not snapshots:
        return frozenset(), None
    newest = snapshots[-1]
    body = "\n".join(newest.read_text(encoding="utf-8", errors="replace").splitlines()[SNAPSHOT_HEADER_LINES:])

    parser = robotparser.RobotFileParser()
    parser.parse(body.splitlines())
    refused = not parser.can_fetch("*", "/")
    host = KBO_HOST
    return (frozenset({host}) if refused else frozenset()), newest.name


def policy_status_of(module: str, blocked: frozenset[str], *, crawler_dir: Path = CRAWLER_DIR) -> PolicyStatus:
    """Return whether a crawler can consult its source.

    A crawler that never asks the policy gets ``UNKNOWN`` rather than ``OK``:
    "allowed" is a claim someone has to make, and nothing here made it.
    """
    domains = source_domains(module, crawler_dir=crawler_dir)
    if not domains:
        return PolicyStatus.UNKNOWN
    if not consults_policy(module, crawler_dir=crawler_dir):
        return PolicyStatus.UNKNOWN
    if any("koreabaseball.com" in host for host in domains) and blocked:
        return PolicyStatus.BLOCKED
    return PolicyStatus.OK


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
) -> tuple[dict[str, tuple[TableMeasurement, ...]], dict[str, int], list[str]]:
    """Read every declared table once, returning measurements, chunks and drift.

    One session for all of it: the readings are independent, and reconnecting per
    table would make the report's own runtime the dominant cost.
    """
    measurements: dict[str, tuple[TableMeasurement, ...]] = {}
    chunks: dict[str, int] = {}
    drift: list[str] = []
    if session_factory is None:
        return measurements, chunks, drift

    schedules = cadence if cadence is not None else scheduled_cadence()
    with session_factory() as session:
        chunks = rag_chunk_counts(session)
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
    return measurements, chunks, drift


def _build_rows(
    *,
    blocked: frozenset[str],
    measurements: dict[str, tuple[TableMeasurement, ...]],
    chunks: dict[str, int],
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
                policy_status=policy_status_of(module, blocked, crawler_dir=crawler_dir),
                declaration=declaration,
                measurements=table_measurements,
                rag_chunks=sum(chunks.get(m.table, 0) for m in table_measurements),
                consumers=(consumers_of(declaration.repository) if declaration and declaration.repository else ()),
            ),
        )
    return rows


def _collect_findings(
    rows: Sequence[SourceRow],
    *,
    snapshot: str | None,
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

    undeclared = sorted(row.crawler for row in rows if not row.declared)
    if undeclared:
        named = ", ".join(undeclared[:ADVISORY_NAME_LIMIT])
        more = " ..." if len(undeclared) > ADVISORY_NAME_LIMIT else ""
        advisories.append(
            f"{len(undeclared)} of {len(rows)} crawlers have no declaration yet, so their tables are "
            f"not measured: {named}{more}",
        )
    advisories.append(
        f"robots policy read from {snapshot}" if snapshot else "no robots snapshot found; policy_status is UNKNOWN"
    )
    advisories.extend(
        f"AI-visible stale data: {row.crawler} feeds {row.rag_chunks} chunks from a table "
        f"last current {row.stalest.render_age() if row.stalest else 'never'}"
        for row in rows
        if row.rag_chunks > 0 and any(m.freshness is Freshness.STALE for m in row.measurements)
    )
    return drift, advisories


def build_inventory(
    *,
    session_factory: Callable[[], Session] | None = None,
    crawler_dir: Path = CRAWLER_DIR,
    robots_dir: Path = ROBOTS_DIR,
    stale_after_days: int = 30,
) -> SourceInventory:
    """Build the inventory, reading the database when one is reachable.

    Args:
        session_factory: Opens a session for the measurements. When omitted, no
            table is measured and every row reports its declaration alone.
        crawler_dir: Directory holding crawler modules (tests override this).
        robots_dir: Directory holding robots snapshots.
        stale_after_days: Default age at which a table is reported stale.

    Returns:
        The inventory, with its own coverage gaps attached.

    """
    blocked, snapshot = blocked_hosts(robots_dir=robots_dir)
    measurements, chunks, drift = _measure_declared(session_factory, stale_after_days=stale_after_days)
    rows = _build_rows(
        blocked=blocked,
        measurements=measurements,
        chunks=chunks,
        crawler_dir=crawler_dir,
    )
    more_drift, advisories = _collect_findings(rows, snapshot=snapshot, crawler_dir=crawler_dir)
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
    "blocked_hosts",
    "build_inventory",
    "consumers_of",
    "discover_crawler_modules",
    "measure_table",
    "policy_status_of",
    "rag_chunk_counts",
    "scheduled_cadence",
    "source_domains",
]
