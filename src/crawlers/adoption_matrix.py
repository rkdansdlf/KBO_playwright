"""Adoption matrix: how far each crawler has moved onto the shared contract.

Tracks 4 and 5 migrated two crawlers end to end -- transport, run ledger, dead
letter queue, replay. Neither added a framework; each just used the pieces that
already existed. That raises the obvious question for the rest of the fleet:
which ones are close, which are far, and in what order should they move?

The matrix answers that by separating two kinds of fact:

* **Derived** facts are read out of the source. Transport, throttling, snapshot,
  persistence, ledger, dead letter, and replay are all visible in the code, so
  reading them by hand would only create something to forget.
* **Declared** facts are design intent that no tool can infer: what a crawler
  treats as its unit of work, and what it does when a source comes back with
  nothing.

`verify()` cross-checks the two. A crawler that claims a dead letter queue
without a ledger, or that reaches for `httpx` while claiming the shared HTTP
client, fails -- so the matrix cannot quietly drift away from the code it
describes.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

CRAWLER_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CRAWLER_DIR.parents[1]


class Transport(StrEnum):
    """A way a crawler reaches a source."""

    CRAWLER_HTTP_CLIENT = "crawler_http_client"
    """`CrawlerHttpClient`: validation, throttle, retry, circuit, classification."""

    RAW_HTTPX = "raw_httpx"
    """Builds its own `httpx.AsyncClient`, or shares one via `BaseHttpCrawler`."""

    PLAYWRIGHT = "playwright"
    """Browser-driven, with no HTTP transport of its own."""

    API_CLIENT = "api_client"
    """Delegated to a purpose-built API client, e.g. the YouTube or Naver wrapper."""


def transports_of(facts: ModuleFacts) -> tuple[Transport, ...]:
    """Return the transports a crawler uses, in reporting order."""
    order = (
        Transport.CRAWLER_HTTP_CLIENT,
        Transport.RAW_HTTPX,
        Transport.PLAYWRIGHT,
        Transport.API_CLIENT,
    )
    return tuple(transport for transport in order if transport in facts.transports)


class EmptySemantics(StrEnum):
    """What "the source answered with nothing" means to a crawler."""

    TYPED = "typed"
    """`CrawlResult.EMPTY` is distinguished from a failure."""

    TYPED_CONFIRMED = "typed_confirmed"
    """`EMPTY` is only claimed when the expected structure was positively seen."""

    COLLAPSED = "collapsed"
    """An empty list or `None` is returned for both a quiet day and an outage."""


class Granularity(StrEnum):
    """The unit of work a crawler records, and the unit a replay re-runs."""

    UNKNOWN = "unknown"
    SOURCE = "source"
    DATE = "date"
    TEAM = "team"
    PLAYER = "player"
    GAME = "game"
    MONTH = "month"
    SEASON = "season"
    DOCUMENT = "document"


class Fallback(StrEnum):
    """What happens when the primary source cannot answer."""

    NONE = "none"
    ALTERNATE_SOURCE = "alternate_source"
    BROWSER = "browser"
    TRANSPORT_RETRY = "transport_retry"


@dataclass(frozen=True)
class DesignFacts:
    """Declared intent: the axes a tool cannot read out of the source."""

    granularity: Granularity
    empty: EmptySemantics
    fallback: Fallback = Fallback.NONE
    note: str = ""


@dataclass(frozen=True)
class ModuleFacts:
    """Derived facts: what the module's own source shows."""

    module: str
    base_class: str
    transports: frozenset[Transport]
    owns_throttle: bool
    snapshot: bool
    persistence: bool
    ledger: bool
    dead_letter: bool
    uses_crawl_result: bool
    has_entrypoint: bool

    @property
    def transport(self) -> Transport | None:
        """Return the single dominant transport, or None when there is none.

        A crawler may legitimately be hybrid -- the schedule reads a Naver API
        and falls back to a KBO browser page -- so this is the first transport in
        reporting order rather than a claim that only one exists.
        """
        found = transports_of(self)
        return found[0] if found else None

    @property
    def has_transport(self) -> bool:
        """Return whether the crawler reaches a source at all."""
        """Return whether the crawler reaches a source at all."""
        return bool(self.transports)

    @property
    def shared_http(self) -> bool:
        """Return whether the crawler uses the shared HTTP client."""
        """Return whether the crawler uses the shared HTTP client."""
        """Return whether the crawler uses the shared HTTP client at all."""
        return Transport.CRAWLER_HTTP_CLIENT in self.transports

    @property
    def replay(self) -> bool:
        """Return whether a replay handler is registered for this crawler."""
        """Return whether a replay handler is registered for this crawler."""
        """Return whether a replay handler is registered for this crawler."""
        return self.module in REPLAY_MODULES


@dataclass(frozen=True)
class CrawlerRow:
    """One row of the matrix: derived facts plus whatever design was declared."""

    facts: ModuleFacts
    design: DesignFacts | None = None

    @property
    def module(self) -> str:
        """Return the crawler module name."""
        return self.facts.module

    @property
    def fully_adopted(self) -> bool:
        """Return whether the crawler closes the whole chain.

        A full chain means the transport is shared, the outcome is typed, and the
        work is recorded, queued, and replayable. A crawler can be perfectly
        useful without it; this only answers "would a replay find its way back".
        """
        return (
            self.facts.shared_http
            and self.facts.uses_crawl_result
            and self.facts.ledger
            and self.facts.dead_letter
            and self.facts.replay
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize the row for JSON artifacts."""
        return {
            "module": self.module,
            "base_class": self.facts.base_class,
            "transports": [transport.value for transport in transports_of(self.facts)],
            "owns_throttle": self.facts.owns_throttle,
            "empty_semantics": (self.design.empty.value if self.design else EmptySemantics.COLLAPSED.value),
            "granularity": (self.design.granularity.value if self.design else Granularity.UNKNOWN.value),
            "fallback": (self.design.fallback.value if self.design else Fallback.NONE.value),
            "snapshot": self.facts.snapshot,
            "persistence": self.facts.persistence,
            "ledger": self.facts.ledger,
            "dead_letter": self.facts.dead_letter,
            "replay": self.facts.replay,
            "uses_crawl_result": self.facts.uses_crawl_result,
            "fully_adopted": self.fully_adopted,
            "note": (self.design.note if self.design else ""),
        }


@dataclass(frozen=True)
class AdoptionMatrix:
    """Every classified crawler plus what the classification found."""

    rows: tuple[CrawlerRow, ...] = field(default_factory=tuple)
    drift: tuple[str, ...] = field(default_factory=tuple)
    advisories: tuple[str, ...] = field(default_factory=tuple)

    def adopted(self) -> tuple[CrawlerRow, ...]:
        """Return the crawlers that close the whole chain."""
        return tuple(row for row in self.rows if row.fully_adopted)

    def by_transport(self, transport: Transport) -> tuple[CrawlerRow, ...]:
        """Return every crawler that reaches a source this way."""
        return tuple(row for row in self.rows if transport in row.facts.transports)

    def roadmap(self) -> tuple[CrawlerRow, ...]:
        """Order the unadopted fetchers for migration.

        Declared priority comes first: how much upstream damage a crawler can do
        matters more than how few of its axes already line up. Only the remainder
        is ordered by nearness -- the ones closest to a finished chain, so a
        migration buys a whole chain for the least work.
        """
        remaining = [
            row for row in self.rows if not row.fully_adopted and row.facts.has_transport and row.facts.has_entrypoint
        ]

        def nearness(row: CrawlerRow) -> tuple[int, str]:
            facts = row.facts
            satisfied = sum(
                (
                    facts.shared_http,
                    facts.uses_crawl_result,
                    facts.ledger,
                    facts.dead_letter,
                    facts.snapshot,
                    facts.persistence,
                    not facts.owns_throttle,
                ),
            )
            # Fewer satisfied axes first: those are the ones still to do.
            return (-satisfied, row.module)

        sequenced = [row for row in remaining if row.module in PRIORITY_ORDER]
        sequenced.sort(key=lambda row: PRIORITY_ORDER.index(row.module))
        rest = sorted((row for row in remaining if row.module not in PRIORITY_ORDER), key=nearness)
        return (*sequenced, *rest)

    def to_dict(self) -> dict[str, Any]:
        """Serialize the matrix for JSON artifacts."""
        return {
            "summary": {
                "total": len(self.rows),
                "fully_adopted": len(self.adopted()),
                "drift": len(self.drift),
                "advisories": len(self.advisories),
            },
            "rows": [row.to_dict() for row in self.rows],
            "roadmap": [row.module for row in self.roadmap()],
            "drift": list(self.drift),
            "advisories": list(self.advisories),
        }


#: Replay handlers registered with the dispatcher, by crawler name.
REPLAY_HANDLERS: dict[str, str] = {
    "awards": "award_crawler",
    "roster_transactions": "roster_transaction_crawler",
    "schedule": "schedule_crawler",
}

#: The module names behind those handlers, for lookup by module.
REPLAY_MODULES: frozenset[str] = frozenset(REPLAY_HANDLERS.values())

#: Design intent, declared by hand. Everything else is derived.
DECLARED: dict[str, DesignFacts] = {
    "award_crawler": DesignFacts(
        granularity=Granularity.SOURCE,
        empty=EmptySemantics.TYPED,
        note="Multi-source aggregation. One source failing is a partial run, not a failed one.",
    ),
    "roster_transaction_crawler": DesignFacts(
        granularity=Granularity.DATE,
        empty=EmptySemantics.TYPED_CONFIRMED,
        fallback=Fallback.BROWSER,
        note="A quiet day is common, so EMPTY requires the expected section to have been seen.",
    ),
    "schedule_crawler": DesignFacts(
        granularity=Granularity.MONTH,
        empty=EmptySemantics.TYPED_CONFIRMED,
        fallback=Fallback.BROWSER,
        note="Naver API primary with a KBO browser fallback. Feeds nearly every other crawl.",
    ),
    "game_detail_crawler": DesignFacts(
        granularity=Granularity.GAME,
        empty=EmptySemantics.COLLAPSED,
        fallback=Fallback.NONE,
        note="Deferred: large surface with validation and partial-recovery logic to preserve.",
    ),
    "relay_crawler": DesignFacts(
        granularity=Granularity.GAME,
        empty=EmptySemantics.COLLAPSED,
        note="Third in line after the schedule and game-detail migrations.",
    ),
    "ticket_crawler": DesignFacts(
        granularity=Granularity.TEAM,
        empty=EmptySemantics.COLLAPSED,
        fallback=Fallback.ALTERNATE_SOURCE,
        note="Shares `BaseHttpCrawler`, so the transport move is a base change.",
    ),
}

#: Migration order decided by upstream impact rather than by how little work is
#: left. The schedule is done -- it fed nearly every other crawl, so a silent
#: failure there poisoned everything downstream. Game detail is the largest
#: remaining surface, and relay comes next.
PRIORITY_ORDER: tuple[str, ...] = (
    "game_detail_crawler",
    "relay_crawler",
)

#: Base classes whose subclasses inherit their HTTP transport.
_HTTP_BASES = frozenset({"BaseHttpCrawler"})
_PLAYWRIGHT_BASES = frozenset({"BasePlaywrightCrawler", "RelayCrawler", "NaverNewsCrawlerBase"})

_ENTRYPOINT_NAMES = frozenset({"run", "crawl", "fetch", "fetch_all", "collect"})
_ENTRYPOINT_PREFIXES = ("crawl", "fetch", "collect")


def _is_entrypoint(name: str) -> bool:
    """Return whether a method name reads as a crawl entrypoint."""
    return name in _ENTRYPOINT_NAMES or name.startswith(_ENTRYPOINT_PREFIXES)


def _module_source(module: str) -> str:
    return (CRAWLER_DIR / f"{module}.py").read_text(encoding="utf-8")


def _crawler_class(tree: ast.Module) -> ast.ClassDef | None:
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name.endswith("Crawler"):
            return node
    return None


def _base_name(node: ast.ClassDef) -> str:
    for base in node.bases:
        return ast.unparse(base)
    return ""


def _has_entrypoint(tree: ast.Module) -> bool:
    """Return whether the module or one of its bases defines a crawl entrypoint."""
    if any(
        isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)) and _is_entrypoint(node.name)
        for node in ast.walk(tree)
    ):
        return True
    # A base class supplies `run()`, so the subclass is a crawler too.
    base = _crawler_class(tree)
    return base is not None and _base_name(base) in _HTTP_BASES | _PLAYWRIGHT_BASES


_PAGE_CALLS = frozenset({"page_context", "new_page", "page"})


def _attribute_transport(name: str) -> Transport | None:
    """Return the transport implied by an attribute or method name."""
    if name == "CrawlerHttpClient":
        return Transport.CRAWLER_HTTP_CLIENT
    if name == "AsyncClient":
        return Transport.RAW_HTTPX
    if name == "http_client":
        # `BaseHttpCrawler.http_client()` hands out a shared AsyncClient.
        return Transport.RAW_HTTPX
    if name in _PAGE_CALLS:
        return Transport.PLAYWRIGHT
    return None


def _name_transport(name: str) -> Transport | None:
    """Return the transport implied by a referenced symbol name."""
    if name == "CrawlerHttpClient":
        return Transport.CRAWLER_HTTP_CLIENT
    if name == "AsyncPlaywrightPool":
        return Transport.PLAYWRIGHT
    if name.endswith("Client"):
        # A purpose-built wrapper: Naver search, YouTube, transit times.
        return Transport.API_CLIENT
    return None


def _transport_of_node(node: ast.AST) -> Transport | None:
    """Return the transport a single AST node implies, if any."""
    if isinstance(node, ast.Name):
        return _name_transport(node.id)
    if isinstance(node, ast.Attribute):
        return _attribute_transport(node.attr)
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        return _attribute_transport(node.func.attr)
    return None


def _resolve_transports(tree: ast.Module, base_class: str) -> frozenset[Transport]:
    """Return every transport a module reaches a source through.

    A crawler can be genuinely hybrid -- an API primary with a browser fallback --
    so this returns a set rather than picking a winner. Detection walks the AST so
    a mention in a comment or a URL cannot register as a transport.
    """
    found: set[Transport] = set()
    for node in ast.walk(tree):
        transport = _transport_of_node(node)
        if transport is not None:
            found.add(transport)
    if base_class in _HTTP_BASES:
        found.add(Transport.RAW_HTTPX)
    if base_class in _PLAYWRIGHT_BASES:
        found.add(Transport.PLAYWRIGHT)
    return frozenset(found)


def scan_module(module: str) -> ModuleFacts:
    """Read one crawler module's source and classify it.

    Args:
        module: Module name, e.g. ``"award_crawler"``.

    Returns:
        The derived facts.

    """
    source = _module_source(module)
    tree = ast.parse(source)
    node = _crawler_class(tree)
    base_class = _base_name(node) if node is not None else ""
    transports = _resolve_transports(tree, base_class)

    return ModuleFacts(
        module=module,
        base_class=base_class,
        transports=transports,
        # A crawler that reaches for the shared throttle alongside its own
        # transport waits twice and hides the adaptive backoff.
        owns_throttle=("throttle.wait" in source or "delay_async" in source)
        and Transport.CRAWLER_HTTP_CLIENT not in transports,
        snapshot="save_raw_snapshots" in source,
        persistence="SessionLocal" in source,
        ledger="track_crawl_run" in source,
        dead_letter="DeadLetterSpec" in source or "enqueue_failure" in source,
        uses_crawl_result="CrawlResult" in source,
        has_entrypoint=_has_entrypoint(tree),
    )


def discover_modules() -> tuple[str, ...]:
    """Return every crawler module present on disk."""
    return tuple(sorted(path.stem for path in CRAWLER_DIR.glob("*_crawler.py")))


def advise_row(row: CrawlerRow) -> list[str]:
    """Return observations that are worth knowing but are not defects.

    A crawler that composes the shared client while inheriting a base that hands
    out raw ``httpx`` clients carries two HTTP paths and uses one. That is not
    broken, but it is exactly the shape that made the award and roster migrations
    necessary, so it belongs on the list.
    """
    facts = row.facts
    notes: list[str] = []
    if facts.shared_http and Transport.RAW_HTTPX in facts.transports:
        notes.append(
            f"{row.module}: uses CrawlerHttpClient but inherits a raw httpx path from "
            f"{facts.base_class or 'its base'}; the second path is unused",
        )
    if facts.uses_crawl_result and not facts.ledger:
        notes.append(f"{row.module}: classifies outcomes but records no run, so failures leave no trace")
    if facts.has_entrypoint and not facts.has_transport:
        # A crawler with an entrypoint but no recognised transport is far more
        # likely to be a gap in detection than a crawler that fetches nothing.
        notes.append(f"{row.module}: has a crawl entrypoint but no transport was detected; check the classifier")
    return notes


def verify_row(row: CrawlerRow) -> list[str]:
    """Return the ways one row disagrees with its own source.

    Args:
        row: The row to check.

    Returns:
        Human-readable drift messages, empty when the row is consistent.

    """
    facts = row.facts
    design = row.design
    problems: list[str] = []

    if facts.shared_http and facts.owns_throttle:
        problems.append(
            f"{row.module}: uses CrawlerHttpClient but also throttles manually; the wait is doubled",
        )
    if design is not None:
        if design.empty in {EmptySemantics.TYPED, EmptySemantics.TYPED_CONFIRMED} and not facts.uses_crawl_result:
            problems.append(f"{row.module}: declares a typed empty but never uses CrawlResult")
        if design.granularity in {Granularity.DATE, Granularity.MONTH} and not facts.replay:
            problems.append(f"{row.module}: a {design.granularity.value}-granular crawler should have a replay handler")
    if facts.dead_letter and not facts.ledger:
        problems.append(f"{row.module}: enqueues dead letters without recording a run")
    if facts.replay and not facts.dead_letter:
        problems.append(f"{row.module}: replay is registered but the crawler enqueues no dead letters")
    if row.fully_adopted and design is None:
        problems.append(f"{row.module}: closes the whole chain but has no declared design facts")
    return problems


def build_matrix() -> AdoptionMatrix:
    """Classify every crawler module and collect the drift found on the way.

    Returns:
        The matrix, with drift messages attached.

    """
    rows: list[CrawlerRow] = []
    drift: list[str] = []
    advisories: list[str] = []
    for module in discover_modules():
        row = CrawlerRow(facts=scan_module(module), design=DECLARED.get(module))
        rows.append(row)
        drift.extend(verify_row(row))
        advisories.extend(advise_row(row))

    present = set(discover_modules())
    drift.extend(
        f"{module}: declared design for a module that does not exist" for module in DECLARED if module not in present
    )

    return AdoptionMatrix(rows=tuple(rows), drift=tuple(drift), advisories=tuple(advisories))


def render_markdown(matrix: AdoptionMatrix) -> str:
    """Render the matrix as a markdown table with a drift section.

    Args:
        matrix: The matrix to render.

    Returns:
        Markdown text.

    """
    header = "| crawler | transport | empty | unit | fallback | snapshot | ledger | DLQ | replay |"
    divider = "| --- | --- | --- | --- | --- | --- | --- | --- | --- |"
    lines = [header, divider]
    for row in sorted(matrix.rows, key=lambda item: (not item.fully_adopted, item.module)):
        facts = row.facts
        design = row.design
        cells = "| `{module}` | {transport} | {empty} | {unit} | {fallback} | {snap} | {ledger} | {dlq} | {replay} |"
        lines.append(
            cells.format(
                module=module_name(row),
                transport="+".join(t.value for t in transports_of(facts)) or "-",
                empty=(design.empty.value if design else "-"),
                unit=(design.granularity.value if design else "-"),
                fallback=(design.fallback.value if design else "-"),
                snap=_mark(present=facts.snapshot),
                ledger=_mark(present=facts.ledger),
                dlq=_mark(present=facts.dead_letter),
                replay=_mark(present=facts.replay),
            ),
        )

    lines.append("")
    adopted = module_names(matrix.adopted())
    lines.append(f"**Fully adopted ({len(adopted)})**: " + ", ".join(f"`{name}`" for name in adopted))
    lines.append("")
    lines.append("**Migration order** (fewest satisfied axes first):")
    for position, row in enumerate(matrix.roadmap(), start=1):
        lines.append(f"{position}. `{module_name(row)}`")
    if matrix.advisories:
        lines.append("")
        lines.append("**Advisories**")
        lines.extend(f"- {message}" for message in matrix.advisories)
    if matrix.drift:
        lines.append("")
        lines.append("**Drift**")
        lines.extend(f"- {message}" for message in matrix.drift)
    return "\n".join(lines)


def _mark(*, present: bool) -> str:
    """Return a table cell for a boolean fact."""
    return "Y" if present else "-"


def module_name(row: CrawlerRow) -> str:
    return row.module


def module_names(rows: tuple[CrawlerRow, ...]) -> tuple[str, ...]:
    return tuple(row.module for row in rows)


__all__ = [
    "DECLARED",
    "PRIORITY_ORDER",
    "REPLAY_HANDLERS",
    "REPLAY_MODULES",
    "AdoptionMatrix",
    "CrawlerRow",
    "DesignFacts",
    "EmptySemantics",
    "Fallback",
    "Granularity",
    "ModuleFacts",
    "Transport",
    "advise_row",
    "build_matrix",
    "discover_modules",
    "render_markdown",
    "scan_module",
    "verify_row",
]
