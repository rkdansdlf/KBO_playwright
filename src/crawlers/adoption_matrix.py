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
import re
from dataclasses import dataclass, field
from enum import StrEnum
from functools import cache, lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterator

CRAWLER_DIR = Path(__file__).resolve().parent
REPO_ROOT = CRAWLER_DIR.parent.parent
PROJECT_ROOT = CRAWLER_DIR.parents[1]
SRC_ROOT = PROJECT_ROOT / "src"


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
    #: Ancestors that supply a governed request path by inheritance. Kept apart
    #: from ``transports`` because the client's use lives in the base's source,
    #: not in this module's: a Naver-news crawler mentions neither
    #: ``CrawlerHttpClient`` nor ``httpx`` and still reaches the shared client
    #: through ``NaverNewsCrawlerBase``. :attr:`shared_http` stays a statement
    #: about this module's own code; this records the path it inherits.
    inherited_shared_http: bool = False
    #: How many non-crawler modules read the data this crawler feeds.
    #:
    #: Derived from the repository's own sources rather than declared. A declared
    #: "important" list goes stale in the direction that matters most: the PBP
    #: crawler feeds the readiness gate, the SLA tracker, the gap report and the
    #: RAG index, and a ranking that omits it looks deliberate while pointing an
    #: operator at a crawler nothing depends on.
    upstream_dependents: int = 0

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
        # The inherited path counts. A crawler whose base owns the request code
        # reaches a source exactly as one that builds its own client does, and
        # reporting otherwise put "no transport was detected; check the
        # classifier" on crawlers whose classifier was already right.
        return bool(self.transports) or self.inherited_shared_http

    @property
    def shared_http(self) -> bool:
        """Return whether the crawler uses the shared HTTP client."""
        """Return whether the crawler uses the shared HTTP client."""
        """Return whether the crawler uses the shared HTTP client at all."""
        return Transport.CRAWLER_HTTP_CLIENT in self.transports

    @property
    def owns_transport(self) -> bool:
        """Return whether the crawler has one governed way of reaching a source.

        ``shared_http`` answers a narrower question -- does it use
        ``CrawlerHttpClient`` -- and reading that as the adoption criterion
        scored a browser-first crawler as half-converted. The three crawlers that
        close the whole reliability chain on Playwright have no HTTP client to
        share, and inheriting ``BaseHttpCrawler`` would not change that.

        What actually matters is the second request path. A crawler is governed
        when it either composes the shared client or drives a browser, and does
        *not* carry a raw ``httpx`` client alongside either. That is the same
        condition :func:`advise_row` already flags as an unused second path, so
        this asks the question once instead of inferring the answer from a
        transport the crawler does not have.

        Inheritance counts, because a crawler that reaches the shared client
        through an intermediate base uses it exactly as much as one that builds
        it. Leaving it out made every such crawler report as ungoverned while
        making good use of the client -- and the roadmap then sent an operator to
        convert a crawler that had already been converted.
        """
        if self.shared_http or self.inherited_shared_http:
            return True
        return Transport.PLAYWRIGHT in self.transports and Transport.RAW_HTTPX not in self.transports

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

        A full chain means the transport is governed, the outcome is typed, and
        the work is recorded, queued, and replayable. A crawler can be perfectly
        useful without it; this only answers "would a replay find its way back".

        ``uses_crawl_result`` is the axis a browser-first crawler has not closed,
        and it is a real gap rather than a structural impossibility: ``CrawlResult``
        models an HTTP fetch outcome, so a crawler driving a browser has no place
        to produce one and needs a browser-side vocabulary of its own. Scoring
        that as "adopted" would retire a gap the matrix cannot otherwise see, so
        it stays required and :attr:`remaining_axes` names it.
        """
        return (
            self.facts.owns_transport
            and self.facts.uses_crawl_result
            and self.facts.ledger
            and self.facts.dead_letter
            and self.facts.replay
        )

    @property
    def remaining_axes(self) -> tuple[str, ...]:
        """Return the required axes this crawler has not closed, in reading order.

        The roadmap ranks crawlers by how close they are, but a bare name gives
        no reason for the ordering, so a browser crawler that appears in it looks
        like a crawler nobody has started on. Naming the axis is what turns the
        list into work someone can pick up.
        """
        facts = self.facts
        gaps: list[tuple[int, str]] = []
        if not facts.owns_transport:
            gaps.append((0, "transport: no governed request path"))
        if not facts.uses_crawl_result:
            gaps.append((1, "typed outcome: no CrawlResult/CrawlOutcome import"))
        if not facts.ledger:
            gaps.append((2, "run ledger"))
        if not facts.dead_letter:
            gaps.append((3, "dead letter queue"))
        if not facts.replay:
            gaps.append((4, "replay handler"))
        return tuple(message for _, message in sorted(gaps))

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
            "upstream_dependents": self.facts.upstream_dependents,
            "persistence": self.facts.persistence,
            "ledger": self.facts.ledger,
            "dead_letter": self.facts.dead_letter,
            "replay": self.facts.replay,
            "uses_crawl_result": self.facts.uses_crawl_result,
            "owns_transport": self.facts.owns_transport,
            "fully_adopted": self.fully_adopted,
            "remaining_axes": list(self.remaining_axes),
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

        Upstream impact comes first: a crawler whose data feeds the readiness
        gate, the SLA tracker and the RAG index poisons all of them when it
        fails silently, so it is migrated before a crawler nothing reads. That
        was the documented rule and it was not implemented -- the order ran on
        nearness alone, which ranks the least-finished crawler first and so put
        the PBP crawler at position 26 of 32 while a crawler feeding nothing
        ranked above it.

        Nearing a finished chain breaks ties within an impact band rather than
        overriding it, because "cheapest to finish" is a tie-breaker among
        equally damaging crawlers, never a reason to delay the damaging one.

        A crawler whose write could not be resolved is ordered last rather than
        by the zero it scored. That zero is the absence of a measurement, not a
        measurement of absence, and ranking on it puts eight crawlers with known
        readers behind ones confirmed to feed nothing.

        That reordering applies only where it is true -- to a crawler with no
        measurement at all. It must not demote one that carries a measured
        impact, because "this caller was not traced" is not a reason to bury the
        two all-series crawlers below every crawler that feeds nothing: they
        resolved their tables from another caller and score 85 and 84 real
        readers. Sorting those by the same flag ranked a silent estimate as if
        it outranked a measurement, and sent the operator after the quietest
        work on the board.
        """
        remaining = [
            row for row in self.rows if not row.fully_adopted and row.facts.has_transport and row.facts.has_entrypoint
        ]

        def nearness(row: CrawlerRow) -> tuple[int, int, int, str]:
            facts = row.facts
            satisfied = sum(
                (
                    facts.owns_transport,
                    facts.uses_crawl_result,
                    facts.ledger,
                    facts.dead_letter,
                    facts.snapshot,
                    facts.persistence,
                    not facts.owns_throttle,
                ),
            )
            # Only a crawler with no measurement is demoted for lacking one. A
            # crawler that already resolved a table has a real reader count, and
            # the unresolved callers beside it are the ones the scan could not
            # trace -- not evidence that the count is untrustworthy.
            attribution = attribution_of(row.module)
            unmeasured = 1 if not attribution.models and attribution.unresolved_callers else 0
            return (unmeasured, -facts.upstream_dependents, -satisfied, row.module)

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
    "game_detail": "game_detail_crawler",
    "relay": "relay_crawler",
    "food": "food_crawler",
    "parking": "parking_crawler",
    "kbo_event": "kbo_event_crawler",
    "player_movement": "player_movement_crawler",
    "team_history": "team_history_crawler",
    "realtime_issue": "realtime_issue_crawler",
    "preview": "preview_crawler",
    "pbp": "pbp_crawler",
    "player_batting_all_series": "player_batting_all_series_crawler",
    "player_pitching_all_series": "player_pitching_all_series_crawler",
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
    "realtime_issue_crawler": DesignFacts(
        granularity=Granularity.SOURCE,
        empty=EmptySemantics.TYPED,
        fallback=Fallback.ALTERNATE_SOURCE,
        note="Naver API falls back to its HTML listing; Naver and MLBPark are separate replay units.",
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
    "team_history_crawler": DesignFacts(
        granularity=Granularity.SEASON,
        empty=EmptySemantics.TYPED,
        note="One page carries every season, so the replay unit is the page rather than a year. The "
        "page reads once per sweep and its empty case is not routine the way a per-year empty is, so "
        "the read is typed against the document the crawl expects and reports drift separately from "
        "absence.",
    ),
    "kbo_event_crawler": DesignFacts(
        granularity=Granularity.DOCUMENT,
        empty=EmptySemantics.TYPED,
        note="Seven standing pages, most of which are guides and are not supposed to link to an "
        "event, so an empty page is the normal state rather than a failure. It is also what a page "
        "that has stopped being the document the sweep asked for looks like from outside, so the "
        "read is typed against the site's own frame and carries whether retrying could still change "
        "it. `CrawlResult` models an HTTP fetch and this crawl has none; the vocabulary is its own.",
    ),
    "player_movement_crawler": DesignFacts(
        granularity=Granularity.SEASON,
        empty=EmptySemantics.TYPED,
        note="Most years record no transfers, so an empty result is the expected state and must not "
        "be alerted on. It is also what a page that lost its year selector looks like from outside, "
        "so the read is typed against the controls the crawl operates and the year is recorded as "
        "drift -- terminal, not retryable -- rather than as a quiet year. No exception is involved, "
        "which is why the year read is held separately from the failure list.",
    ),
    "food_crawler": DesignFacts(
        granularity=Granularity.TEAM,
        empty=EmptySemantics.TYPED,
        note="A page that could not be read used to arrive as an empty list with a log line and nothing "
        "else, which is indistinguishable from a stadium that genuinely sells nothing. The failure is "
        "now recorded per team and enqueued as that team's own letter, so the replay unit is the "
        "stadium that failed rather than the sweep.",
    ),
    "parking_crawler": DesignFacts(
        granularity=Granularity.TEAM,
        empty=EmptySemantics.TYPED,
        note="Same shape as the food sweep: per-team isolation so one unreadable lot page cannot "
        "quietly become an empty result for the whole stadium.",
    ),
    "relay_crawler": DesignFacts(
        granularity=Granularity.GAME,
        empty=EmptySemantics.TYPED,
        note="Typed per-game outcome. Absence, a mid-game fetch stop and a hard failure are distinct: only a "
        "genuine absence is a clean empty, because reporting a blocked or unparseable crawl as an empty game "
        "tells an operator the source has no relay when it was never asked.",
    ),
    "ticket_crawler": DesignFacts(
        granularity=Granularity.TEAM,
        empty=EmptySemantics.COLLAPSED,
        fallback=Fallback.ALTERNATE_SOURCE,
        note="KBO and team ticket pages use the shared HTTP client; the LG fallback remains explicit.",
    ),
    "preview_crawler": DesignFacts(
        granularity=Granularity.DATE,
        empty=EmptySemantics.TYPED_CONFIRMED,
        fallback=Fallback.BROWSER,
        note="Pregame data for a whole day is fetched at once and an empty pregame day is routine "
        "before first pitch, so EMPTY requires the game's own list to confirm the date holds no "
        "games. The write belongs to the preview batch, which also writes the manifest, so this "
        "crawler records what it read.",
    ),
    "pbp_crawler": DesignFacts(
        granularity=Granularity.GAME,
        empty=EmptySemantics.TYPED,
        fallback=Fallback.NONE,
        note="A browser crawl has no HTTP request, so the page read carries its own vocabulary: a "
        "game with no plays, a blocked crawl, a login redirect and a changed document are four "
        "different answers and only two of them are worth retrying. The crawl does not persist -- "
        "its callers decide whether the rows are a relay refresh or a repair -- so the run records "
        "what was read and the replay stores it.",
    ),
    "player_batting_all_series_crawler": DesignFacts(
        granularity=Granularity.SEASON,
        empty=EmptySemantics.TYPED,
        fallback=Fallback.ALTERNATE_SOURCE,
        note="One season and series is the unit, because that is what the page asks for and what a "
        "replay has to name to reproduce it. A page that cannot be read falls back to DB "
        "aggregation, which is recorded as partial rather than failed -- data was served, but it "
        "is the previously stored rows -- and the fallback monitor already raises that incident, "
        "so queueing a letter as well would make one failure into two.",
    ),
    "player_pitching_all_series_crawler": DesignFacts(
        granularity=Granularity.SEASON,
        empty=EmptySemantics.TYPED,
        fallback=Fallback.ALTERNATE_SOURCE,
        note="The pitching twin of the batting crawl, with the same unit, the same alternate "
        "source and the same reason for reporting a fallback as partial rather than failed.",
    ),
}

#: Migration order decided by upstream impact rather than by how little work is
#: left. The large surfaces are done -- schedule, game detail and relay all feed
#: something downstream, and a silent failure in any of them poisons whatever
#: reads it. Food and parking closed the last unthrottled request path, and the
#: three browser crawlers closed the last page-outcome vocabulary: a browser crawl
#: cannot produce a ``CrawlResult`` because it makes no HTTP request, so each one
#: states what its page said and whether retrying could still change it.
#:
#: What is named here is what comes next, and it must name live work. A priority
#: entry pointing at an already-adopted crawler is worse than no entry at all: the
#: report looks deliberate while directing an operator at finished work, so
#: `verify_priority_order` treats that as drift rather than letting it stand.
#:
#: An entry may only lead the computed order by naming a crawler whose impact
#: already puts it near the top. `verify_priority_order` enforces that: the
#: previous pair led with two crawlers that feed 16 modules and none, while the
#: crawler feeding 143 sat at position 26, so the report was sending an
#: operator to the quietest work on the board. Declared order is a way to
#: override a computed one, and without this check it could override in the
#: opposite direction for as long as nobody re-read the prose above it.
PRIORITY_ORDER: tuple[str, ...] = ()

#: Base classes that hand their subclasses a governed transport by inheritance.
#:
#: These name the *roots* only. A base is looked up by walking the inheritance
#: chain (see :func:`_inherits_any`), so an intermediate class never has to be
#: listed here and cannot be misfiled: ``NaverNewsCrawlerBase`` subclasses
#: ``BaseHttpCrawler``, and listing it as a browser base made every Naver-news
#: crawler report a browser transport while using the shared HTTP client.
#: ``RelayCrawler`` stays listed on its own evidence -- it drives a browser from
#: its base even though it also subclasses ``BaseHttpCrawler``.
_HTTP_BASES = frozenset({"BaseHttpCrawler"})
_PLAYWRIGHT_BASES = frozenset({"BasePlaywrightCrawler", "RelayCrawler"})

#: Bases that supply a crawl entrypoint to their subclasses.
_ENTRYPOINT_BASES = _HTTP_BASES | _PLAYWRIGHT_BASES

_ENTRYPOINT_NAMES = frozenset({"run", "crawl", "fetch", "fetch_all", "collect"})
_ENTRYPOINT_PREFIXES = ("crawl", "fetch", "collect")


def _is_entrypoint(name: str) -> bool:
    """Return whether a method name reads as a crawl entrypoint."""
    return name in _ENTRYPOINT_NAMES or name.startswith(_ENTRYPOINT_PREFIXES)


def _module_source(module: str) -> str:
    return (CRAWLER_DIR / f"{module}.py").read_text(encoding="utf-8")


def _crawler_source_or_empty(module: str) -> str:
    """Return a crawler module's source, empty when it is not one.

    ``src.crawlers`` also holds non-crawler modules -- ``futures_batting`` and
    the other data helpers -- and an ``ImportFrom`` of one is not a crawler
    import. Reading them as crawlers would raise on a file that is not named
    after a crawler class.
    """
    path = CRAWLER_DIR / f"{module}.py"
    return path.read_text(encoding="utf-8") if path.is_file() else ""


def _read_source(relative_path: str) -> str:
    """Return a repository-relative source file's text, empty when absent."""
    path = REPO_ROOT / relative_path
    return path.read_text(encoding="utf-8") if path.is_file() else ""


def _crawler_class(tree: ast.Module) -> ast.ClassDef | None:
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name.endswith("Crawler"):
            return node
    return None


def _base_name(node: ast.ClassDef) -> str:
    for base in node.bases:
        return ast.unparse(base)
    return ""


def _module_defining_class(name: str) -> ast.ClassDef | None:
    """Return the crawler class named ``name`` as it is defined in this repository."""
    for module in discover_modules():
        source = _read_source(f"src/crawlers/{module}.py")
        if not source or not re.search(rf"^class {re.escape(name)}\b", source, re.MULTILINE):
            continue
        try:
            tree = ast.parse(source)
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and node.name == name:
                return node
    return None


def _inherits_any(node: ast.ClassDef | None, roots: frozenset[str]) -> bool:
    """Return whether ``node``'s inheritance chain reaches one of ``roots``.

    Resolved from the repository's own source instead of a hand-maintained list
    of intermediate classes. A subclass of a subclass is the common shape here,
    and a list can only name the levels its author happened to see: every new
    intermediate base had to be added by hand or its subclasses silently lost
    the transport they inherit.

    Args:
        node: The class to start from.
        roots: Base class names that mean "inherits this capability".

    Returns:
        Whether any ancestor is one of ``roots``. An unresolvable ancestor is not
        treated as a match -- an unknown base proves nothing either way, and
        guessing would let an external base claim a transport it never supplies.

    """
    seen: set[str] = set()
    pending = [_base_name(node)] if node is not None else []
    while pending:
        current = pending.pop()
        if not current or current in seen:
            continue
        seen.add(current)
        if current in roots:
            return True
        parent = _module_defining_class(current)
        if parent is not None:
            pending.append(_base_name(parent))
    return False


def _has_entrypoint(tree: ast.Module) -> bool:
    """Return whether the module or one of its bases defines a crawl entrypoint."""
    if any(
        isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)) and _is_entrypoint(node.name)
        for node in ast.walk(tree)
    ):
        return True
    # A base class supplies `run()`, so the subclass is a crawler too.
    return _inherits_any(_crawler_class(tree), _ENTRYPOINT_BASES)


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


#: Module-level httpx helpers that perform a request with no client object of
#: their own, so finding one means the module reaches httpx directly.
_HTTPX_REQUEST_FUNCS = frozenset(
    {"get", "post", "put", "patch", "delete", "head", "options", "request", "stream"},
)

#: httpx names that mean a transport, as opposed to one of its error types.
_HTTPX_CLIENT_CLASSES = frozenset({"AsyncClient", "Client"})


def _reaches_httpx_itself(tree: ast.Module) -> bool:
    """Return whether the module makes its own HTTP calls.

    Inheriting ``BaseHttpCrawler`` only makes a raw client *available*; it does
    not mean the crawler uses one. So the test is whether the module builds a
    client or calls a module-level httpx helper, not whether the word appears.

    The word is a poor proxy, and this gate has been wrong about it twice. A
    crawler that listed ``httpx.HTTPError`` among the exceptions it catches was
    reported as still reaching for httpx, so a fully migrated crawler read as
    half-converted forever -- the opposite of what a migration gate is for. The
    same string search also drove ``uses_crawl_result``, which missed crawlers
    that fully branch on ``CrawlOutcome`` without ever naming ``CrawlResult``.
    """
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not isinstance(func, ast.Attribute):
            continue
        if not isinstance(func.value, ast.Name) or func.value.id != "httpx":
            continue
        if func.attr in _HTTPX_CLIENT_CLASSES or func.attr in _HTTPX_REQUEST_FUNCS:
            return True
    return False


#: The typed result vocabulary a crawler imports to classify its outcome.
_RESULT_VOCABULARY = frozenset({"CrawlResult", "CrawlOutcome"})


def _imports_result_vocabulary(tree: ast.Module) -> bool:
    """Return whether the module classifies outcomes with the shared result types.

    Importing the vocabulary is the evidence: a crawler can only branch on
    ``result.outcome`` or compare against ``CrawlOutcome.SCHEMA_CHANGED`` if it
    imported them. Searching the source for the string ``CrawlResult`` missed
    crawlers that use ``CrawlOutcome`` alone, and reported them as untyped.
    """
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom) or node.module != "src.crawlers.result":
            continue
        if any(alias.name in _RESULT_VOCABULARY for alias in node.names):
            return True
    return False


def _looks_like_outcome_value(node: ast.ClassDef) -> bool:
    """Return whether a class carries an outcome and whether it can still change.

    Two members together, because either alone is too easy to hit by accident:
    a ``status`` field is a common name, and an ``is_terminal`` property could
    be about anything. Together they describe what this repository means by a
    typed outcome -- what happened, and whether retrying could still change it --
    which is exactly the distinction a crawler has to make before it decides
    between queueing a dead letter and calling a quiet source quiet.
    """
    names = {child.name for child in node.body if isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef)}
    annotated = {
        target.id
        for child in node.body
        if isinstance(child, ast.AnnAssign) and isinstance(child.target, ast.Name)
        for target in (child.target,)
    }
    return "status" in annotated and "is_terminal" in names


def _imports_page_outcome_vocabulary(tree: ast.Module) -> bool:
    """Return whether the module classifies a *page read* into a typed outcome.

    ``CrawlResult`` models an HTTP fetch. A crawler that drives a browser has no
    HTTP request to describe, so it cannot produce one -- ``kbo_event_crawler``
    states this explicitly and carries its own vocabulary instead. Reading only
    ``CrawlResult`` therefore reported a finished crawler as untyped and parked
    it at the top of the roadmap with the gap named, which is worse than not
    ranking it at all: an operator following the report is sent to redo work
    someone already finished.

    Detection is structural rather than a declared list. The crawler is followed
    into the vocabulary module it imports, and the class is accepted only if it
    carries both members that make an outcome worth having -- see
    :func:`_looks_like_outcome_value`. That keeps the gate honest without adding
    a registry that goes stale the moment a second browser crawler adopts one.
    """
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom) or node.module is None:
            continue
        if not node.module.startswith("src.crawlers."):
            continue
        vocabulary = _read_source(f"{node.module.replace('.', '/')}.py")
        if not vocabulary:
            continue
        try:
            vocabulary_tree = ast.parse(vocabulary)
        except SyntaxError:
            continue
        imported = {alias.name for alias in node.names}
        for candidate in vocabulary_tree.body:
            if not isinstance(candidate, ast.ClassDef) or candidate.name not in imported:
                continue
            if _looks_like_outcome_value(candidate):
                return True
    return False


def _resolve_transports(tree: ast.Module, crawler: ast.ClassDef | None, *, reaches_httpx: bool) -> frozenset[Transport]:
    """Return every transport a module reaches a source through.

    A crawler can be genuinely hybrid -- an API primary with a browser fallback --
    so this returns a set rather than picking a winner. Detection walks the AST so
    a mention in a comment or a URL cannot register as a transport.

    An inherited transport is credited only when the ancestry actually reaches the
    base that supplies it, which is why this takes the class node rather than its
    name: the name alone cannot distinguish a direct ``BaseHttpCrawler`` subclass
    from one that inherits the same client through an intermediate base.
    """
    found: set[Transport] = set()
    for node in ast.walk(tree):
        transport = _transport_of_node(node)
        if transport is not None:
            found.add(transport)
    if reaches_httpx and _inherits_any(crawler, _HTTP_BASES):
        found.add(Transport.RAW_HTTPX)
    if _inherits_any(crawler, _PLAYWRIGHT_BASES):
        found.add(Transport.PLAYWRIGHT)
    return frozenset(found)


def _iter_source_files() -> Iterator[Path]:
    """Yield every Python source under ``src/`` except the crawlers themselves."""
    for path in sorted(SRC_ROOT.rglob("*.py")):
        if CRAWLER_DIR in path.parents or path.parent == CRAWLER_DIR:
            continue
        yield path


def _imported_model_names(source: str) -> set[str]:
    """Return the ORM model names a module imports.

    Restricted to names that are declared classes under ``src/models``. Those
    modules also export module-level constants -- ``crawl_execution`` defines
    ``RUN_STATUS_FAILED`` beside ``CrawlExecutionRun`` -- and a constant is not a
    table. Counting one put the movement crawler in charge of readers who read
    the run ledger, not its output.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return set()
    names: set[str] = set()
    known = _model_class_names()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and "models" in node.module:
            names.update(alias.name for alias in node.names if alias.name in known)
    return names


@lru_cache(maxsize=1)
def _model_reader_counts() -> dict[str, int]:
    """Return, per model name, how many non-crawler modules import it.

    Cached because it walks the whole source tree, and :func:`build_matrix`
    scans every crawler against it.
    """
    counts: dict[str, int] = {}
    for path in _iter_source_files():
        source = path.read_text(encoding="utf-8", errors="ignore")
        for name in _imported_model_names(source):
            counts[name] = counts.get(name, 0) + 1
    return counts


@lru_cache(maxsize=1)
def _crawler_caller_models() -> dict[str, frozenset[str]]:
    """Return, per crawler, the models read by the code that calls it.

    Most crawlers only fetch -- the pipeline step or service that invoked them
    owns the write, which is the same delegation :data:`DELEGATED_CAPABILITIES`
    records for the reliability axes. Following the callers rather than the
    crawler is therefore the only way to see what a crawler feeds: the PBP
    crawler names no model at all, yet the tables it fills are read by dozens of
    modules.

    Args:
        None.

    Returns:
        A crawler module name mapped to the model names its callers import.

    """
    by_crawler: dict[str, set[str]] = {}
    for path in _iter_source_files():
        source = path.read_text(encoding="utf-8", errors="ignore")
        models = _imported_model_names(source)
        for node in ast.walk(ast.parse(source) if source else ""):
            if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("src.crawlers."):
                crawler = node.module.rsplit(".", 1)[-1]
                by_crawler.setdefault(crawler, set()).update(models)
    return {crawler: frozenset(models) for crawler, models in by_crawler.items()}


@lru_cache(maxsize=1)
def _model_reader_files() -> dict[str, frozenset[str]]:
    """Return, per model name, the set of non-crawler modules importing it.

    A set of paths rather than a tally, because the unit being counted is a
    reading module. Tallying per model and summing double-counted every file
    that imported several of a crawler's tables, which inflated the PBP crawler
    from 82 readers to 143 and made the number scale with table count instead of
    with how much depends on the crawler.
    """
    by_model: dict[str, set[str]] = {}
    for path in _iter_source_files():
        source = path.read_text(encoding="utf-8", errors="ignore")
        for name in _imported_model_names(source):
            by_model.setdefault(name, set()).add(str(path))
    return {name: frozenset(paths) for name, paths in by_model.items()}


def _repository_model_names(source: str) -> set[str]:
    """Return the tables the repositories a source imports write.

    Resolved through each repository's body rather than through its name, so a
    crawler that persists through ``BroadcastRepository`` is scored against
    ``GameBroadcast`` -- the table that class actually constructs.

    Args:
        source: The importing module's source.

    Returns:
        Model names written by the repositories it imports.

    """
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return set()
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and "repositories" in node.module:
            for alias in node.names:
                if alias.name.endswith("Repository"):
                    names |= _repository_tables(alias.name)
    return names


@lru_cache(maxsize=1)
def _crawler_written_models() -> dict[str, frozenset[str]]:
    """Return, per crawler, the tables it is responsible for filling.

    Resolved from two sources, and only the second is approximate.

    A crawler that persists itself names its tables through the repositories it
    imports, which is direct evidence. Most crawlers only fetch: the pipeline
    step that called them owns the write, so the tables are read from that
    caller. A caller driving several crawlers cannot be split by its imports
    alone -- ``advanced_daily_steps`` imports the fielding and baserunning models
    and calls both -- so the caller's own call sites are used to tell which
    result reaches which repository. Where that is still ambiguous the crawler
    scores nothing rather than inheriting a neighbour's readers.

    Returns:
        A crawler module name mapped to the tables it writes.

    """
    caller_files: dict[str, list[Path]] = {}
    for path in _iter_source_files():
        tree = _parse_or_none(path)
        if tree is None:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("src.crawlers."):
                caller_files.setdefault(node.module.rsplit(".", 1)[-1], []).append(path)

    written: dict[str, set[str]] = {}
    for module in discover_modules():
        own = _repository_model_names(_module_source(module)) | _imported_model_names(_module_source(module))
        own |= _tables_behind_own_save_flag(module)
        attributed: set[str] = set()
        for caller in caller_files.get(module, ()):
            attributed |= _models_written_beside(caller, module)
            # A caller may also own the write for a crawler that only parses.
            # The PBP crawler hands back events and the recovery engine persists
            # them, so no result variable ever reaches a repository on the
            # crawler's behalf -- and the tables it fills are read by the
            # readiness gate, the SLA tracker and the RAG index.
            attributed |= _models_owned_by(caller, module)
        # The run ledger a crawler records is not the output it fetches. It says
        # the crawl happened, not what it produced, so it never displaces a table
        # a caller was found persisting on the crawler's behalf.
        written[module] = attributed or own
    return {module: frozenset(models) for module, models in written.items()}


def _tables_behind_own_save_flag(module: str) -> set[str]:
    """Return the tables a crawler writes when its own ``save`` flag is set.

    A crawler may persist itself rather than hand rows to a caller. The schedule
    crawler takes ``save=True`` and calls ``save_schedule_games`` from inside its
    own run ledger, so no caller passes it a result and the call-side walk found
    nothing -- the schedule crawler scored zero for the table it fills every
    morning.

    Confined to the guarded branch: a crawler that calls a writer somewhere else
    for an unrelated purpose should not claim its tables.

    Args:
        module: The crawler module name.

    Returns:
        Model names written under the save flag.

    """
    path = CRAWLER_DIR / f"{module}.py"
    tree = _parse_or_none(path)
    if tree is None:
        return set()
    known = _model_class_names()
    tables: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.If) or "save" not in ast.unparse(node.test):
            continue
        for statement in node.body:
            body = ast.unparse(statement)
            for name in re.findall(r"\b(\w+)\(", body):
                if name in known:
                    tables.add(name)
            for call in ast.walk(statement):
                if not isinstance(call, ast.Call):
                    continue
                called = ast.unparse(call.func).rsplit(".", 1)[-1]
                target = _file_for_imported_symbol(path, called)
                function = _function_body(target, called) if target else None
                if function is None:
                    continue
                tables |= _models_in_write_body(_with_local_helpers(target, function), called)
    return tables


def _is_write_call(name: str) -> bool:
    """Return whether a call name reads as persisting the rows it is handed.

    Verb-based, covering the three families this repository uses: ``persist``,
    ``save`` and ``upsert``. The row-replacement helpers -- ``_replace_records_for_side``,
    ``_replace_pregame_lineups`` -- write too, and the pregame writer reaches its
    lineups only through them. Matching a prefix instead left every replaced table
    invisible, which is the same miss as scoring the writer zero.
    """
    lowered = name.lower()
    return any(verb in lowered for verb in ("persist", "save", "upsert", "insert", "replace", "write", "store"))


def _models_written_by(
    caller: Path, call: ast.Call, *, depth: int = 0, exclude_standalone_rows: set[str] | None = None
) -> set[str]:
    """Return the tables a write call reaches, following the named function.

    Two hops, and the bound is what keeps the answer attributable. One hop
    follows the call to the function it names, which is where ``live_crawler``
    reaches ``save_relay_data`` in ``game_repository``. The second follows a
    *delegated write inside a resolved writer*: the pregame writer aggregates
    context and then hands the rows to ``save_pregame_lineups``, so stopping
    after one hop found a body that names no table and scored the preview
    crawler zero for the lineups it fills. Beyond that the call graph stops
    being worth following for a ranking heuristic -- a write three modules away
    is shared by too many crawlers to attribute, and an unattributable crawler
    scores nothing rather than inheriting everyone else's readers.

    Args:
        caller: The file making the call.
        call: The call node itself.
        depth: How many write hops have been followed.
        exclude_standalone_rows: Parameter names carrying rows the crawler's
            fetch produced, used to tell a handed row from one the writer builds.

    Returns:
        Model names the call is responsible for writing.

    """
    name = ast.unparse(call.func).rsplit(".", 1)[-1]
    # A method reached through a repository variable is not a module-level
    # function, so the file search below found nothing for it.
    # ``repo.save_daily_rosters(chunk)`` names the class in the receiver's
    # binding, and that class's body is where the table is written.
    repository_tables = _repository_method_tables(_repository_owning(call, set(), caller), _method_called(call))
    if repository_tables:
        return repository_tables
    local = _function_body(caller, name)
    if local:
        # A local helper is part of the caller's own body, so the value handed to
        # it arrives as a parameter rather than as a name. The preview batch
        # returns `previews` from the crawl and writes them in
        # `_save_preview_contexts`, so following only the outer call stopped one
        # frame short of the actual `save_pregame_lineups` row write.
        return _write_body_tables(caller, _with_local_helpers(caller, local), name, depth, exclude_standalone_rows)
    # The caller's own import says which module the name came from. Resolving by
    # a repository-wide search instead threw that away, and a name defined twice
    # -- ``save_relay_data`` exists in both ``game_relay`` and
    # ``relay_repository`` -- became unattributable even though the caller had
    # already named the module it meant.
    target = _file_for_imported_symbol(caller, name)
    if target is not None:
        body = _function_body(target, name)
        if body:
            return _write_body_tables(target, _with_local_helpers(target, body), name, depth, exclude_standalone_rows)
    fallback = _file_defining_symbol(name)
    if fallback is not None:
        body = _function_body(fallback, name)
        if body:
            return _write_body_tables(fallback, _with_local_helpers(fallback, body), name, depth)
    return set()


def _write_body_tables(module: Path, body: str, name: str, depth: int, exclude: set[str] | None = None) -> set[str]:
    """Return the tables one writer's body names, following a delegated write.

    The body is read first, because a writer that builds its own rows is the
    common case and its own evidence is the strongest. A writer that names no
    table may be a coordinator rather than an empty one -- the pregame writer
    aggregates context and then hands the rows to ``save_pregame_lineups`` --
    and following that call is the same evidence one frame further in, not a
    weaker claim: the resolved module's own import is what names the writer, so
    the attribution stays unambiguous.

    Args:
        module: The file the writer was resolved to.
        body: The writer's body, with its same-file helpers.
        name: The writer's name.
        depth: How many write hops have been followed.
        exclude: Parameters carrying rows the caller's crawler produced; a row
            built without them belongs to the writer rather than the crawler.

    Returns:
        Model names the writer is responsible for.

    """
    tables = _models_in_write_body(body, name)
    if exclude:
        tables -= _rows_built_alone(body, name, exclude)
    if tables or depth >= _MAX_WRITE_HOPS:
        return tables
    return _delegated_write_tables(module, body, depth)


def _rows_built_alone(body: str, name: str, parameters: set[str]) -> set[str]:
    """Return the rows a writer constructs without reference to its parameters.

    A writer handed rows may still build a row of its own -- the recovery engine
    looks up the parent game and creates a stub when the row is missing, from
    its own context and literals. Such a row is the writer's, not the crawler's:
    nothing the crawler produced is involved in building it, so it must not be
    credited to the fetch that reached this function.
    """
    try:
        tree = ast.parse(body)
    except SyntaxError:
        return set()
    writer = next(
        (
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and node.name == name
        ),
        None,
    )
    if writer is None:
        return set()
    alone: set[str] = set()
    for statement in ast.walk(writer):
        if not isinstance(statement, ast.stmt) or not _constructs_a_model(statement):
            continue
        if any(
            isinstance(child, ast.Name) and child.id in parameters
            for inner in ast.walk(statement)
            for child in ast.walk(inner)
        ):
            continue
        alone |= _models_in_statement(statement)
    return alone


def _constructs_a_model(statement: ast.stmt) -> bool:
    """Return whether a statement builds a model row."""
    return any(
        isinstance(node, ast.Call) and ast.unparse(node.func).rsplit(".", 1)[-1] in _model_class_names()
        for node in ast.walk(statement)
    )


def _models_in_statement(statement: ast.stmt) -> set[str]:
    """Return the model classes a statement constructs."""
    known = _model_class_names()
    return {
        ast.unparse(node.func).rsplit(".", 1)[-1]
        for node in ast.walk(statement)
        if isinstance(node, ast.Call) and ast.unparse(node.func).rsplit(".", 1)[-1] in known
    }


def _parameters_handed_to(caller: Path, call: ast.Call) -> set[str]:
    """Return the writer's own parameters, which are what the rows arrive as.

    Any parameter a writer body references is something it was given, so the
    distinction the writer draws is between a statement that mentions what it
    was handed and one that builds a row without mentioning it. The signature
    already answers that; no argument inspection is needed.
    """
    name = ast.unparse(call.func).rsplit(".", 1)[-1]
    for path in (caller, *_import_sources(caller, name)):
        body = _function_body(path, name)
        if body is None:
            continue
        return {
            argument.arg
            for argument in (*body.args.posonlyargs, *body.args.args, *body.args.kwonlyargs)
            if isinstance(argument, ast.arg)
        } - {"self", "cls"}
    return set()


def _import_sources(caller: Path, name: str) -> list[Path]:
    """Return the files a caller could have imported a writer from."""
    found = _file_for_imported_symbol(caller, name)
    return [found] if found is not None else []


def _delegated_write_tables(module: Path, body: str, depth: int) -> set[str]:
    """Return the tables the write calls inside a resolved writer reach.

    Args:
        module: The file the writer was resolved to, which is what makes the
            inner call's own import resolvable.
        body: The writer's body, with its same-file helpers.
        depth: Write hops followed so far, used to bound the recursion.

    Returns:
        Model names reached from the writer's delegated calls.

    """
    try:
        tree = ast.parse(body)
    except SyntaxError:
        return set()
    tables: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        called = ast.unparse(node.func).rsplit(".", 1)[-1]
        if not _is_write_call(called):
            continue
        tables |= _models_written_by(module, node, depth=depth + 1)
    return tables


def _file_for_imported_symbol(caller: Path, symbol: str) -> Path | None:
    """Return the file that actually defines ``symbol`` for ``caller``.

    The caller's import is the entry point, but repositories re-export: the
    preview batch imports ``save_pregame_lineups`` from ``game_repository``,
    which only re-exports it from ``game_save``, where the function lives. Stopping
    at the re-exporting module finds the import and not the writer.

    Args:
        caller: The file making the call.
        symbol: The function name being called.

    Returns:
        The defining file, or ``None`` when the chain does not resolve.

    """
    tree = _parse_or_none(caller)
    if tree is None:
        return None
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom) or not node.module:
            continue
        if not any(alias.name == symbol for alias in node.names):
            continue
        return _resolve_export(symbol, node.module, depth=0)
    return None


def _resolve_export(symbol: str, module_path: str, depth: int) -> Path | None:
    """Return the file defining ``symbol``, following re-exports.

    Args:
        symbol: The name to locate.
        module_path: The dotted module the name was imported from.
        depth: How many re-export hops have been followed.

    Returns:
        The defining file, or ``None`` when the name is unresolvable.

    """
    if depth > _MAX_EXPORT_HOPS:
        # Each hop multiplies the candidates a name could refer to. Past this
        # point the chain is no longer evidence of one writer, and continuing
        # would pick a definition the caller never had in mind.
        return None
    path = SRC_ROOT / f"{module_path[len('src.') :].replace('.', '/')}.py"
    if not path.is_file():
        return None
    tree = _parse_or_none(path)
    if tree is None:
        return None
    if any(isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and node.name == symbol for node in tree.body):
        return path
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and any(alias.name == symbol for alias in node.names):
            return _resolve_export(symbol, node.module, depth + 1)
    return None


def _with_local_helpers(path: Path, function: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
    """Return a writer's body together with the helpers it delegates to.

    A save function rarely builds its own rows. ``save_relay_data`` hands the
    events to ``_build_relay_event_rows`` and the raw rows to
    ``_build_relay_pbp_rows``, and both helpers construct the model the function
    is named for. Reading only the entry point finds neither.

    Scoped to helpers defined in the same file. A call into another module is
    the ambiguous multi-hop case that stops attribution rather than guessing at
    it.

    Args:
        path: The file defining the writer.
        function: The writer's definition.

    Returns:
        Unparsed source of the writer plus its same-file helpers.

    """
    tree = _parse_or_none(path)
    if tree is None:
        return ast.unparse(function)
    helpers = {node.name: node for node in tree.body if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)}
    collected: dict[str, object] = {}
    pending = [function]
    while pending:
        current = pending.pop()
        if current.name in collected:
            continue
        collected[current.name] = current
        for node in ast.walk(current):
            if isinstance(node, ast.Call):
                callee = ast.unparse(node.func).rsplit(".", 1)[-1]
                helper = helpers.get(callee)
                if helper is None and _is_write_call(callee):
                    # Only for a writer, and only one hop. Following any call out
                    # of the file pulled in whatever the delegate happened to
                    # touch: the preview writer's table set grew to include the
                    # metadata and summary rows a game-detail writer produces.
                    helper = _imported_function(path, callee)
                if helper is not None and helper.name not in collected:
                    pending.append(helper)
    return "\n".join(ast.unparse(node) for node in collected.values())


def _imported_function(caller: Path, symbol: str) -> ast.FunctionDef | ast.AsyncFunctionDef | None:
    """Return a writer function ``caller`` delegates to in another module."""
    if not _is_write_call(symbol):
        return None
    target = _file_for_imported_symbol(caller, symbol)
    return _function_body(target, symbol) if target else None


def _models_in_write_body(body: str, name: str) -> set[str]:
    """Return the models a save function constructs.

    Construction is the test, and only construction. A writer reads the tables
    it needs -- ``save_relay_data`` runs ``session.query(Game)`` to find the
    parent row before inserting events -- and counting a read as a write hands
    the crawler whatever table the writer happened to look up. ``Game`` is read
    by every module that touches a game, so crediting it to the play-by-play
    crawler inflated its reader count and pushed the rows the crawler actually
    produces down the ranking.

    Args:
        body: The unparsed function source.
        name: The function's name, retained for the signature contract.

    Returns:
        Model names the function instantiates.

    """
    del name  # the body alone carries the evidence
    known = _model_class_names()
    try:
        tree = ast.parse(body)
    except SyntaxError:
        return set()
    constructed = _constructed_models(tree, known)
    return constructed | _tables_named_as_arguments(tree, known) | _tables_passed_to_writers(tree, known)


def _constructed_models(tree: ast.Module, known: frozenset[str]) -> set[str]:
    """Return the models a writer instantiates or delegates to a repository."""
    constructed: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        called = ast.unparse(node.func).rsplit(".", 1)[-1]
        if called in known:
            if not _is_a_read(node, called):
                constructed.add(called)
        elif called.endswith("Repository"):
            constructed |= _repository_tables(called)
    return constructed


def _tables_passed_to_writers(tree: ast.Module, known: frozenset[str]) -> set[str]:
    """Return the models named as the target of a bulk write.

    SQLAlchemy's dialect inserts name the table as a value rather than a
    constructor: ``pg_insert(TeamDailyRoster).values(rows)``. The roster repository
    writes its rows that way, so a constructor-only scan saw no table and the
    crawler scored zero for the table it fills every morning.
    """
    named: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        callee = ast.unparse(node.func).rsplit(".", 1)[-1]
        if not (callee.endswith("_insert") or callee in {"insert", "upsert", "bulk_save_objects"}):
            continue
        for argument in node.args:
            named |= {inner.id for inner in ast.walk(argument) if isinstance(inner, ast.Name) and inner.id in known}
    return named


def _tables_named_as_arguments(tree: ast.Module, known: frozenset[str]) -> set[str]:
    """Return the models handed to a persisting call rather than constructed.

    Rows are not always built with ``Model(...)``. The pregame writer replaces
    them through a helper that takes the table as a value --
    ``_replace_records_for_side(session, RecordKey(GameLineup, ...), rows)`` --
    so a constructor-only scan found nothing for a table the writer replaces
    every night. A model named as an argument to a call that persists rows is
    the same claim, and the scan still rejects plain reads: those appear as
    ``session.query(Model)`` and are arguments to nothing.

    Args:
        tree: The writer's parsed body.
        known: Every ORM class name.

    Returns:
        Model names passed to a persisting call.

    """
    named: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        callee = ast.unparse(node.func).rsplit(".", 1)[-1]
        if callee in known or not _is_write_call(callee):
            continue
        for argument in node.args:
            named |= {inner.id for inner in ast.walk(argument) if isinstance(inner, ast.Name) and inner.id in known}
    return named


def _is_a_read(call: ast.Call, model: str) -> bool:
    """Return whether a model call only reads rather than builds a row.

    ``session.query(Game).filter(...).first()`` is how every writer finds the
    parent row it needs before inserting anything. Counting that as a write gave
    the crawler the whole ``Game`` table -- read by every module that touches a
    game -- which is how the play-by-play crawler ended up ranked above the
    tables it actually fills.

    A ``Model(...)`` constructor is never a read, and neither is a model handed
    to a persisting call: those name the row being written.

    Args:
        call: The call node naming the model.
        model: The model name.

    Returns:
        Whether the call only reads.

    """
    inner = call.func
    if not isinstance(inner, ast.Attribute):
        return False
    if inner.attr == "query":
        return True
    del model  # the attribute alone identifies the read
    return False


@lru_cache(maxsize=1)
def _repository_tables(repository: str) -> frozenset[str]:
    """Return the tables a repository class actually constructs.

    The class name is a naming convention, not a declaration.
    ``BroadcastRepository`` writes ``GameBroadcast``, not ``Broadcast`` -- a name
    that is not an ORM class at all -- so stripping the suffix produced a table
    that exists nowhere in the schema and scored the crawler against readers who
    could never appear. The body of the class is where the table is named.

    This is the class-wide view, for call sites that do not say which method ran;
    :func:`_repository_method_tables` narrows it to the one method called.

    Args:
        repository: The repository class name.

    Returns:
        Model names the class instantiates.

    """
    known = _model_class_names()
    path = _file_defining_class(repository)
    if path is None:
        return frozenset()
    try:
        tree = ast.parse(path.read_text(encoding="utf-8", errors="ignore"))
    except (OSError, SyntaxError):
        return frozenset()
    for node in tree.body:
        if not isinstance(node, ast.ClassDef) or node.name != repository:
            continue
        return frozenset(_models_in_methods(node, {}, known))
    return frozenset()


def _repository_method_tables(repository: str, method: str) -> frozenset[str]:
    """Return the tables one repository method constructs.

    A repository is not one writer. ``PlayerRepository`` constructs ``Player``,
    ``PlayerIdentity`` and ``PlayerMovement`` in three different methods, so
    crediting a caller with the whole class handed it tables it never wrote: the
    profile collector calls ``upsert_player_profile`` alone and was scored against
    identity and movement rows belonging to other callers.

    The method's own helpers count, because that is the call's real path --
    ``upsert_player_profile`` builds its row through ``_get_or_create_player``.

    Args:
        repository: The repository class name.
        method: The method name; empty for the whole class.

    Returns:
        Model names the method instantiates.

    """
    known = _model_class_names()
    path = _file_defining_class(repository)
    if path is None:
        return frozenset()
    tree = _parse_or_none(path)
    if tree is None:
        return frozenset()
    for node in tree.body:
        if not isinstance(node, ast.ClassDef) or node.name != repository:
            continue
        methods = {
            inner.name: inner for inner in node.body if isinstance(inner, ast.FunctionDef | ast.AsyncFunctionDef)
        }
        if method and method not in methods:
            # Naming a method the class does not have means the call site was
            # misread. The class-wide view is more honest than an empty answer,
            # which reads as "this writes nothing".
            return _repository_method_tables(repository, "")
        return frozenset(_models_in_methods(methods.get(method, node), methods, known))
    return frozenset()


def _models_in_methods(
    root: ast.AST, methods: dict[str, ast.FunctionDef | ast.AsyncFunctionDef], known: frozenset[str]
) -> set[str]:
    """Return the models a method and its own helpers instantiate."""
    collected: set[str] = set()
    pending: list[ast.AST] = [root]
    seen: set[str] = set()
    while pending:
        current = pending.pop()
        key = getattr(current, "name", "")
        if key:
            if key in seen:
                continue
            seen.add(key)
        for inner in ast.walk(current):
            if not isinstance(inner, ast.Call):
                continue
            called = ast.unparse(inner.func).rsplit(".", 1)[-1]
            if called in known:
                if not _is_a_read(inner, called):
                    collected.add(called)
            elif called in methods:
                pending.append(methods[called])
    # A dialect insert names its target as a value, so the loop above -- which
    # looks for constructors and repository calls -- never sees it.
    collected |= _tables_passed_to_writers(root, known)
    # ``super().__init__(session, TeamSeasonBatting)`` names the model through a
    # call argument, and the loop above only looks for the model as a callee.
    collected |= _models_named_as_arguments(root, known)
    return collected


def _models_named_as_arguments(root: ast.AST, known: frozenset[str]) -> set[str]:
    """Return the models a class passes as arguments to its own base.

    A repository that names its model in ``super().__init__`` declares it there
    just as much as one that names it in a constructor, and a scan that stops
    at the callee never sees it.
    """
    named: set[str] = set()
    for node in ast.walk(root):
        if not isinstance(node, ast.Call):
            continue
        for argument in node.args:
            named.update(inner.id for inner in ast.walk(argument) if isinstance(inner, ast.Name) and inner.id in known)
    return named


@lru_cache(maxsize=1)
def _class_declarations() -> dict[str, tuple[Path, ...]]:
    """Return every ``src/`` file grouped by the class it declares.

    One pass, because resolving a name by scanning the tree per lookup turns a
    scan of thirty crawlers into thousands of full-tree reads.
    """
    declarations: dict[str, list[Path]] = {}
    for path in SRC_ROOT.rglob("*.py"):
        source = path.read_text(encoding="utf-8", errors="ignore")
        for name in re.findall(r"^class (\w+)\b", source, re.MULTILINE):
            declarations.setdefault(name, []).append(path)
    return {name: tuple(paths) for name, paths in declarations.items()}


def _file_defining_class(class_name: str) -> Path | None:
    """Return the single file under ``src/`` declaring ``class_name``.

    Returns ``None`` when the name is declared more than once, since a caller
    reaching this by bare name cannot say which it meant.
    """
    matches = _class_declarations().get(class_name, ())
    return matches[0] if len(matches) == 1 else None


@lru_cache(maxsize=1)
def _model_class_names() -> frozenset[str]:
    """Return every ORM class name declared under ``src/models``."""
    names: set[str] = set()
    for path in (SRC_ROOT / "models").glob("*.py"):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8", errors="ignore"))
        except (OSError, SyntaxError):
            continue
        names.update(node.name for node in tree.body if isinstance(node, ast.ClassDef))
    return frozenset(names)


@lru_cache(maxsize=1)
def _function_declarations() -> dict[str, tuple[Path, ...]]:
    """Return every ``src/`` file grouped by the module-level function it defines.

    One pass for the same reason as :func:`_class_declarations`: a per-lookup tree
    scan makes each symbol resolution cost a full read of ``src/``.
    """
    declarations: dict[str, list[Path]] = {}
    for path in SRC_ROOT.rglob("*.py"):
        source = path.read_text(encoding="utf-8", errors="ignore")
        for name in re.findall(r"^(?:async )?def (\w+)\b", source, re.MULTILINE):
            declarations.setdefault(name, []).append(path)
    return {name: tuple(paths) for name, paths in declarations.items()}


def _file_defining_symbol(symbol: str) -> Path | None:
    """Return the source file defining ``symbol``.

    Returns ``None`` when the name is ambiguous -- defined more than once under
    ``src/`` -- because the callers that reach this do so by bare name.
    ``save_relay_data`` is defined twice, in ``game_relay`` and in
    ``relay_repository``, and they persist different things. Following the wrong
    one credits a crawler with tables the real writer never touches, which is
    worse than crediting it with none: the number orders the roadmap, so an
    inflated figure sends an operator after the wrong crawler while looking
    deliberate.
    """
    matches = _function_declarations().get(symbol, ())
    return matches[0] if len(matches) == 1 else None


def _function_body(path: Path, name: str) -> ast.FunctionDef | ast.AsyncFunctionDef | None:
    """Return the definition of ``name`` in ``path``, if present."""
    try:
        tree = ast.parse(path.read_text(encoding="utf-8", errors="ignore"))
    except (OSError, SyntaxError):
        return None
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and node.name == name:
            return node
    return None


def _produced_names(tree: ast.Module, symbols: set[str]) -> set[str]:
    """Return the variables holding a crawler's output.

    Both shapes a driver takes. The obvious one is a static call, ``crawl(...)``.
    The shape that actually appears is an instance: ``crawler = PreviewCrawler()``
    binds the class to a name, and every later call goes through that name --
    ``previews = await crawler.crawl_preview_for_date(...)``. Matching the class
    name alone found no result at all in either the profile or the preview path,
    and both crawlers scored zero while feeding real tables.

    Resolving the receiver needs the binding, so instances are tracked as they
    are assigned and a method call counts only when its receiver is one of them.

    Args:
        tree: The caller's parsed module.
        symbols: Class names imported from the crawler module.

    Returns:
        Variable names assigned from a crawler call.

    """
    produced: set[str] = set()
    instances: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign | ast.AnnAssign) or node.value is None:
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        names = {target.id for target in targets if isinstance(target, ast.Name)}
        for inner in ast.walk(node.value):
            if not isinstance(inner, ast.Call):
                continue
            if _call_constructs_crawler(inner, symbols):
                # The constructor is where the instance begins, not where rows
                # appear. ``_call_uses_crawler`` matches it too -- the class name
                # is one of the symbols -- so the assignment target landed in the
                # result set, and a caller that only drives a crawler looked like
                # one that had bound output and then lost it. That phantom gap was
                # reported against every crawler such a driver runs.
                instances |= names
            elif _call_uses_crawler(inner, instances, symbols):
                produced |= names
    return produced


def _call_uses_crawler(call: ast.Call, instances: set[str], symbols: set[str]) -> bool:
    """Return whether a call reaches a crawler class or one of its instances.

    Args:
        call: The call to judge.
        instances: Variables bound to a crawler object.
        symbols: Class names imported from the crawler module.

    Returns:
        Whether the call goes through a crawler.

    """
    func = call.func
    if isinstance(func, ast.Attribute):
        return isinstance(func.value, ast.Name) and func.value.id in instances
    return isinstance(func, ast.Name) and func.id in symbols


def _call_constructs_crawler(call: ast.Call, symbols: set[str]) -> bool:
    """Return whether a call instantiates one of the crawler's classes."""
    return isinstance(call.func, ast.Name) and call.func.id in symbols


def _handed_result(call: ast.Call, produced: set[str]) -> bool:
    """Return whether a call is handed one of the crawler's own results.

    Args:
        call: The call node.
        produced: Names bound to a crawler result.

    Returns:
        Whether the call receives one of them.

    """
    return any(isinstance(child, ast.Name) and child.id in produced for child in ast.walk(call))


def _models_written_via_helper(caller: Path, call: ast.Call, produced: set[str]) -> set[str]:
    """Return the tables a local helper writes for a crawler result.

    A caller often keeps the write one frame away from the crawl: the preview
    batch fetches its rows and then calls ``_save_preview_contexts(previews)``,
    which is where ``save_pregame_lineups`` actually runs. Resolving only the
    outer call finds the helper and stops, and the crawler scores zero for a
    table the batch fills every night.

    Args:
        caller: The file making the call.
        call: The call node to the helper.
        produced: Names bound to a crawler result.

    Returns:
        Model names the helper writes from that result.

    """
    tree = _parse_or_none(caller)
    if tree is None:
        return set()
    helpers = {node.name: node for node in tree.body if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)}
    helper = helpers.get(ast.unparse(call.func).rsplit(".", 1)[-1])
    if helper is None:
        return set()
    carried = _renamed_parameters(helper, call, produced)
    if not carried:
        return set()
    models: set[str] = set()
    for inner in ast.walk(helper):
        if not isinstance(inner, ast.Call):
            continue
        name = ast.unparse(inner.func).rsplit(".", 1)[-1]
        if not (name.endswith("Repository") or _is_write_call(name)):
            continue
        if not _handed_result(inner, carried):
            continue
        if name.endswith("Repository"):
            models |= _repository_method_tables(_repository_owning(inner, carried, caller), _method_called(inner))
        else:
            models |= _models_written_by(caller, inner)
    return models


def _renamed_parameters(
    function: ast.FunctionDef | ast.AsyncFunctionDef, call: ast.Call, produced: set[str]
) -> set[str]:
    """Return the helper's parameters that were handed a crawler result.

    Read off the call rather than by matching the two name sets in order. The
    preview batch passes its result first and the target date second, and pairing
    the names positionally labelled ``target_date`` as the crawled rows -- the
    value that reached the writer was then traced as something the crawler never
    produced. Only the argument index says which parameter received which value.

    Args:
        function: The helper being called.
        call: The call site.
        produced: Names bound to a crawler result.

    Returns:
        Parameter names inside the helper carrying the crawler's output.

    """
    positional = [arg.arg for arg in (*function.args.posonlyargs, *function.args.args)]
    carried: set[str] = set()
    for index, argument in enumerate(call.args):
        if index < len(positional) and _handed_result(argument, produced):
            carried.add(positional[index])
    keyword_only = {arg.arg for arg in function.args.kwonlyargs}
    for keyword in call.keywords:
        if keyword.arg in keyword_only and _handed_result(keyword.value, produced):
            carried.add(keyword.arg)
    return _expanded_carried(function, carried)


def _expanded_carried(tree: ast.AST, carried: set[str]) -> set[str]:
    """Return ``carried`` closed over repackaging and iteration.

    Two ways a crawled value changes name on its way to a writer, and both were
    invisible to the walk that stopped at the crawl's own result.

    Iteration: a caller walks the rows and writes each one under a new name --
    the preview batch loops ``for preview in previews``. Repackaging: a caller
    rebuilds the rows before storing them, as the profile collector does when it
    fills a ``PlayerProfileParsed`` from the crawler's dict and hands *that* to
    the repository. In both cases the value the writer receives descends from the
    crawl while no argument names the crawler's result, so the write looked like
    someone else's and the crawler scored zero for a table it fills nightly.

    Scoped to the scope it was given. A module-wide closure walks every
    definition in the file, so a name introduced inside an unrelated function
    joined the set -- and in ``live_crawler`` that pulled in the relay and PBP
    fetches performed elsewhere in the file, crediting the schedule crawler with
    the play-by-play tables. Derivation follows the value within one scope; a
    function is reached only by being called with it.

    Args:
        tree: The function or module to scan.
        carried: Names holding the crawler's output.

    Returns:
        The fixed point of the derivation.

    """
    names = set(carried)
    while True:
        fresh = _names_derived_in(tree, names)
        if fresh <= names:
            return names
        names |= fresh


def _names_derived_in(tree: ast.AST, names: set[str]) -> set[str]:
    """Return the names bound from values already known to carry the output.

    Scoped to the statements of the scope being scanned. ``ast.walk`` descends
    into nested function definitions, so scanning a module also picked up names
    local to unrelated functions -- and in a module driving four crawlers that
    merged their results into one set.

    Args:
        tree: The scope to scan.
        names: Names already known to carry the crawler's output.

    Returns:
        Names bound from those values in this scope.

    """
    derived: set[str] = set()
    for node in _own_statements(tree):
        derived |= _assignment_targets(node, names)
        derived |= _iteration_targets(node, names)
    return derived


def _own_statements(tree: ast.AST) -> list[ast.AST]:
    """Return the nodes belonging to one scope, excluding nested definitions.

    Blocks are included: ``for preview in previews`` sits inside a ``with`` in the
    preview batch, and a listing of the scope's top-level statements alone would
    never reach it -- which is the loop that renames the crawled rows on their
    way to the writer.

    A comprehension or lambda is part of the expression that contains it, so only
    ``def`` and ``async def`` introduce a scope of their own.
    """
    if isinstance(tree, ast.Module):
        return [node for node in tree.body if not _defines_scope(node)]
    return [node for node in ast.walk(tree) if not _defines_scope(node)]


def _defines_scope(node: ast.AST) -> bool:
    """Return whether a node introduces a nested scope."""
    return isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef)


def _assignment_targets(node: ast.AST, names: set[str]) -> set[str]:
    """Return the names assigned from a carried value."""
    if not isinstance(node, ast.Assign | ast.AnnAssign) or node.value is None:
        return set()
    if not _handed_result(node.value, names):
        return set()
    targets = node.targets if isinstance(node, ast.Assign) else [node.target]
    return {target.id for target in targets if isinstance(target, ast.Name)}


def _iteration_targets(node: ast.AST, names: set[str]) -> set[str]:
    """Return the names bound to an element of a carried value."""
    if not isinstance(node, ast.For | ast.comprehension) or not _handed_result(node.iter, names):
        return set()
    return {child.id for child in ast.walk(node.target) if isinstance(child, ast.Name)}


def _models_committed_beside(caller: Path, symbols: set[str]) -> set[str]:
    """Return the tables a caller's own write functions say they commit.

    The path taken when the caller uses a crawler as a helper rather than as a
    fetcher. ``relay_recovery_engine`` calls ``PBPCrawler._format_base_string``
    while building relay rows and then persists those rows itself, logging
    "Committed N GameEvent rows and N GamePlayByPlay rows". There is no result
    variable to follow, so the writer's own account of its write is the only
    evidence available -- and it is checked against the function that does the
    writing, not the module's header.

    Args:
        caller: The file using the crawler.
        symbols: The class names imported from the crawler module.

    Returns:
        Model names the caller's write functions name.

    """
    tree = _parse_or_none(caller)
    if tree is None:
        return set()
    helpers_used = any(
        any(re.search(rf"\b{re.escape(symbol)}\.", ast.unparse(node)) for symbol in symbols) for node in ast.walk(tree)
    )
    if not helpers_used:
        return set()
    # Follow the rows the helper formats into the writer, rather than crediting
    # every model the writing function touches. The recovery engine calls the
    # PBP crawler's `_format_base_string` while building events, then creates a
    # `Game` stub because the parent row was missing, and only then saves. Reading
    # the whole writer made the stub look like crawler output -- and `Game` is the
    # most-read table in the project.
    produced = _rows_built_with(tree, symbols)
    if not produced:
        return set()
    found: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = ast.unparse(node.func).rsplit(".", 1)[-1]
        if not (name.endswith("Repository") or _is_write_call(name)):
            continue
        if not _handed_result(node, produced):
            continue
        if name.endswith("Repository"):
            found |= _repository_method_tables(_repository_owning(node, produced, caller), _method_called(node))
        else:
            # A writer that was handed rows may still build a row of its own
            # before saving them: the recovery engine looks the parent game up
            # and creates a stub when the row is missing, from its own context
            # and literals. That stub is not what the crawler fetched, and
            # crediting it handed the crawler the most-read table in the project.
            found |= _models_written_by(caller, node, exclude_standalone_rows=_parameters_handed_to(caller, node))
    return found


def _rows_built_with(tree: ast.Module, symbols: set[str]) -> set[str]:
    """Return the values assembled while a crawler's static helper is used.

    The path taken when the crawler is a helper rather than a fetcher: no result
    is bound to a name, but the helper shapes the rows a later write persists.

    Args:
        tree: The caller's module.
        symbols: Class names imported from the crawler module.

    Returns:
        Names assigned from an expression calling one of the crawler helpers.

    """
    produced: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign | ast.AnnAssign) or node.value is None:
            continue
        if not _calls_crawler_helper(node.value, symbols):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        produced.update(target.id for target in targets if isinstance(target, ast.Name))
    produced |= _rows_collected_with(tree, symbols)
    if not produced:
        return produced
    return _rows_propagated(tree, produced)


def _rows_propagated(tree: ast.Module, produced: set[str]) -> set[str]:
    """Close the set over every way a row reaches the writer in this module.

    Each rule answers a different hop, and none of them subsumes another: the
    rows are returned, unpacked alongside a sibling, handed to a helper as a
    parameter, reshaped by a call, or passed through a function that filters
    them -- and the writer's argument name is whichever name survived the last
    one. Applied once, the chain breaks wherever two hops meet. Applied to a
    fixed point, it follows them all, which is what the recovery engine's path
    from the crawler's formatter to ``save_relay_data`` requires.
    """
    names = set(produced)
    while True:
        grown = names | _returned_rows(tree, names) | _names_unpacked_with(tree, names)
        grown = grown | _rows_carried_into_a_helper(tree, grown)
        grown = grown | _rows_reshaped_by_a_call(tree, grown)
        grown = grown | _returned_by_a_called_helper(tree, grown)
        grown = _expanded_carried(tree, grown)
        if grown <= names:
            return names
        names = grown


def _names_unpacked_with(tree: ast.Module, produced: set[str]) -> set[str]:
    """Return the names a multi-value call assigns alongside a carried row.

    ``_normalize_sources`` returns the events together with the PBP rows and the
    source name, and the caller unpacks all four in one statement:
    ``kbo_events, naver_events, raw_pbp_rows, source_used = self._normalize...``.
    The rows the writer receives arrive in the same tuple as the crawler's, so
    tracking the first name alone left the others untraced -- and ``raw_pbp_rows``
    is the one ``save_relay_data`` writes.
    """
    unpacked: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Call):
            continue
        if not _returns_a_carried_name(node.targets, produced):
            continue
        for target in node.targets:
            if isinstance(target, ast.Tuple | ast.List):
                unpacked.update(element.id for element in ast.walk(target) if isinstance(element, ast.Name))
    return unpacked


def _returns_a_carried_name(targets: list[ast.expr], produced: set[str]) -> bool:
    """Return whether a multiple-assignment target names a carried row.

    The single-name case is already covered by :func:`_handed_result`, which
    reads the call's arguments. Here the rows come back *as* the call's result,
    so the question is whether any target name is already tracked.
    """
    return any(
        element.id in produced
        for target in targets
        if isinstance(target, ast.Tuple | ast.List)
        for element in ast.walk(target)
        if isinstance(element, ast.Name)
    )


def _returned_by_a_called_helper(tree: ast.Module, produced: set[str]) -> set[str]:
    """Return the names bound from a call that received carried rows.

    The recovery engine's last hop: ``canonical_events`` is what
    ``filter_new_events(kbo_events, ...)`` returns, and that is the name
    ``save_relay_data(events=canonical_events)`` is given. Reading only the
    arguments left the binding unreached, so the writer looked like it had been
    handed something nobody produced.
    """
    bound: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign | ast.AnnAssign) or node.value is None:
            continue
        if not _handed_result(node.value, produced):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        bound.update(target.id for target in targets if isinstance(target, ast.Name))
    return bound


def _rows_reshaped_by_a_call(tree: ast.Module, produced: set[str]) -> set[str]:
    """Return the rows a caller hands to a function and binds under a new name.

    The last hop before the writer, and a common one: the recovery engine takes
    the KBO events the PBP crawler shaped, passes them to ``filter_new_events``,
    and binds what comes back as ``canonical_events`` -- the name the persisting
    call actually receives. Tracing only assignments of the crawler's own output
    stopped one transformation before the write.
    """
    receiving = _parameters_holding_rows(tree, produced)
    if not receiving:
        return set()
    reshaping = {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id in receiving
    }
    if not reshaping:
        return set()
    return {
        target.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign | ast.AnnAssign) and node.value is not None
        for target in (node.targets if isinstance(node, ast.Assign) else [node.target])
        if isinstance(target, ast.Name)
        and any(
            isinstance(inner, ast.Call) and ast.unparse(inner.func).rsplit(".", 1)[-1] in reshaping
            for inner in ast.walk(node.value)
        )
    }


def _calls_crawler_helper(expression: ast.AST, symbols: set[str]) -> bool:
    """Return whether an expression calls a static helper of the crawler."""
    return any(
        isinstance(inner, ast.Call)
        and any(re.search(rf"\b{re.escape(symbol)}\.", ast.unparse(inner.func)) for symbol in symbols)
        for inner in ast.walk(expression)
    )


def _rows_collected_with(tree: ast.Module, symbols: set[str]) -> set[str]:
    """Return the containers a helper's output is appended to.

    The recovery engine does not assign the helper's rows; it puts them inside
    a dict literal it appends to a list. ``outs_before`` and the base string the
    crawler's own formatter produced end up as *values* of an event row rather
    than as a name anyone later reads, so tracing assignments found no result
    and the play-by-play tables were attributed to the run ledger alone.
    """
    collected: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        if node.func.attr not in {"append", "extend", "add", "update"}:
            continue
        if any(_calls_crawler_helper(argument, symbols) for argument in node.args):
            receiver = ast.unparse(node.func.value)
            if receiver:
                collected.add(receiver)
    if not collected:
        return collected
    return _expanded_carried(tree, collected)


def _returned_rows(tree: ast.Module, produced: set[str]) -> set[str]:
    """Return the names bound from a local helper that returns built rows.

    The recovery engine assembles events inside a local function and returns
    them; the caller binds the result to a new name. Tracing only the direct
    assignment missed the write because the values crossed a return boundary
    first.
    """
    returning: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        if any(
            isinstance(inner, ast.Return) and inner.value is not None and _handed_result(inner.value, produced)
            for inner in ast.walk(node)
        ):
            returning.add(node.name)
    if not returning:
        return set()
    return {
        target.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign | ast.AnnAssign) and node.value is not None
        for target in (node.targets if isinstance(node, ast.Assign) else [node.target])
        if isinstance(target, ast.Name)
        and any(
            isinstance(inner, ast.Call) and ast.unparse(inner.func).rsplit(".", 1)[-1] in returning
            for inner in ast.walk(node.value)
        )
    }


def _parameters_holding_rows(tree: ast.Module, produced: set[str]) -> set[str]:
    """Return the parameters a caller hands helper-built rows into.

    The reverse hop of :func:`_returned_rows`. That one follows rows out of a
    helper that returns them; this follows them into the helper that receives
    them. The recovery engine normalizes events in one method and persists them
    in another, passing ``canonical_events`` and ``raw_pbp_rows`` as parameters,
    so the rows never appear in an assignment inside the writing method and the
    trace stopped at its signature.
    """
    return {
        argument.arg
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
        for argument in (*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs)
        if isinstance(argument, ast.arg) and argument.arg in produced
    }


def _rows_carried_into_a_helper(tree: ast.Module, produced: set[str]) -> set[str]:
    """Return the helper parameters a caller passes built rows to.

    Read off the call site, so the writer is found by the argument it is given
    rather than by the shape of the expression on either side of it.
    """
    receiving = _parameters_holding_rows(tree, produced)
    if not receiving:
        return set()
    definitions = {
        node.name: node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
    }
    carried: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name):
            continue
        target = definitions.get(node.func.id)
        if target is None:
            continue
        names = [
            argument.arg for argument in (*target.args.posonlyargs, *target.args.args) if isinstance(argument, ast.arg)
        ]
        for index, argument in enumerate(node.args):
            if index < len(names) and names[index] in receiving and _handed_result(argument, produced):
                carried.add(names[index])
        for keyword in node.keywords:
            if keyword.arg in receiving and _handed_result(keyword.value, produced):
                carried.add(keyword.arg)
    return carried


def _models_owned_by(caller: Path, module: str) -> set[str]:
    """Return the tables a caller writes specifically for ``module``.

    Scoped to the statements that produce this crawler's result. A caller that
    drives several crawlers -- ``live_crawler`` runs the schedule, game detail,
    the Naver relay and the PBP crawler in one file -- would otherwise credit
    every one of them with every table the file persists, because each write
    call looks the same from outside. Only a write whose argument can be traced
    to a call of *this* crawler counts.

    Args:
        caller: The file driving the crawler.
        module: The crawler module being attributed.

    Returns:
        Model names attributable to this crawler alone.

    """
    tree = _parse_or_none(caller)
    if tree is None:
        return set()

    crawlers_driven = {
        node.module.rsplit(".", 1)[-1]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("src.crawlers.")
    }
    if module not in crawlers_driven:
        return set()

    symbols = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module == f"src.crawlers.{module}"
        for alias in node.names
    }
    instances = _crawler_instances(tree, symbols)
    if not instances:
        # The caller never instantiates this crawler, so no result can be traced
        # from a fetch to a write. That is a different shape from a caller that
        # drives several crawlers -- the recovery engine uses the PBP crawler's
        # static helpers and then persists relay rows itself -- so the write is
        # taken from what the write function says it commits rather than from
        # argument tracing, which has nothing to trace.
        return _models_committed_beside(caller, symbols)

    persisted: set[str] = set()
    for scope in _scopes(tree):
        # Each scope is evaluated on its own. A module driving four crawlers
        # shares function bodies with unrelated fetches, and a module-wide
        # derivation merged all of their results -- which is how the schedule
        # crawler came to claim the play-by-play tables.
        results = _crawler_results(scope, instances)
        if not results:
            continue
        persisted |= _writes_in_scope(caller, scope, _expanded_carried(scope, results))
    return persisted - _shared_with_other_crawlers(caller, persisted, crawlers_driven, module)


def _scopes(tree: ast.Module) -> list[ast.AST]:
    """Return each module-level definition and the bare module body as a scope."""
    scopes: list[ast.AST] = [tree]
    scopes.extend(node for node in tree.body if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef))
    return scopes


def _crawler_instances(tree: ast.AST, symbols: set[str]) -> set[str]:
    """Return the variables bound to a crawler object.

    ``sched_crawler`` is the object the fetch is made on, not the result of it.
    Treating it as a result made every write touching it -- including the ones
    performed for other crawlers in the same scope -- look like this crawler's
    output, and it is what made the schedule crawler claim tables filled by the
    relay fetch that happened to share its scope.
    """
    instances: set[str] = set()
    # Walks the whole module: a crawler is bound inside a function, and looking
    # only at the module's own statements would find no binding at all.
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign | ast.AnnAssign) or node.value is None:
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        names = {target.id for target in targets if isinstance(target, ast.Name)}
        if any(
            isinstance(inner, ast.Call) and _call_constructs_crawler(inner, symbols) for inner in ast.walk(node.value)
        ):
            instances |= names
    return instances


def _crawler_results(scope: ast.AST, instances: set[str]) -> set[str]:
    """Return the names a crawler fetch produced inside one scope."""
    produced: set[str] = set()
    for node in _own_statements(scope):
        if not isinstance(node, ast.Assign | ast.AnnAssign) or node.value is None:
            continue
        for inner in ast.walk(node.value):
            if not isinstance(inner, ast.Call) or not _call_uses_crawler(inner, instances, set()):
                continue
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            produced |= {target.id for target in targets if isinstance(target, ast.Name)}
    return produced


def _writes_in_scope(caller: Path, scope: ast.AST, carried: set[str]) -> set[str]:
    """Return the tables the writes in one scope persist for a carried value."""
    persisted: set[str] = set()
    for node in _own_statements(scope):
        for call in ast.walk(node):
            if isinstance(call, ast.Call):
                persisted |= _tables_from_write(caller, call, carried)
    return persisted


def _tables_from_write(caller: Path, call: ast.Call, carried: set[str]) -> set[str]:
    """Return the tables one call persists for a carried value.

    Only writes handed this crawler's own result count. ``live_crawler`` calls
    ``save_game_snapshot`` for the schedule and game-detail rows too, and
    crediting the PBP crawler with those would put the most-read table in the
    file behind a crawler that does not own it.
    """
    name = ast.unparse(call.func).rsplit(".", 1)[-1]
    repository = _repository_owning(call, carried, caller)
    method_tables = _repository_method_tables(repository, _method_called(call))
    if not method_tables:
        # The call may be the crawler's own helper rather than the writer. The
        # preview batch's ``_save_preview_contexts`` reads as a writer by name --
        # it starts with ``_save`` -- but resolves to no repository and no
        # imported function, and the write happens one frame inside it. Asking
        # first is what resolves it; asking last never got there.
        via_helper = _models_written_via_helper(caller, call, carried)
        if via_helper:
            return via_helper
        if not _is_write_call(name):
            return set()
    if not _handed_result(call, carried):
        # The rows may reach the writer through a callback the caller supplies.
        # ``collect_rosters`` passes ``save_callback=save_chunk`` into the fetch,
        # so the crawl call itself hands over no rows and the write happens later,
        # inside a function the crawl invoked. Reading the call as a plain fetch
        # found no write at all.
        return _tables_in_callback(caller, call)
    if method_tables:
        # The repository's body, not its name: ``BroadcastRepository`` constructs
        # ``GameBroadcast``, and stripping the suffix pointed the crawler at a
        # table that does not exist. Narrowed to the method called, because a
        # repository is not one writer: ``m_repo.save_player_movements(rows)``
        # reached the class-wide view and attributed rows the method never
        # touches.
        return method_tables
    # The write usually happens one module away: the live crawler hands the rows
    # to ``save_relay_data``, and the repository is a second hop from there.
    return _models_written_by(caller, call)


def _tables_in_callback(caller: Path, call: ast.Call) -> set[str]:
    """Return the tables a function the caller passes in as a callback writes.

    A crawler may be handed the writer rather than the rows: the roster collector
    passes ``save_callback=save_chunk`` into the fetch, and the callback commits
    each chunk as it arrives. The write is therefore reached from the argument,
    not from the call's return value.

    Args:
        caller: The file making the call.
        call: The call passing the callback.

    Returns:
        Model names the callback's write persists.

    """
    tree = _parse_or_none(caller)
    if tree is None:
        return set()
    local = {node.name: node for node in tree.body if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)}
    tables: set[str] = set()
    for keyword in call.keywords:
        if not isinstance(keyword.value, ast.Name) or keyword.value.id not in local:
            continue
        callback = local[keyword.value.id]
        tables |= _models_in_write_body(_with_local_helpers(caller, callback), keyword.value.id)
        # The callback may itself hand the rows to a repository in another
        # module, as the roster collector's does. Scanning its body finds the
        # call and not the table, so the delegated write is followed the same way
        # a direct one would be.
        for inner in ast.walk(callback):
            if isinstance(inner, ast.Call) and _is_write_call(ast.unparse(inner.func).rsplit(".", 1)[-1]):
                tables |= _models_written_by(caller, inner)
    return tables


def _shared_with_other_crawlers(caller: Path, persisted: set[str], crawlers_driven: set[str], module: str) -> set[str]:
    """Return the tables in ``persisted`` that another crawler here also writes."""
    return {
        model
        for model in persisted
        if any(
            model in (_repository_model_names(_crawler_source_or_empty(other)) | _models_written_beside(caller, other))
            for other in crawlers_driven
            if other != module
        )
    }


def _method_called(call: ast.Call) -> str:
    """Return the repository method a call names.

    ``repo.upsert_player_profile(...)`` reaches its tables through a method, and
    which one decides what gets written. Empty when the call is not a method
    call, which asks for the class-wide view.
    """
    return call.func.attr if isinstance(call.func, ast.Attribute) else ""


def _repository_owning(call: ast.Call, produced: set[str], caller: Path) -> str:
    """Return the repository class a call was made on.

    ``PlayerRepository(session).upsert_player_profile(...)`` names the class only
    in the chain that produced the receiver, so the receiver is traced back
    through its assignment.
    """
    func = call.func
    if not isinstance(func, ast.Attribute):
        return ast.unparse(func).rsplit(".", 1)[-1]
    receiver = func.value
    if isinstance(receiver, ast.Call):
        return ast.unparse(receiver.func).rsplit(".", 1)[-1]
    if isinstance(receiver, ast.Name):
        tree = _parse_or_none(caller)
        if tree is not None:
            for node in ast.walk(tree):
                if isinstance(node, ast.Assign | ast.AnnAssign) and node.value is not None:
                    targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                    if any(isinstance(target, ast.Name) and target.id == receiver.id for target in targets):
                        for inner in ast.walk(node.value):
                            if isinstance(inner, ast.Call):
                                return ast.unparse(inner.func).rsplit(".", 1)[-1]
    del produced  # the receiver binding is the only evidence needed
    return ""


def _models_written_beside(caller: Path, module: str) -> set[str]:
    """Return the tables a caller writes for ``module`` specifically.

    Attributes a repository write to whichever crawler call the written value
    came from. This is the narrowest evidence available without running
    anything: the caller names the crawler it called and the repository it handed
    that result to, and the two appear in the same statement or in the one that
    produced the argument.

    Args:
        caller: The file that calls the crawler.
        module: The crawler module whose result is being written.

    Returns:
        Model names attributable to this crawler alone. Empty when the caller's
        structure does not say which repository the result reaches.

    """
    tree = _parse_or_none(caller)
    if tree is None:
        return set()

    crawler_names = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module == f"src.crawlers.{module}"
        for alias in node.names
    }
    if not crawler_names:
        return set()

    result_vars = _produced_names(tree, crawler_names)
    if not result_vars:
        return set()

    models: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = ast.unparse(node.func)
        if "Repository" not in name:
            continue
        if _mentions_result(node, result_vars):
            models.add(name.rsplit(".", 1)[-1].removesuffix("Repository"))
    return models


def _mentions_result(node: ast.AST, result_vars: set[str]) -> bool:
    """Return whether a repository call is handed one of the crawler results."""
    return any(isinstance(child, ast.Name) and child.id in result_vars for child in ast.walk(node))


@cache
def _parse_or_none(path: Path) -> ast.Module | None:
    """Parse a source file, returning ``None`` when it cannot be read.

    Tolerates a file that vanished or fails to compile rather than raising: this
    scan walks the working tree, and a half-written module elsewhere in the
    repository should not take down a report about crawlers.

    Cached per path because the same files are reached through many independent
    lookups -- resolving a repository class, then its method, then the write
    function each walk the tree from a different entry point -- and parsing was
    the dominant cost of the report: the uncached scan called ``ast.parse``
    roughly five thousand times for a tree of a few hundred files.

    The cached tree is shared, so callers must only read it. Nothing in this
    module rewrites a parsed tree, and a stale cache is not a risk here because
    a report is built from one reading of the working tree.
    """
    try:
        return ast.parse(path.read_text(encoding="utf-8", errors="ignore"))
    except (OSError, SyntaxError, ValueError):
        return None


def written_models(module: str) -> frozenset[str]:
    """Return the tables a crawler is responsible for filling.

    Args:
        module: Crawler module name.

    Returns:
        Model names, from the crawler's own imports or from the caller that
        writes its result. Empty when neither names a table.

    """
    return _crawler_written_models().get(module, frozenset())


def upstream_reader_files(module: str) -> frozenset[str]:
    """Return the distinct modules that read what ``module`` feeds.

    Args:
        module: Crawler module name.

    Returns:
        Repository-relative paths of the readers. A reader that imports several
        of the crawler's tables appears once.

    """
    by_model = _model_reader_files()
    readers: set[str] = set()
    for model in written_models(module):
        readers |= by_model.get(model, frozenset())
    return frozenset(readers)


_MAX_EXPORT_HOPS = 3

#: How many write hops attribution follows from a caller to the rows it fills.
#: Two: the call itself and one delegated write inside the resolved writer.
_MAX_WRITE_HOPS = 2


@dataclass(frozen=True)
class Attribution:
    """What a crawler's upstream impact could and could not be measured from.

    ``upstream_dependents`` alone cannot tell "nothing reads this" from "the
    write could not be resolved". The two call for opposite responses -- one is a
    measurement, the other a gap in the analysis -- and they arrived here with
    the same value, so a roadmap could send an operator after a crawler whose
    readers were simply never found.
    """

    module: str
    models: frozenset[str]
    reader_files: frozenset[str]
    unresolved_callers: tuple[str, ...]

    @property
    def attributed(self) -> bool:
        """Return whether at least one writer resolved to a table."""
        return bool(self.models)

    @property
    def readers(self) -> int:
        """Return how many distinct modules read the resolved tables."""
        return len(self.reader_files)


@cache
def attribution_of(module: str) -> Attribution:
    """Return the measured attribution for one crawler.

    Args:
        module: Crawler module name.

    Returns:
        The resolved tables, the modules reading them, and the callers whose
        write could not be resolved.

    """
    models = written_models(module)
    readers = upstream_reader_files(module)
    return Attribution(
        module=module,
        models=models,
        reader_files=readers,
        unresolved_callers=tuple(sorted(_unresolved_callers(module) if models else _caller_paths(module))),
    )


def _caller_paths(module: str) -> set[str]:
    """Return the source files importing ``module``."""
    callers: set[str] = set()
    for path in _iter_source_files():
        tree = _parse_or_none(path)
        if tree is None:
            continue
        if any(isinstance(node, ast.ImportFrom) and node.module == f"src.crawlers.{module}" for node in ast.walk(tree)):
            callers.add(str(path))
    return callers


def _unresolved_callers(module: str) -> set[str]:
    """Return the callers holding a crawler result that reached no table.

    A caller that binds the crawler's output and hands it somewhere is evidence
    of a write the analysis failed to follow. Leaving it out would report the
    failure as a clean zero.

    Excludes a caller that asked the crawler to persist its own rows. Those
    rows were not handed anywhere by the caller: ``run_daily_update`` reads back
    the ticket prices it just saved only to count them, and the table the rows
    reached is already attributed to the crawler that filled it. Counting that
    as an untraced write claims a write was missed when the write had in fact
    been resolved, which buries the pairs where the analysis really did fail.
    """
    unresolved: set[str] = set()
    for path in _iter_source_files():
        tree = _parse_or_none(path)
        if tree is None:
            continue
        symbols = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module == f"src.crawlers.{module}"
            for alias in node.names
        }
        if not symbols or not _produced_names(tree, symbols):
            continue
        if _asked_crawler_to_save(tree, symbols):
            continue
        if not _models_owned_by(path, module):
            unresolved.add(str(path))
    return unresolved


def _asked_crawler_to_save(tree: ast.Module, symbols: set[str]) -> bool:
    """Return whether a caller turned on the crawler's own persistence.

    Read from the call that produced the result, not from the crawler's source:
    the flag is the caller's decision, and a crawler that supports saving is
    not thereby saving. A caller that sets it has already accounted for the
    write the rows will reach.
    """
    instances: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign | ast.AnnAssign) or node.value is None:
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        names = {target.id for target in targets if isinstance(target, ast.Name)}
        for inner in ast.walk(node.value):
            if not isinstance(inner, ast.Call):
                continue
            if _call_constructs_crawler(inner, symbols):
                instances |= names
    if not instances:
        return False
    return any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id in instances
        and any(_is_literal_true(keyword) for keyword in node.keywords)
        for node in ast.walk(tree)
    )


def _is_literal_true(keyword: ast.keyword) -> bool:
    """Return whether a keyword literally asks for ``True``.

    Only a literal counts. ``save=save`` forwards whatever the caller was given,
    so reading it as an instruction to persist would exempt every caller that
    offers a ``--save`` flag -- including the ones that spend most of their time
    without it. The literal is the claim the crawler will act on.
    """
    return keyword.arg == "save" and isinstance(keyword.value, ast.Constant) and keyword.value.value is True


def upstream_dependents_of(module: str) -> int:
    """Return how many non-crawler modules read the data ``module`` feeds.

    Args:
        module: Crawler module name.

    Returns:
        The number of distinct modules reading the tables the crawler writes.
        Zero for a crawler nothing downstream depends on.

    """
    return len(upstream_reader_files(module))


#: Where a crawler deliberately leaves part of its run bookkeeping to the code
#: out complete, so the service that writes it is the only place that can record
#: the run or queue the game. The capability stays derived rather than declared:
#: the owner is read and must contain the evidence, so deleting the enqueue or
#: the run recording there turns the capability off and trips the drift gate
#: exactly as a crawler-local implementation would.
DELEGATED_CAPABILITIES: dict[str, tuple[tuple[str, tuple[str, ...]], ...]] = {
    "game_detail_crawler": (
        ("src/services/game_collection_service.py", ("enqueue_failure", "DeadLetterSpec")),
        ("src/services/game_detail_runs.py", ("open_runs", "record_success")),
    ),
    # Relay reaches its ledger and its queue the same way game detail does: the
    # crawler fetches, and the service that owns the write decides what was
    # stored. Pointing this at the crawler module would have gone stale the
    # moment either half moved.
    "relay_crawler": (
        ("src/services/game_collection_service.py", ("enqueue_failure", "DeadLetterSpec")),
        ("src/services/relay_runs.py", ("open_runs", "record_success")),
    ),
    # The all-series crawlers hand their bookkeeping to the runner they share.
    # The two modules are twins, so the same owner serves both, and the runner is
    # read for the evidence exactly as a crawler-local implementation would be:
    # the shared-file router does not exist -- one of these crawlers waiting on
    # the other's ledger row, or inheriting the other's dead letters, is not
    # something either would report loudly.
    "player_batting_all_series_crawler": (
        ("src/crawlers/season_series_outcome.py", ("enqueue_failure", "DeadLetterSpec")),
        ("src/crawlers/season_series_outcome.py", ("track_crawl_run",)),
    ),
    "player_pitching_all_series_crawler": (
        ("src/crawlers/season_series_outcome.py", ("enqueue_failure", "DeadLetterSpec")),
        ("src/crawlers/season_series_outcome.py", ("track_crawl_run",)),
    ),
}


def _owns(module: str, source: str, evidence: tuple[str, ...], delegated_index: int) -> bool:
    """Return whether a capability is present locally or through its owner."""
    if all(token in source for token in evidence):
        return True
    owners = DELEGATED_CAPABILITIES.get(module, ())
    if len(owners) <= delegated_index:
        return False
    owner, owner_evidence = owners[delegated_index]
    return all(token in _read_source(owner) for token in owner_evidence)


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
    transports = _resolve_transports(tree, node, reaches_httpx=_reaches_httpx_itself(tree))

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
        ledger=_owns(module, source, ("track_crawl_run",), 1),
        dead_letter=_owns(module, source, ("DeadLetterSpec", "enqueue_failure"), 0),
        uses_crawl_result=_imports_result_vocabulary(tree) or _imports_page_outcome_vocabulary(tree),
        has_entrypoint=_has_entrypoint(tree),
        inherited_shared_http=_ancestor_uses_shared_client(node),
        upstream_dependents=upstream_dependents_of(module),
    )


def _ancestor_uses_shared_client(node: ast.ClassDef | None) -> bool:
    """Return whether an ancestor's source reaches for the shared HTTP client.

    Resolved from the base classes' own source rather than assumed from the
    ancestry alone: inheriting ``BaseHttpCrawler`` only makes a client
    *available*, exactly as the docstring on ``_reaches_httpx_itself`` records.
    The claim is therefore evidence-based -- an intermediate base that never
    calls ``http_client()`` leaves its subclasses ungoverned, which is the
    honest answer for them.

    Args:
        node: The crawler class to start from.

    Returns:
        Whether any ancestor composes ``CrawlerHttpClient`` or ``http_client``.

    """
    seen: set[str] = set()
    pending = [_base_name(node)] if node is not None else []
    while pending:
        current = pending.pop()
        if not current or current in seen:
            continue
        seen.add(current)
        parent = _module_defining_class(current)
        if parent is None:
            continue
        body = ast.unparse(parent)
        if "CrawlerHttpClient" in body or "http_client" in body:
            return True
        pending.append(_base_name(parent))
    return False


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
    drift.extend(verify_priority_order(rows))

    return AdoptionMatrix(rows=tuple(rows), drift=tuple(drift), advisories=tuple(advisories))


def verify_priority_order(rows: list[CrawlerRow], order: tuple[str, ...] = PRIORITY_ORDER) -> list[str]:
    """Return the ways the declared migration order no longer points at work.

    A priority entry naming an adopted crawler is worse than an empty tuple: the
    report still looks deliberate while sending an operator at finished work, and
    nothing else in the matrix would say so. Both halves are checked -- an adopted
    name and a name that is not a crawler at all.

    The order is a parameter rather than read from the module so the check can be
    exercised against orders nobody would declare, which is the only way to prove
    it fires at all.
    """
    by_module = {row.module: row for row in rows}
    problems: list[str] = []
    for module in order:
        row = by_module.get(module)
        if row is None:
            problems.append(f"{module}: declared migration priority but no such crawler")
            continue
        if row.fully_adopted:
            problems.append(f"{module}: declared migration priority but is already adopted")
            continue
        # A declared entry overrides the computed order, so it must not invert the
        # rule it is meant to express. Naming a crawler that feeds nothing while a
        # crawler feeding many is left waiting sends an operator to the least
        # damaging work on the board, which is the opposite of what a priority
        # list is for. Only the declared entries are compared: an undeclared
        # crawler is simply ordered by the axis.
        better = max(
            (other.facts.upstream_dependents for other in rows if other.module not in order),
            default=0,
        )
        if row.facts.upstream_dependents < better:
            loudest = max(
                (other for other in rows if other.module not in order),
                key=lambda other: other.facts.upstream_dependents,
                default=None,
            )
            problems.append(
                f"{module}: declared migration priority but feeds {row.facts.upstream_dependents} downstream "
                f"modules while {loudest.module} feeds {loudest.facts.upstream_dependents}",
            )
    return problems


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
    lines.append("**Migration order** (widest upstream impact first, then fewest satisfied axes; the gap is named):")
    for position, row in enumerate(matrix.roadmap(), start=1):
        gaps = ", ".join(row.remaining_axes)
        suffix = f" -- {gaps}" if gaps else ""
        lines.append(f"{position}. `{module_name(row)}`{suffix}")
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
    "verify_priority_order",
    "verify_row",
]
