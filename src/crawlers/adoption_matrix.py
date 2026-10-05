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
REPO_ROOT = CRAWLER_DIR.parent.parent
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
        """
        if self.shared_http:
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
                    facts.owns_transport,
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
    "game_detail": "game_detail_crawler",
    "relay": "relay_crawler",
    "food": "food_crawler",
    "parking": "parking_crawler",
    "kbo_event": "kbo_event_crawler",
    "player_movement": "player_movement_crawler",
    "team_history": "team_history_crawler",
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
        note="Shares `BaseHttpCrawler`, so the transport move is a base change.",
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
#: The names below lead the computed order as well as the declared one. They have
#: none of the reliability chain yet, so this batch is the whole contract rather
#: than its last axis.
PRIORITY_ORDER: tuple[str, ...] = (
    "baserunning_stats_crawler",
    "broadcast_crawler",
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


def _resolve_transports(tree: ast.Module, base_class: str, *, reaches_httpx: bool) -> frozenset[Transport]:
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
    if base_class in _HTTP_BASES and reaches_httpx:
        found.add(Transport.RAW_HTTPX)
    if base_class in _PLAYWRIGHT_BASES:
        found.add(Transport.PLAYWRIGHT)
    return frozenset(found)


#: Where a crawler deliberately leaves part of its run bookkeeping to the code
#: that owns the write, together with the evidence that proves the owner really
#: does it. A crawler that only fetches cannot know whether its payload turned
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
    transports = _resolve_transports(tree, base_class, reaches_httpx=_reaches_httpx_itself(tree))

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
        elif row.fully_adopted:
            problems.append(f"{module}: declared migration priority but is already adopted")
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
    lines.append("**Migration order** (fewest satisfied axes first; the gap is named, not implied):")
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
