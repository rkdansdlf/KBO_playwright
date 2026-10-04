"""The adoption matrix must describe the crawlers that actually exist.

A matrix is only worth building if it cannot quietly go stale. These tests pin
the two things that make it trustworthy: every crawler module on disk is
classified, and the declared design is cross-checked against the code it claims
to describe. The migrated crawlers are held to the full contract so a later edit
that quietly undoes one of them fails here rather than in production.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

SRC_CRAWLERS = Path(__file__).resolve().parents[2] / "src" / "crawlers"

from src.crawlers.adoption_matrix import (
    DECLARED,
    PRIORITY_ORDER,
    REPLAY_HANDLERS,
    AdoptionMatrix,
    CrawlerRow,
    DesignFacts,
    EmptySemantics,
    Fallback,
    Granularity,
    ModuleFacts,
    Transport,
    advise_row,
    build_matrix,
    _imports_page_outcome_vocabulary,
    _looks_like_outcome_value,
    _reaches_httpx_itself,
    discover_modules,
    render_markdown,
    scan_module,
    transports_of,
    verify_priority_order,
    verify_row,
)


def _module_source(module: str) -> str:
    """Read a crawler module the way the matrix reads it."""
    return (SRC_CRAWLERS / f"{module}.py").read_text()


@pytest.fixture(scope="module")
def matrix() -> AdoptionMatrix:
    return build_matrix()


def _row(module: str) -> CrawlerRow:
    return CrawlerRow(facts=scan_module(module), design=DECLARED.get(module))


def _facts(**overrides) -> ModuleFacts:
    base = {
        "module": "example_crawler",
        "base_class": "",
        "transports": frozenset({Transport.RAW_HTTPX}),
        "owns_throttle": False,
        "snapshot": False,
        "persistence": False,
        "ledger": False,
        "dead_letter": False,
        "uses_crawl_result": False,
        "has_entrypoint": True,
    }
    base.update(overrides)
    return ModuleFacts(**base)  # type: ignore[arg-type]


class TestCoverage:
    def test_every_module_on_disk_is_classified(self, matrix: AdoptionMatrix) -> None:
        """A new crawler that nobody classified must fail here.

        The directory is globbed directly rather than through the module's own
        helper: comparing the helper against itself would pass even if it stopped
        finding crawlers at all.
        """
        on_disk = {path.stem for path in Path(SRC_CRAWLERS).glob("*_crawler.py")}
        classified = {row.module for row in matrix.rows}

        assert on_disk, "the crawler directory glob found nothing, so the test is vacuous"
        assert on_disk == classified

    def test_discovery_finds_the_same_modules_as_the_directory(self) -> None:
        assert set(discover_modules()) == {path.stem for path in Path(SRC_CRAWLERS).glob("*_crawler.py")}

    def test_the_matrix_is_not_empty(self, matrix: AdoptionMatrix) -> None:
        assert len(matrix.rows) > 40

    def test_declared_design_points_at_real_modules(self, matrix: AdoptionMatrix) -> None:
        modules = {row.module for row in matrix.rows}

        for module in DECLARED:
            assert module in modules, f"{module} has declared design but no module"


class TestReplayRegistrationMatchesTheDispatcher:
    def test_declared_handlers_match_the_real_dispatcher(self) -> None:
        """The matrix must not claim a replay that was never registered."""
        from src.services.crawl_replay_dispatcher import build_default_dispatcher

        registered = build_default_dispatcher().registered_crawlers()

        assert set(REPLAY_HANDLERS) == registered

    def test_every_handler_points_at_a_real_module(self) -> None:
        modules = set(discover_modules())

        for module in REPLAY_HANDLERS.values():
            assert module in modules


class TestMigratedCrawlersStayMigrated:
    @pytest.mark.parametrize("module", ["award_crawler", "roster_transaction_crawler", "schedule_crawler"])
    def test_the_chain_is_closed(self, module: str) -> None:
        row = _row(module)

        assert row.fully_adopted
        assert row.facts.shared_http
        assert row.facts.uses_crawl_result
        assert row.facts.ledger
        assert row.facts.dead_letter
        assert row.facts.replay

    @pytest.mark.parametrize("module", ["award_crawler", "roster_transaction_crawler", "schedule_crawler"])
    def test_none_of_them_throttles_manually(self, module: str) -> None:
        """A second wait doubles every delay and hides the adaptive backoff."""
        assert not _row(module).facts.owns_throttle

    def test_award_keeps_source_granularity(self) -> None:
        """One row per source, so one source failing is a partial run."""
        row = _row("award_crawler")

        assert row.design is not None
        assert row.design.granularity is Granularity.SOURCE
        assert row.design.empty is EmptySemantics.TYPED

    def test_roster_keeps_a_confirmed_empty(self) -> None:
        """A quiet day is common, so EMPTY may not be claimed on a guess."""
        row = _row("roster_transaction_crawler")

        assert row.design is not None
        assert row.design.granularity is Granularity.DATE
        assert row.design.empty is EmptySemantics.TYPED_CONFIRMED
        assert row.design.fallback is Fallback.BROWSER

    def test_schedule_keeps_a_confirmed_empty(self) -> None:
        """An off-season month is common, so EMPTY may not be claimed on a guess."""
        row = _row("schedule_crawler")

        assert row.design is not None
        assert row.design.granularity is Granularity.MONTH
        assert row.design.empty is EmptySemantics.TYPED_CONFIRMED
        assert row.design.fallback is Fallback.BROWSER

    def test_no_drift_is_reported(self, matrix: AdoptionMatrix) -> None:
        assert matrix.drift == ()


class TestTransportDetection:
    def test_the_schedule_keeps_both_paths(self) -> None:
        """Naver API through the shared client, KBO page through the browser."""
        facts = scan_module("schedule_crawler")

        assert facts.shared_http
        assert Transport.PLAYWRIGHT in facts.transports

    def test_roster_uses_the_shared_client_and_a_browser_fallback(self) -> None:
        facts = scan_module("roster_transaction_crawler")

        assert facts.shared_http
        assert Transport.PLAYWRIGHT in facts.transports
        assert Transport.RAW_HTTPX not in facts.transports

    def test_a_base_http_crawler_inherits_the_raw_path(self) -> None:
        facts = scan_module("ticket_crawler")

        assert facts.base_class == "BaseHttpCrawler"
        assert Transport.RAW_HTTPX in facts.transports

    def test_transport_order_is_stable(self) -> None:
        facts = scan_module("food_crawler")

        assert transports_of(facts)[0] is Transport.CRAWLER_HTTP_CLIENT

    def test_a_data_only_module_reports_no_transport(self) -> None:
        facts = scan_module("draft_history_crawler")

        assert facts.has_transport


class TestDetectionFollowsCallsNotWords:
    """Naming httpx, or ending a class name with CrawlResult, proves nothing.

    Both of these gates were once string searches, and both were wrong in a way
    that pointed at the wrong work. A crawler that listed `httpx.HTTPError`
    among the exceptions it catches read as still reaching for httpx, so a fully
    migrated one looked half-converted forever. In the other direction, a
    crawler's own `RelayCrawlResult` dataclass satisfied a search for the string
    `CrawlResult`, so a crawler with no typed results at all read as having them.
    """

    def test_a_crawler_naming_its_own_result_type_is_not_typed(self) -> None:
        """`RelayCrawlResult` ends in the word the old search looked for."""
        facts = scan_module("text_relay_crawler")

        assert "RelayCrawlResult" in _module_source("text_relay_crawler")
        assert facts.uses_crawl_result is False

    def test_an_external_stats_result_type_is_not_the_shared_one(self) -> None:
        facts = scan_module("external_stats_crawler")

        assert "ExternalCrawlResult" in _module_source("external_stats_crawler")
        assert facts.uses_crawl_result is False

    def test_importing_the_shared_outcome_is_what_counts_as_typed(self) -> None:
        """`CrawlOutcome` alone is enough: branching on it is the whole point."""
        facts = scan_module("food_crawler")

        assert "CrawlOutcome" in _module_source("food_crawler")
        assert "CrawlResult" not in _module_source("food_crawler")
        assert facts.uses_crawl_result is True

    def test_catching_an_httpx_error_is_not_reaching_for_httpx(self) -> None:
        """The exception list is how a migrated crawler names what it handles."""
        source = _module_source("food_crawler")

        assert "httpx.HTTPError" in source
        facts = scan_module("food_crawler")
        assert Transport.RAW_HTTPX not in facts.transports

    def test_a_migrated_crawler_reports_only_the_shared_client(self) -> None:
        facts = scan_module("food_crawler")

        assert facts.shared_http
        assert transports_of(facts) == (Transport.CRAWLER_HTTP_CLIENT,)

    def test_building_a_client_still_counts_as_raw(self) -> None:
        """The gate must not have been tightened into reporting nothing."""
        assert _reaches_httpx_itself(
            ast.parse("async def f():\n    return httpx.AsyncClient()\n"),
        )
        assert _reaches_httpx_itself(ast.parse("x = httpx.get('https://x.test')\n"))

    def test_naming_an_error_type_is_not_building_a_client(self) -> None:
        assert not _reaches_httpx_itself(ast.parse("E = (httpx.HTTPError,)\n"))
        assert not _reaches_httpx_itself(ast.parse("# httpx.AsyncClient is documented here\n"))

    def test_a_typed_crawler_is_fully_adopted_once_replay_exists(self) -> None:
        """The point of the fix: the crawler was already safe, and said so."""
        facts = scan_module("food_crawler")
        row = _row("food_crawler")

        assert facts.uses_crawl_result
        assert facts.ledger
        assert facts.dead_letter
        assert Transport.RAW_HTTPX not in facts.transports
        assert row.fully_adopted is True

    def test_the_typed_outcome_axis_turns_away_a_crawler_that_only_logs(self) -> None:
        """No real row depends on it today, so it is tested on a row that would.

        Every crawler that closes the rest of the chain also classifies its
        outcome, so dropping the condition would change nobody's verdict *now*.
        It is kept because it is what keeps the ledger honest for the next crawler
        to adopt everything else: without it, one whose failures exist only as
        log lines would count as fully adopted.

        This asserts the coincidence and says why, rather than pretending the
        axis is load-bearing today. An earlier version asserted the opposite --
        that dropping it *would* change the verdict -- which was true while the
        browser crawlers still owed a vocabulary and went false the moment all
        three adopted one.
        """
        matrix = build_matrix()
        without_typed_outcome = {
            row.module
            for row in matrix.rows
            if row.facts.owns_transport and row.facts.ledger and row.facts.dead_letter and row.facts.replay
        }
        assert without_typed_outcome == {row.module for row in matrix.adopted()}, (
            "a crawler closes the chain without classifying outcomes: "
            f"{sorted(without_typed_outcome - {row.module for row in matrix.adopted()})}"
        )

        untyped = CrawlerRow(
            facts=_facts(
                transports=frozenset({Transport.PLAYWRIGHT}),
                ledger=True,
                dead_letter=True,
            ),
            design=DesignFacts(granularity=Granularity.SEASON, empty=EmptySemantics.TYPED),
        )
        assert untyped.facts.owns_transport is True
        assert untyped.fully_adopted is False

    def test_the_transport_axis_turns_away_a_second_request_path(self) -> None:
        """No real row depends on it yet, so it is tested on a row that would.

        Every crawler that closes the rest of the chain happens to have a single
        governed request path, so dropping the transport condition would change
        nobody's verdict today -- which is exactly why it needs a test that does
        not depend on the current fleet. The condition protects the next crawler
        to adopt the chain with an ungoverned client alongside, and this asserts
        that it does, rather than leaving it to look like dead weight.
        """
        ungoverned = CrawlerRow(
            facts=_facts(
                transports=frozenset({Transport.PLAYWRIGHT, Transport.RAW_HTTPX}),
                uses_crawl_result=True,
                ledger=True,
                dead_letter=True,
            ),
            design=DesignFacts(granularity=Granularity.SOURCE, empty=EmptySemantics.TYPED),
        )

        assert ungoverned.facts.owns_transport is False
        assert ungoverned.fully_adopted is False

    def test_no_real_crawler_carries_an_ungoverned_second_path(self) -> None:
        """So the axis above guards a real invariant rather than a hypothetical.

        ``preview_crawler`` drives a browser *and* opens a raw ``httpx`` client,
        so "no crawler has both" would be false -- and correctly so: it is a
        roadmap item precisely because of that second path. The claim the axis
        actually makes is narrower, and this pins it: nothing that calls itself
        adopted may carry an ungoverned second path.
        """
        both_paths = {
            row.module
            for row in build_matrix().rows
            if {Transport.PLAYWRIGHT, Transport.RAW_HTTPX} <= row.facts.transports
        }
        adopted = {row.module for row in build_matrix().adopted()}

        assert adopted & both_paths == set(), (
            f"an adopted crawler drives a browser and a raw client at once: {sorted(adopted & both_paths)}"
        )


class TestDriftDetection:
    def test_a_dead_letter_without_a_ledger_is_drift(self) -> None:
        row = CrawlerRow(facts=_facts(dead_letter=True))

        assert any("without recording a run" in message for message in verify_row(row))

    def test_replay_without_dead_letters_is_drift(self) -> None:
        # `replay` is derived from the module name, so a registered name is the
        # only way to switch that axis on.
        row = CrawlerRow(facts=_facts(module="award_crawler", ledger=True, dead_letter=False))

        assert row.facts.replay is True
        assert any("enqueues no dead letters" in message for message in verify_row(row))

    def test_a_declared_typed_empty_needs_crawl_result(self) -> None:
        row = CrawlerRow(
            facts=_facts(),
            design=DesignFacts(granularity=Granularity.TEAM, empty=EmptySemantics.TYPED),
        )

        assert any("never uses CrawlResult" in message for message in verify_row(row))

    def test_a_date_crawler_without_replay_is_drift(self) -> None:
        row = CrawlerRow(
            facts=_facts(),
            design=DesignFacts(granularity=Granularity.DATE, empty=EmptySemantics.COLLAPSED),
        )

        assert any("should have a replay handler" in message for message in verify_row(row))

    def test_a_consistent_row_reports_nothing(self) -> None:
        row = CrawlerRow(
            facts=_facts(
                module="award_crawler",
                transports=frozenset({Transport.CRAWLER_HTTP_CLIENT}),
                uses_crawl_result=True,
                ledger=True,
                dead_letter=True,
            ),
            design=DesignFacts(granularity=Granularity.SOURCE, empty=EmptySemantics.TYPED),
        )

        assert row.fully_adopted
        assert verify_row(row) == []


class TestTheDeclaredPriorityPointsAtLiveWork:
    """A priority naming an adopted crawler misdirects while looking deliberate."""

    def test_the_current_priority_is_not_drift(self, matrix: AdoptionMatrix) -> None:
        assert matrix.drift == () or not any("declared migration priority" in message for message in matrix.drift)

    def test_an_adopted_name_is_reported(self) -> None:
        rows = [_row(module) for module in ("award_crawler", *PRIORITY_ORDER)]

        problems = verify_priority_order(rows, ("award_crawler",))

        assert any("already adopted" in message and "award_crawler" in message for message in problems), problems

    def test_a_name_that_is_not_a_crawler_is_reported(self) -> None:
        problems = verify_priority_order([_row("baserunning_stats_crawler")], ("ghost_crawler",))

        assert any("no such crawler" in message for message in problems), problems

    def test_a_live_name_is_not_reported(self) -> None:
        rows = [_row(module) for module in PRIORITY_ORDER]

        assert verify_priority_order(rows, PRIORITY_ORDER) == []

    def test_the_declared_order_leads_the_computed_one(self, matrix: AdoptionMatrix) -> None:
        """A declared order that the computed order contradicts is a preference, not a plan."""
        order = [row.module for row in matrix.roadmap()]

        assert order[: len(PRIORITY_ORDER)] == list(PRIORITY_ORDER)


class TestAdvisories:
    def test_two_http_paths_are_advised(self) -> None:
        row = CrawlerRow(facts=_facts(transports=frozenset({Transport.CRAWLER_HTTP_CLIENT, Transport.RAW_HTTPX})))

        assert any("second path is unused" in message for message in advise_row(row))

    def test_manual_throttle_alongside_the_shared_client_is_drift(self) -> None:
        """The shared client already waits, so a second wait is a defect."""
        row = CrawlerRow(facts=_facts(transports=frozenset({Transport.CRAWLER_HTTP_CLIENT}), owns_throttle=True))

        assert any("throttles manually" in message for message in verify_row(row))

    def test_classifying_without_recording_is_advised(self) -> None:
        row = CrawlerRow(facts=_facts(uses_crawl_result=True))

        assert any("leave no trace" in message for message in advise_row(row))

    def test_an_entrypoint_with_no_transport_is_advised(self) -> None:
        """A blank transport is more likely a detection gap than a crawler that
        fetches nothing, so the matrix says so instead of asserting it.
        """
        row = CrawlerRow(facts=_facts(transports=frozenset()))

        assert any("check the classifier" in message for message in advise_row(row))

    def test_a_clean_row_is_not_advised(self) -> None:
        row = CrawlerRow(
            facts=_facts(
                transports=frozenset({Transport.CRAWLER_HTTP_CLIENT}),
                uses_crawl_result=True,
                ledger=True,
                dead_letter=True,
            ),
        )

        assert advise_row(row) == []


#: Crawlers that satisfy every axis the matrix tracks. Kept as one constant so a
#: new canary does not have to be chased through the summary and the roadmap
#: separately.
FULLY_ADOPTED = (
    "award_crawler",
    "food_crawler",
    "game_detail_crawler",
    "kbo_event_crawler",
    "parking_crawler",
    "player_movement_crawler",
    "relay_crawler",
    "roster_transaction_crawler",
    "schedule_crawler",
    "team_history_crawler",
)

#: Crawlers that close the ledger/dead-letter/replay chain without any HTTP
#: client. Kept as a list because the transport criterion is the claim under test.
BROWSER_FIRST_CHAINED = (
    "kbo_event_crawler",
    "player_movement_crawler",
    "team_history_crawler",
)


class TestOwnsTransportIsNotSharedHttp:
    """A browser-first crawler has no HTTP client to share.

    ``shared_http`` was the adoption criterion, which scored the three crawlers
    that close the whole reliability chain on Playwright as half-converted --
    inheriting ``BaseHttpCrawler`` would not have helped them, since they never
    make an HTTP request. What matters is the absence of a second, ungoverned
    request path, so that is the question asked.
    """

    @pytest.mark.parametrize("module", BROWSER_FIRST_CHAINED)
    def test_a_playwright_crawler_with_one_path_governs_its_transport(self, module: str) -> None:
        facts = scan_module(module)

        assert facts.shared_http is False
        assert facts.owns_transport is True

    @pytest.mark.parametrize("module", BROWSER_FIRST_CHAINED)
    def test_each_browser_crawler_now_closes_the_chain(self, module: str) -> None:
        """All three drove a browser and all three carry their own vocabulary.

        ``CrawlResult`` models an HTTP fetch, so none of them could produce one;
        each states what its page said and whether retrying could still change
        it. That is the axis the migration report used to name as missing on
        work that was already finished.
        """
        row = _row(module)

        assert row.remaining_axes == ()
        assert row.fully_adopted is True


class TestAPageOutcomeCountsAsATypedOutcome:
    """A browser crawler's own vocabulary is the typed outcome, not a gap.

    ``kbo_event_crawler`` states that a page read cannot be described by an HTTP
    result type and carries ``KboEventPageRead`` instead. Reading only
    ``CrawlResult`` left a finished crawler at the top of the migration list
    with the gap named -- which is worse than not ranking it, because an
    operator following the report is sent to redo work someone already did.
    """

    def test_the_browser_crawler_that_adopted_one_is_typed(self) -> None:
        facts = scan_module("kbo_event_crawler")

        assert facts.uses_crawl_result is True
        assert "CrawlResult" not in _module_source("kbo_event_crawler")

    def test_and_therefore_closes_the_whole_chain(self) -> None:
        row = _row("kbo_event_crawler")

        assert row.remaining_axes == ()
        assert row.fully_adopted is True

    def test_a_status_field_alone_is_not_enough(self) -> None:
        """Either member can be hit by accident; together they mean something.

        A ``status`` field is a common name and an ``is_terminal`` property
        could describe anything, so a vocabulary carrying only one of them has
        not stated what happened *and* whether retrying could still change it.
        """
        source = "from dataclasses import dataclass\n@dataclass\nclass OnlyStatus:\n    status: str\n"
        assert not _looks_like_outcome_value(ast.parse(source).body[1])

    def test_a_terminal_flag_alone_is_not_enough(self) -> None:
        source = (
            "from dataclasses import dataclass\n"
            "@dataclass\nclass OnlyTerminal:\n"
            "    events: list\n"
            "    @property\n    def is_terminal(self) -> bool:\n        return True\n"
        )
        assert not _looks_like_outcome_value(ast.parse(source).body[1])

    def test_both_members_are_accepted(self) -> None:
        source = (
            "from dataclasses import dataclass\n"
            "@dataclass\nclass Both:\n"
            "    status: str\n"
            "    @property\n    def is_terminal(self) -> bool:\n        return True\n"
        )
        assert _looks_like_outcome_value(ast.parse(source).body[1])

    def test_a_class_the_crawler_does_not_import_does_not_count(self) -> None:
        """The vocabulary has to be reachable from the crawler, not merely present.

        Otherwise a helper module carrying one outcome class would mark every
        crawler that imports anything at all from its package as typed.
        """
        assert scan_module("text_relay_crawler").uses_crawl_result is False

    def test_an_import_from_outside_the_crawler_package_does_not_count(self) -> None:
        """The vocabulary has to be the repository's own contract.

        Any installed library could expose a dataclass with these two members;
        accepting one would make the gate a guess about somebody else's code.
        """
        source = "from requests.models import Response\n"
        assert not _imports_page_outcome_vocabulary(ast.parse(source))

    def test_a_crawler_carrying_raw_httpx_is_not_governed(self) -> None:
        """The second path is what the gate exists to catch, whatever else is true."""
        facts = _facts(transports=frozenset({Transport.PLAYWRIGHT, Transport.RAW_HTTPX}))

        assert facts.owns_transport is False

    def test_the_shared_client_alone_also_governs_the_transport(self) -> None:
        facts = _facts(transports=frozenset({Transport.CRAWLER_HTTP_CLIENT}))

        assert facts.owns_transport is True

    def test_a_crawler_with_no_recognized_path_governs_nothing(self) -> None:
        facts = _facts(transports=frozenset())

        assert facts.owns_transport is False


class TestRemainingAxesNameTheGap:
    def test_an_adopted_crawler_has_none(self, matrix: AdoptionMatrix) -> None:
        for row in matrix.adopted():
            assert row.remaining_axes == ()

    def test_an_untouched_crawler_names_every_missing_axis(self) -> None:
        """``seat_crawler`` drives nothing governed and records nothing.

        A crawler that reaches its source through a raw client it built itself
        has the widest gap, so it exercises every axis at once.
        """
        row = _row("seat_crawler")

        assert "run ledger" in row.remaining_axes
        assert "dead letter queue" in row.remaining_axes
        assert "replay handler" in row.remaining_axes

    def test_the_transport_axis_is_named_before_the_reliability_chain(self) -> None:
        """Order carries meaning: the transport is what blocks the rest."""
        row = _row("seat_crawler")

        assert row.remaining_axes[0].startswith("transport:")

    def test_a_browser_crawler_is_not_asked_for_a_transport_migration(self) -> None:
        """It drives a browser, which is already governed -- nothing to do there."""
        row = _row("broadcast_crawler")

        assert not any(axis.startswith("transport:") for axis in row.remaining_axes)

    def test_the_json_artifact_carries_the_gap(self, matrix: AdoptionMatrix) -> None:
        payload = {entry["module"]: entry for entry in matrix.to_dict()["rows"]}

        assert payload["broadcast_crawler"]["owns_transport"] is True
        assert "typed outcome: no CrawlResult/CrawlOutcome import" in payload["broadcast_crawler"]["remaining_axes"]

    def test_a_browser_crawler_with_its_own_vocabulary_has_no_gap_left(self, matrix: AdoptionMatrix) -> None:
        """The three browser-first crawlers carry page-outcome vocabularies.

        Each names what a *page read* meant rather than what an HTTP fetch
        returned, because a browser has no request to describe. Once that
        vocabulary exists the axis is closed, and reporting it open would send
        an operator to redo work that is finished.
        """
        payload = {entry["module"]: entry for entry in matrix.to_dict()["rows"]}

        for module in BROWSER_FIRST_CHAINED:
            assert payload[module]["remaining_axes"] == [], module

    def test_the_markdown_roadmap_says_why_not_just_who(self, matrix: AdoptionMatrix) -> None:
        rendered = render_markdown(matrix)

        assert "`baserunning_stats_crawler` -- typed outcome" in rendered


class TestRoadmap:
    def test_declared_priority_leads(self, matrix: AdoptionMatrix) -> None:
        """The schedule feeds nearly every other crawl, so it comes first even
        though other crawlers already satisfy more axes.
        """
        order = [row.module for row in matrix.roadmap()]

        assert order[: len(PRIORITY_ORDER)] == list(PRIORITY_ORDER)

    def test_adopted_crawlers_are_not_recommended_again(self, matrix: AdoptionMatrix) -> None:
        order = [row.module for row in matrix.roadmap()]

        for module in FULLY_ADOPTED:
            assert module not in order

    def test_data_only_modules_are_left_out(self, matrix: AdoptionMatrix) -> None:
        for row in matrix.roadmap():
            assert row.facts.has_transport
            assert row.facts.has_entrypoint

    def test_every_roadmap_entry_exists(self, matrix: AdoptionMatrix) -> None:
        modules = set(discover_modules())

        for row in matrix.roadmap():
            assert row.module in modules


class TestRendering:
    def test_markdown_lists_every_row(self, matrix: AdoptionMatrix) -> None:
        rendered = render_markdown(matrix)

        for row in matrix.rows:
            assert f"`{row.module}`" in rendered

    def test_markdown_shows_the_adopted_set(self, matrix: AdoptionMatrix) -> None:
        rendered = render_markdown(matrix)

        assert f"Fully adopted ({len(FULLY_ADOPTED)})" in rendered
        assert "`award_crawler`" in rendered

    def test_json_is_serializable(self, matrix: AdoptionMatrix) -> None:
        payload = json.loads(json.dumps(matrix.to_dict()))

        assert payload["summary"]["total"] == len(matrix.rows)
        assert payload["summary"]["fully_adopted"] == len(FULLY_ADOPTED)
        # An adopted crawler is not recommended for migration again.
        for module in FULLY_ADOPTED:
            assert module not in payload["roadmap"]
        assert payload["roadmap"][0] == PRIORITY_ORDER[0]

    def test_summary_counts_match_the_rows(self, matrix: AdoptionMatrix) -> None:
        summary = matrix.to_dict()["summary"]

        assert summary["drift"] == len(matrix.drift)
        assert summary["advisories"] == len(matrix.advisories)


class TestCli:
    def test_markdown_report_exits_zero(self, capsys: pytest.CaptureFixture[str]) -> None:
        from src.cli.reports.crawler_adoption_matrix import main

        assert main([]) == 0
        assert "crawler" in capsys.readouterr().out

    def test_json_report_is_valid_json(self, capsys: pytest.CaptureFixture[str]) -> None:
        from src.cli.reports.crawler_adoption_matrix import main

        assert main(["--format", "json"]) == 0
        assert json.loads(capsys.readouterr().out)["summary"]["total"] > 40

    def test_output_file_is_written(self, tmp_path: Path) -> None:
        from src.cli.reports.crawler_adoption_matrix import main

        target = tmp_path / "nested" / "matrix.md"

        assert main(["--output", str(target)]) == 0
        assert target.read_text(encoding="utf-8").startswith("| crawler |")

    def test_advisories_only_is_line_oriented(self, capsys: pytest.CaptureFixture[str]) -> None:
        from src.cli.reports.crawler_adoption_matrix import main

        assert main(["--advisories-only"]) == 0
        assert "|" not in capsys.readouterr().out

    def test_strict_turns_advisories_into_a_failure(self) -> None:
        from src.cli.reports.crawler_adoption_matrix import main

        assert main(["--strict"]) == 1

    def test_drift_is_reported_as_a_failure(
        self,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from src.cli.reports import crawler_adoption_matrix as cli

        broken = AdoptionMatrix(rows=(), drift=("something disagrees",))
        monkeypatch.setattr(cli, "build_matrix", lambda: broken)

        assert cli.main([]) == 1
        assert "something disagrees" in capsys.readouterr().err
