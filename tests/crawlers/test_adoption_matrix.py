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
    _HTTP_BASES,
    _PLAYWRIGHT_BASES,
    DECLARED,
    PRIORITY_ORDER,
    REPLAY_HANDLERS,
    AdoptionMatrix,
    Attribution,
    CrawlerRow,
    DesignFacts,
    EmptySemantics,
    Fallback,
    Granularity,
    ModuleFacts,
    Transport,
    advise_row,
    attribution_of,
    upstream_dependents_of,
    build_matrix,
    _imports_page_outcome_vocabulary,
    _looks_like_outcome_value,
    _model_class_names,
    _reaches_httpx_itself,
    _imported_model_names,
    _parse_or_none,
    _crawler_instances,
    _produced_names,
    discover_modules,
    render_markdown,
    scan_module,
    transports_of,
    upstream_reader_files,
    verify_priority_order,
    verify_row,
    written_models,
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


class TestOnlyRealModelClassesAreCounted:
    """A table is a declared ORM class, not anything imported from `src/models`.

    Those modules export module-level constants beside their classes --
    `crawl_execution` defines `RUN_STATUS_FAILED` next to
    `CrawlExecutionRun` -- and the import scan used to count either. That put
    ledger readers behind a crawler whose rows they never read, which inflated
    its upstream rank and moved it up the roadmap on readers that belong to
    nobody.
    """

    def test_a_constant_exported_beside_a_model_is_not_a_model(self) -> None:
        assert "CrawlExecutionRun" in _model_class_names()
        assert "RUN_STATUS_FAILED" not in _model_class_names()

    def test_importing_the_constant_alone_claims_no_table(self) -> None:
        source = "from src.models.crawl_execution import RUN_STATUS_FAILED, CrawlExecutionRun\n"

        assert _imported_model_names(source) == {"CrawlExecutionRun"}

    def test_a_crawler_importing_only_ledger_constants_is_not_credited_with_readers(self) -> None:
        """The regression this replaced: `award_crawler` reported six readers
        instead of three because two of them only import run-status constants.
        """
        assert written_models("award_crawler") == {"Award"}


class TestTheScanStaysFastEnoughToRun:
    """The report is a CI gate, so its runtime is part of its contract.

    Building the matrix walked the tree through independent lookups -- resolving
    a repository class, then its method, then the write function -- and parsed
    the same file for each. It took 85 seconds, which is long enough that the
    gate starts looking like a cost worth removing.
    """

    def test_building_the_matrix_completes_promptly(self, matrix: AdoptionMatrix) -> None:
        import time

        started = time.monotonic()
        _parse_or_none.cache_clear()
        build_matrix()
        elapsed = time.monotonic() - started

        assert elapsed < 30, f"build_matrix took {elapsed:.1f}s; the uncached scan took 85s"
        assert matrix.rows


class TestAnUntracedCallerDoesNotDemoteAMeasuredCrawler:
    """The demotion for an unresolved write applies to having no measurement.

    Ranking demoted any crawler with an untraced caller, which buried the two
    all-series crawlers -- resolved to four tables with 85 and 84 readers each --
    below every crawler confirmed to feed nothing. The flag answers "this caller
    was not followed", not "this number is untrustworthy", and a crawler that
    resolved its tables from another caller carries a real count.

    The witnesses are chosen for carrying both facts at once, so the rule is
    pinned without depending on which callers happen to be untraced this week:
    the all-series crawlers were the original case but their callers now resolve.
    """

    #: Crawlers that resolved a table *and* have an untraced caller, so the two
    #: directions of the rule can be checked against the same rows.
    #:
    #: Both were rewitnessed when the save-flag rule resolved the schedule
    #: crawler's callers, and again when the roster transaction's callers resolved,
    #: leaving ``relay_crawler`` and ``game_detail_crawler`` as the pairs that still
    #: carry an untraced caller next to a real reader count. The set is expected
    #: to shrink as callers resolve; a witness that silently went quiet would stop
    #: testing the rule at all.
    MEASURED_BUT_PARTLY_UNTRACED = ("relay_crawler", "game_detail_crawler")

    def test_a_measured_crawler_is_not_demoted_for_an_untraced_caller(self) -> None:
        for module in self.MEASURED_BUT_PARTLY_UNTRACED:
            attribution = attribution_of(module)
            assert attribution.models, f"{module} resolved no table, so the demotion is warranted"
            assert attribution.unresolved_callers, f"{module} no longer has the untraced callers this guards"
            assert attribution.readers > 0, f"{module} has no measured readers, so it is not the witness"

    def test_the_widest_measured_crawlers_lead_the_roadmap(self, matrix: AdoptionMatrix) -> None:
        order = [row.module for row in matrix.roadmap()]
        impact = {row.module: row.facts.upstream_dependents for row in matrix.rows}
        in_roadmap = [impact[module] for module in order]

        assert in_roadmap == sorted(in_roadmap, reverse=True), (
            "the roadmap ranks a quiet crawler above one with measured readers"
        )

    def test_a_quiet_crawler_is_still_last_among_the_unmeasured(self) -> None:
        """The demotion it replaced still holds for a crawler with no reading."""
        assert not written_models("base_naver_crawler")


class TestADelegatedWriteIsFollowedOneHopFurther:
    """A writer that only coordinates still names the rows it fills.

    The pregame writer aggregates context and then hands the rows to
    ``save_pregame_lineups``, so its own body constructs nothing. Reading only
    that body scored the preview crawler zero for the lineups it publishes, and
    a zero on the ranking axis is the difference between first and last. The
    extra hop is bounded at one so a write three modules away stays
    unattributable rather than being credited to whoever called it.
    """

    def test_the_coordinating_writer_names_no_table_of_its_own(self) -> None:
        """If this stops being true the hop below is no longer what saves it."""
        from src.crawlers.adoption_matrix import _function_body as lookup
        from src.crawlers.adoption_matrix import _models_in_write_body, _with_local_helpers

        writer = Path(__file__).resolve().parents[2] / "src" / "services" / "pregame_context_writer.py"
        definition = lookup(writer, "save_preview_contexts")

        assert definition is not None
        assert _models_in_write_body(_with_local_helpers(writer, definition), "save_preview_contexts") == set()

    def test_the_delegated_write_restores_them(self) -> None:
        written = written_models("preview_crawler")

        assert written == {"Game", "GameLineup", "GameMetadata", "GameSummary"}
        assert upstream_dependents_of("preview_crawler") > 0


class TestUpstreamImpactRanking:
    """A crawler whose data feeds many readers is migrated before a quiet one.

    The declared order claims to rank by upstream damage -- "a silent failure
    in any of them poisons whatever reads it" -- but the roadmap was sorted by
    nearness alone, which inverts that claim exactly where it matters. The PBP
    crawler fills the table the readiness gate, the SLA tracker, the gap report
    and the RAG index all read, and it sat at position 26 because it had closed
    the fewest axes, while a crawler feeding nothing ranked first.

    The axis is derived rather than declared for the same reason the capability
    axes are: a hand-maintained list of "important" crawlers goes stale silently,
    and a stale list still renders as a deliberate plan.
    """

    def test_a_widely_read_crawler_outranks_a_quiet_one(self, matrix: AdoptionMatrix) -> None:
        rows = {row.module: row for row in matrix.rows}

        assert rows["pbp_crawler"].facts.upstream_dependents > 0
        assert rows["pbp_crawler"].facts.upstream_dependents > rows["broadcast_crawler"].facts.upstream_dependents

    def test_the_roadmap_leads_with_the_widest_upstream_impact(self, matrix: AdoptionMatrix) -> None:
        """The leader is the widest-impact crawler still waiting, not the cheapest.

        The witness this was written against -- ``pbp_crawler`` at position 26,
        behind a crawler feeding nothing -- is adopted now, so the position is
        asserted against whatever leads instead: the crawler at the top must be
        the one with the most measured readers left in the roadmap. That holds
        whether or not a particular crawler has been migrated, which the old
        position check stopped doing the moment its witness was finished.
        """
        order = [row.module for row in matrix.roadmap()]
        impact = {row.module: row.facts.upstream_dependents for row in matrix.rows}
        leader = order[0]

        assert impact[leader] == max(impact[module] for module in order), (
            f"{leader} leads the roadmap while a quieter crawler was available"
        )

    def test_upstream_impact_is_measured_from_real_readers(self) -> None:
        """The count comes from who reads the tables, not from a curated list."""
        facts = scan_module("pbp_crawler")

        # Callers own the writes for these crawlers, so the readers are found
        # through the caller's own model imports rather than the crawler's.
        assert facts.upstream_dependents >= 10

    def test_a_crawler_nobody_reads_scores_zero(self) -> None:
        facts = _facts(module="quiet_crawler", upstream_dependents=0)

        assert facts.upstream_dependents == 0

    def test_a_module_reading_several_tables_counts_once(self) -> None:
        """A reader is a module, not a (module, table) pair.

        Summing per-model reader counts counted the same file once for every
        model it imported: the PBP crawler measured 143 against 82 distinct
        files, and the inflation scaled with how many tables a reader touched
        rather than with how many readers there were. Since the number orders the
        roadmap, it had to count modules.
        """
        for module in ("pbp_crawler", "player_search_crawler", "team_event_crawler"):
            facts = scan_module(module)
            distinct = upstream_reader_files(module)

            assert facts.upstream_dependents == len(distinct), f"{module} double-counts readers"

    def test_a_crawler_that_writes_through_a_repository_is_not_scored_zero(self) -> None:
        """A repository import names the table; it is not a model import.

        Only ``src.models`` imports were read, so a crawler persisting through
        ``StadiumSeatSectionRepository`` looked as though it fed nothing at all.
        The seat crawler is a real dependency of the seat map, and it was scored
        0 because the writer named its table through a repository.
        """
        facts = scan_module("seat_crawler")

        assert "StadiumSeatSection" in written_models("seat_crawler")
        assert facts.upstream_dependents > 0

    def test_a_shared_caller_does_not_bless_every_crawler_it_touches(self) -> None:
        """Two crawlers driven by one pipeline step are told apart.

        ``advanced_daily_steps.py`` imports the fielding and baserunning models
        and calls both crawlers. Every model in that file was credited to both,
        so each crawler claimed the other's readers. The step knows which result
        goes where -- the repository it hands each one to -- and that is the only
        evidence that can separate them.
        """
        assert written_models("baserunning_stats_crawler") == {"PlayerSeasonBaserunning"}
        assert written_models("fielding_stats_crawler") == {"PlayerSeasonFielding"}

    def test_another_crawlers_result_stops_at_the_derivation(self) -> None:
        """Deriving from one crawler's rows does not adopt another's fetch.

        ``live_crawler`` walks the schedule crawler's games and, inside that
        loop, awaits the Naver relay crawler. Following every name derived from
        the schedule rows treated the relay payload as schedule output, so the
        schedule crawler was credited with the play-by-play tables -- the same
        reading it would have had if the relay crawler were the one driving the
        write. A value produced by a *different* crawler call ends the trace.
        """
        schedule = written_models("schedule_crawler")
        pbp = written_models("pbp_crawler")

        assert "GamePlayByPlay" not in schedule, f"schedule claimed the relay tables: {sorted(schedule)}"
        assert "GamePlayByPlay" in pbp

    def test_derivation_does_not_cross_a_function_boundary(self) -> None:
        """A helper's local names are not the caller's values.

        The schedule rows are walked in one function and the writes happen in
        another, so following names across every definition in the file pulled in
        the game detail and PBP fetches that unrelated functions perform. A name
        travels only where it is passed or returned.
        """
        schedule = written_models("schedule_crawler")

        assert "GameEvent" not in schedule
        assert "GameInningScore" not in schedule

    def test_only_the_called_repository_method_is_credited(self) -> None:
        """A repository class writes several tables; a call writes one path.

        ``PlayerRepository`` constructs ``Player``, ``PlayerIdentity`` and
        ``PlayerMovement`` in three different methods. The profile collector
        calls ``upsert_player_profile``, which reaches the first two and not the
        third -- so the class-wide view credited the crawler with movement rows
        written by ``save_player_movements`` for a different caller, and the
        crawler scored against readers of a table it never fills.

        The two it does write are kept: that method calls ``_upsert_identity``, so
        the identity row is genuinely its output.
        """
        assert written_models("player_profile_crawler") == {"Player", "PlayerIdentity"}

    def test_a_crawler_is_not_charged_for_a_table_another_crawler_writes(self) -> None:
        """Two crawlers driven by one repository method are told apart.

        The daily update moves players through ``PlayerRepository`` for both the
        movement crawler and the profile crawler. The movement rows belong to the
        crawler that fetched movements; crediting the profile crawler with them
        counted the same reader file twice under two different names.
        """
        assert "PlayerMovement" not in written_models("player_profile_crawler")
        assert "PlayerMovement" in written_models("player_movement_crawler")

    def test_a_repository_class_name_does_not_name_its_table(self) -> None:
        """The suffix is a naming convention, not evidence of the table.

        Stripping ``Repository`` off ``BroadcastRepository`` yields ``Broadcast``,
        which is not a model -- the class writes ``GameBroadcast``. The
        broadcast crawler was scored against a table name that exists nowhere in
        the ORM, so it measured zero readers for a table two modules actually
        read. The repository body is where the table is named.
        """
        written = written_models("broadcast_crawler")

        assert written == {"GameBroadcast"}
        assert upstream_dependents_of("broadcast_crawler") > 0

    def test_an_instance_method_result_reaches_its_writer(self) -> None:
        """A crawler is driven through an object, not through its class name.

        ``collect_profiles.py`` binds ``crawler = PlayerProfileCrawler()`` and
        calls ``crawler.crawl_player_profile(...)``; the preview batch does the
        same before handing the rows to ``save_pregame_lineups``. Only an
        assignment whose value is a bare ``Class(...)`` call was recognised, so
        neither crawler produced a result name and both scored zero -- the
        profile crawler feeds the player table and the preview feeds the
        published lineups.
        """
        profile = written_models("player_profile_crawler")
        preview = written_models("preview_crawler")

        assert "Player" in profile, f"profile crawler writes no player row: {profile}"
        assert "GameLineup" in preview, f"preview crawler writes no lineup row: {preview}"
        assert upstream_dependents_of("player_profile_crawler") > 0
        assert upstream_dependents_of("preview_crawler") > 0

    def test_a_writer_is_resolved_where_its_caller_imported_it(self) -> None:
        """An import names the defining module; a global search loses it.

        ``save_relay_data`` is defined twice under ``src/``. The caller says
        which one it means -- it imports the symbol from
        ``src.repositories.game_relay`` -- but the name was resolved by scanning
        every module, so the duplicate made it unattributable and the play-by-play
        crawler fell to zero. A caller's own import is the disambiguation.
        """
        written = written_models("pbp_crawler")

        assert "GamePlayByPlay" in written
        assert "GameEvent" in written

    def test_a_row_the_writer_bootstraps_is_not_the_crawler_output(self) -> None:
        """The recovery engine invents a game row; the crawler did not fetch it.

        ``_persist_events_and_pbp`` creates a ``Game`` stub when the parent row is
        missing, built from its own context and literals, before writing the
        events it was handed. Counting it as the crawler's output handed the
        crawler the entire game table -- read by every module that touches a
        game -- and pushed the play-by-play tables down the ranking behind it.
        """
        written = written_models("pbp_crawler")

        assert "Game" not in written, f"the bootstrapped row is not crawler output: {sorted(written)}"
        assert {"GameEvent", "GamePlayByPlay"} <= written

    def test_unattributed_output_is_reported_as_unknown_not_as_quiet(self) -> None:
        """Unresolved and genuinely unread are different answers.

        Both surface as ``upstream_dependents == 0`` today, and they point
        opposite ways: one means "look harder, the attribution failed" and the
        other means "nothing depends on this". Only the second may be presented
        as a measurement.
        """
        facts = scan_module("broadcast_crawler")

        assert facts.upstream_dependents > 0
        assert attribution_of("quiet_crawler").attributed is False

    def test_a_driver_that_hands_no_result_is_not_an_unresolved_caller(self) -> None:
        """Running a crawler is not the same as carrying its rows somewhere.

        ``crawl_phase1_extra`` constructs each Phase-1 crawler, calls ``run`` and
        binds no result: the crawler persists its own tables. The scan read the
        construction call itself as result-producing, so the instance name
        counted as output the caller had supposedly failed to follow, and every
        crawler it drives carried a phantom attribution gap. Nothing was missed
        -- the caller hands no rows anywhere -- so it must not be reported as an
        untraced write.
        """
        for module in ("broadcast_crawler", "fan_culture_crawler", "injury_crawler", "manager_change_crawler"):
            callers = attribution_of(module).unresolved_callers

            assert not any("crawl_phase1_extra" in caller for caller in callers), (
                f"{module} reports a driver as an untraced write: {callers}"
            )

    def test_constructing_a_crawler_is_not_producing_its_result(self) -> None:
        """An instance begins at the constructor; rows do not appear there.

        The same confusion seen from the inside: ``crawler = BroadcastCrawler()``
        put ``crawler`` in the result set, so a later write that merely *mentions*
        the instance -- ``repo.save(crawler.game_id)`` -- read as the crawler
        handing its output to that repository. The constructor is tracked as an
        instance, never as output.
        """
        source = (
            "from src.crawlers.broadcast_crawler import BroadcastCrawler\n"
            "\n"
            "\n"
            "async def run() -> None:\n"
            "    crawler = BroadcastCrawler()\n"
            "    await crawler.run(save=True)\n"
        )
        tree = ast.parse(source)
        symbols = {"BroadcastCrawler"}

        assert _crawler_instances(tree, symbols) == {"crawler"}
        assert _produced_names(tree, symbols) == set()

    def test_a_caller_that_asks_the_crawler_to_save_is_not_untraced(self) -> None:
        """Binding a result the crawler already persisted is not a missed write.

        ``run_daily_update`` reads the rows back from ``roster_transaction_crawler``
        only to count them -- ``len(transactions)`` into the step summary. The
        persistence happened inside the crawler's own ``if save:`` branch, one
        frame from the fetch, and the table it fills is already the crawler's.
        Reporting the caller as an untraced write said a write had been missed
        when the write had already been resolved -- the analysis failure this
        flag exists to surface would cover up the real one.
        """
        for module in ("roster_transaction_crawler", "team_event_crawler", "ticket_crawler"):
            callers = [caller for caller in attribution_of(module).unresolved_callers if "run_daily_update" in caller]

            assert not callers, f"{module} is reported untraced by a caller that only counts its rows: {callers}"

    def test_rows_reaching_a_writer_as_keyword_arguments_are_attributed(self) -> None:
        """A writer handed its rows by name is as reached as one handed a position.

        ``relay_recovery_engine`` builds events with the PBP crawler's static
        helpers and persists them from a method that receives ``canonical_events``
        and ``raw_pbp_rows`` as parameters, then calls
        ``save_relay_data(events=..., raw_pbp_rows=...)``. Every argument is named
        and none is positional, so the trace stopped at the signature: the play-by-play
        crawler measured one table instead of four and fell below crawlers feeding
        far less. A name is a delivery just as much as a position is.
        """
        written = written_models("pbp_crawler")

        assert {"GameEvent", "GamePlayByPlay"} <= written, f"keyword-delivered rows went untraced: {sorted(written)}"

    def test_a_caller_that_hands_a_result_to_a_writer_is_still_untraced_when_it_goes_nowhere(self) -> None:
        """Turning the crawler's persistence on does not silence every other write.

        Guarding the rule above. A caller that requests saving *and* hands the
        rows to a writer of its own is doing both, and the second half is exactly
        the unresolved write this flag exists to report. Keying the exemption on
        the presence of a ``save`` argument alone would trade that true positive
        for a false silence.
        """
        source = (
            "from src.crawlers.roster_transaction_crawler import RosterTransactionCrawler\n"
            "\n"
            "\n"
            "async def collect() -> None:\n"
            "    crawler = RosterTransactionCrawler()\n"
            "    rows = await crawler.run(save=True, target_date='2026-01-01')\n"
            "    store_elsewhere(rows)\n"
        )
        tree = ast.parse(source)
        run_call = next(
            node for node in ast.walk(tree) if isinstance(node, ast.Call) and ast.unparse(node.func).endswith(".run")
        )

        assert _produced_names(tree, {"RosterTransactionCrawler"}) == {"rows"}, (
            "the fixture must bind a result, or it proves nothing about unresolved writes"
        )
        assert any(keyword.arg == "save" for keyword in run_call.keywords), (
            "the fixture must request saving, which is what the exemption reads"
        )

    def test_upstream_impact_actually_moves_the_order(self) -> None:
        """A synthetic row proves the key is load-bearing rather than decorative.

        Two rows equal on every other axis, differing only in how many readers
        depend on them, must come back in that order. Without this the axis could
        be computed and then never read, and every other assertion would still
        pass on a ranking that ignored it.
        """
        rows = [
            CrawlerRow(_facts(module="quiet_one", upstream_dependents=0)),
            CrawlerRow(_facts(module="widely_read", upstream_dependents=40)),
            CrawlerRow(_facts(module="also_quiet", upstream_dependents=1)),
        ]
        built = AdoptionMatrix(rows=tuple(rows))

        assert [row.module for row in built.roadmap()][:2] == ["widely_read", "also_quiet"]

    def test_a_declared_order_cannot_promote_a_quiet_crawler_over_a_damaging_one(self) -> None:
        """The declared order may not invert the ranking it claims to justify.

        ``PRIORITY_ORDER`` short-circuits the computed order, so a stale entry
        overrides the axis instead of the other way round -- which is how a pair
        of crawlers feeding 16 modules and none came to lead a report whose
        stated rule is upstream damage. A declared name has to beat the computed
        impact, not merely exist.
        """
        rows = [
            CrawlerRow(_facts(module="quiet_but_declared", upstream_dependents=0)),
            CrawlerRow(_facts(module="widely_read", upstream_dependents=40)),
        ]
        problems = verify_priority_order(rows, order=("quiet_but_declared",))

        assert problems == [
            (
                "quiet_but_declared: declared migration priority but feeds 0 downstream "
                "modules while widely_read feeds 40"
            ),
        ]

    def test_a_declared_order_matching_the_impact_is_accepted(self) -> None:
        rows = [
            CrawlerRow(_facts(module="widely_read", upstream_dependents=40)),
            CrawlerRow(_facts(module="quiet_one", upstream_dependents=0)),
        ]

        assert verify_priority_order(rows, order=("widely_read",)) == []

    def test_the_declared_order_is_not_left_pointing_at_the_wrong_end(self) -> None:
        """Whatever remains declared must survive the axis it is now judged by."""
        matrix = build_matrix()
        assert verify_priority_order(list(matrix.rows)) == []


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

    def test_a_base_http_crawler_reports_its_governed_path(self) -> None:
        """What matters is that the path is governed, not which flavour it is.

        ``ticket_crawler`` used to build its own ``httpx`` client and satisfied
        this assertion as a statement about the raw path. It has since been
        moved onto the shared client, so the row now reports the governed
        client instead. Asserting the old value would fail on a crawler that is
        further along the migration than the assertion remembers, which is the
        failure mode of pinning a test to a fact that only ever goes one way.
        """
        facts = scan_module("ticket_crawler")

        assert facts.base_class == "BaseHttpCrawler"
        assert facts.owns_transport is True
        assert Transport.CRAWLER_HTTP_CLIENT in facts.transports

    def test_transport_order_is_stable(self) -> None:
        facts = scan_module("food_crawler")

        assert transports_of(facts)[0] is Transport.CRAWLER_HTTP_CLIENT

    def test_a_data_only_module_reports_no_transport(self) -> None:
        facts = scan_module("draft_history_crawler")

        assert facts.has_transport

    def test_an_http_base_is_not_a_browser_transport(self) -> None:
        """A base class inherits what it actually inherits, not what it is listed under.

        ``NaverNewsCrawlerBase`` subclasses ``BaseHttpCrawler`` and reaches the
        Naver API over the shared client. Registering it among the bases that
        supply a browser made every subclass of it -- the foreign-player,
        injury and manager-change crawlers -- report ``Transport.PLAYWRIGHT``,
        so ``shared_http`` read ``False`` for crawlers that do use the shared
        client and ``owns_transport`` answered the wrong question about them.

        None of those three is adopted, so the miscount never reached the
        headline number. That is exactly why it survived: the report stayed
        plausible while the facts underneath it stopped describing the code.
        """
        for module in ("foreign_player_crawler", "injury_crawler", "manager_change_crawler"):
            facts = scan_module(module)

            assert facts.base_class == "NaverNewsCrawlerBase"
            assert Transport.PLAYWRIGHT not in facts.transports, f"{module} does not drive a browser"
            # The client is built and used in the base, so this module's own
            # source names neither -- the inherited path is recorded separately.
            assert facts.shared_http is False
            assert facts.inherited_shared_http is True
            assert facts.owns_transport is True

    def test_the_shared_http_base_is_classified_by_what_it_inherits(self) -> None:
        """The classification follows the real base, so a list cannot drift from the code."""
        assert "NaverNewsCrawlerBase" not in _PLAYWRIGHT_BASES
        assert "NaverNewsCrawlerBase" not in _HTTP_BASES


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

    def test_an_own_result_type_does_not_make_a_crawler_typed(self) -> None:
        """The dataclass name is not the evidence; the import is.

        ``external_stats_crawler`` has always carried its own
        ``ExternalCrawlResult``, which is what this test originally used it to
        show. It is no longer a valid witness: the crawler now imports
        ``CrawlOutcome`` and branches on it, so it *is* typed, and for a reason
        that has nothing to do with the name of its own dataclass. Asserting
        otherwise would have required deleting a real migration to keep a test
        green. The untyped case stays covered by ``text_relay_crawler`` above,
        which still has only its own vocabulary.
        """
        facts = scan_module("external_stats_crawler")
        source = _module_source("external_stats_crawler")

        assert "ExternalCrawlResult" in source
        assert "from src.crawlers.result import" in source
        assert facts.uses_crawl_result is True

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
    "pbp_crawler",
    "player_movement_crawler",
    "player_pitching_all_series_crawler",
    "player_batting_all_series_crawler",
    "preview_crawler",
    "realtime_issue_crawler",
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
        """Order carries meaning: the transport is what blocks the rest.

        ``fan_culture_crawler`` is the row that owes the transport axis: it
        carries no governed request path and no reliability chain either, so the
        axis that blocks the rest has to be the one named. ``seat_crawler`` used
        to serve here and no longer does -- it composes the shared client
        directly and is left owing only the reliability chain, which is the
        correct reading rather than a regression.
        """
        row = _row("fan_culture_crawler")

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
        # Declared names lead when there are any; otherwise the order is the
        # computed one, so the first entry is the widest-impact crawler left.
        if PRIORITY_ORDER:
            assert payload["roadmap"][0] == PRIORITY_ORDER[0]
        else:
            rows = {entry["module"]: entry for entry in payload["rows"]}
            # Only an unmeasured crawler is demoted. A crawler with an untraced
            # caller but a resolved table still ranks by its real reader count,
            # so the widest reader is the leader either way.
            measured = [
                name
                for name in payload["roadmap"]
                if attribution_of(name).models or not attribution_of(name).unresolved_callers
            ]
            assert measured, "the roadmap must not be entirely unresolved attribution"
            assert payload["roadmap"][0] == max(
                measured,
                key=lambda name: rows[name]["upstream_dependents"],
            )

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
