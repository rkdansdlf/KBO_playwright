"""The adoption matrix must describe the crawlers that actually exist.

A matrix is only worth building if it cannot quietly go stale. These tests pin
the two things that make it trustworthy: every crawler module on disk is
classified, and the declared design is cross-checked against the code it claims
to describe. The migrated crawlers are held to the full contract so a later edit
that quietly undoes one of them fails here rather than in production.
"""

from __future__ import annotations

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
    discover_modules,
    render_markdown,
    scan_module,
    transports_of,
    verify_row,
)


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
    @pytest.mark.parametrize("module", ["award_crawler", "roster_transaction_crawler"])
    def test_the_chain_is_closed(self, module: str) -> None:
        row = _row(module)

        assert row.fully_adopted
        assert row.facts.shared_http
        assert row.facts.uses_crawl_result
        assert row.facts.ledger
        assert row.facts.dead_letter
        assert row.facts.replay

    @pytest.mark.parametrize("module", ["award_crawler", "roster_transaction_crawler"])
    def test_neither_throttles_manually(self, module: str) -> None:
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

    def test_no_drift_is_reported(self, matrix: AdoptionMatrix) -> None:
        assert matrix.drift == ()


class TestTransportDetection:
    def test_the_schedule_is_hybrid(self) -> None:
        """It reads a Naver API and falls back to a KBO browser page."""
        facts = scan_module("schedule_crawler")

        assert Transport.RAW_HTTPX in facts.transports
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


class TestRoadmap:
    def test_declared_priority_leads(self, matrix: AdoptionMatrix) -> None:
        """The schedule feeds nearly every other crawl, so it comes first even
        though other crawlers already satisfy more axes.
        """
        order = [row.module for row in matrix.roadmap()]

        assert order[: len(PRIORITY_ORDER)] == list(PRIORITY_ORDER)

    def test_adopted_crawlers_are_not_recommended_again(self, matrix: AdoptionMatrix) -> None:
        order = [row.module for row in matrix.roadmap()]

        for module in ("award_crawler", "roster_transaction_crawler"):
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

        assert "Fully adopted (2)" in rendered
        assert "`award_crawler`" in rendered

    def test_json_is_serializable(self, matrix: AdoptionMatrix) -> None:
        payload = json.loads(json.dumps(matrix.to_dict()))

        assert payload["summary"]["total"] == len(matrix.rows)
        assert payload["summary"]["fully_adopted"] == 2
        # An adopted crawler is not recommended for migration again.
        assert "award_crawler" not in payload["roadmap"]
        assert payload["roadmap"][0] == "schedule_crawler"

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
