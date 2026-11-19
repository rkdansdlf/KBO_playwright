"""Contract tests for the retrieval-necessity audit.

The number this produces is the argument for or against keeping semantic
retrieval at all, so the tests pin the two ways it could lie: counting a table
that came from prose as if a SQL query could have served it, and quietly folding
a case it could not classify into one side or the other.

The bucket boundaries are read from the code that owns them. A test that
asserted a hardcoded list here would keep passing while the real list drifted,
which is exactly the failure this audit exists to measure elsewhere.
"""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from src.cli.rag import audit_retrieval_necessity as necessity
from src.cli.rag.build_rag_index import _MARKDOWN_SOURCE_TABLES, _SOURCE_TABLE_BY_SOURCE
from src.services.retrieval_evaluation import GoldenQuery, load_golden_queries

if TYPE_CHECKING:
    from collections.abc import Iterator

    from src.cli.rag.audit_retrieval_necessity import NecessityReport

RETRIEVER_SOURCE = Path(necessity.__file__).resolve().parents[2] / "services" / "structured_retriever.py"
PRODUCTION_DATASET = Path("Docs/references/rag_golden_queries.json")
ROUTING_DATASET = Path("Docs/references/rag_routing_golden_queries.json")


@pytest.fixture(autouse=True)
def _clear_derivation_caches() -> Iterator[None]:
    """Keep a patched derivation from leaking into the next test."""
    necessity.prose_source_tables.cache_clear()
    necessity.indexed_source_tables.cache_clear()
    necessity.structured_source_tables.cache_clear()
    yield
    necessity.prose_source_tables.cache_clear()
    necessity.indexed_source_tables.cache_clear()
    necessity.structured_source_tables.cache_clear()


def _write_dataset(path: Path, payload: list[dict[str, object]]) -> Path:
    """Write a golden dataset fixture and return its path."""
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _case(source_table: str | None, case_id: str = "case") -> GoldenQuery:
    """Build a golden query that declares (or omits) the table it came from."""
    filters = {"source_table": source_table} if source_table is not None else {}
    return GoldenQuery(query="q", relevant_chunk_ids=(), filters=filters or None, case_id=case_id)


class TestTheBoundariesAreReadFromTheCodeThatOwnsThem:
    """A copied list is a fourth place to forget."""

    def test_the_audit_imports_the_builds_own_source_maps(self) -> None:
        tree = ast.parse(Path(necessity.__file__).read_text(encoding="utf-8"))
        imported: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and (node.module or "").endswith("build_rag_index"):
                imported.update(alias.name for alias in node.names)
        assert {"_MARKDOWN_SOURCE_TABLES", "_SOURCE_TABLE_BY_SOURCE"} <= imported

    def test_the_prose_set_follows_the_build_constant(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(necessity, "_MARKDOWN_SOURCE_TABLES", ("markdown_docs", "kbo_regulations"))
        necessity.prose_source_tables.cache_clear()
        assert necessity.prose_source_tables() == frozenset({"markdown_docs", "kbo_regulations"})

    def test_the_prose_set_matches_the_build_today(self) -> None:
        assert necessity.prose_source_tables() == frozenset(_MARKDOWN_SOURCE_TABLES)

    def test_the_indexed_set_matches_the_build_today(self) -> None:
        assert necessity.indexed_source_tables() == frozenset(_SOURCE_TABLE_BY_SOURCE.values())

    def test_structured_coverage_is_read_out_of_the_retriever_source(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake = tmp_path / "structured_retriever.py"
        fake.write_text('RESULT = build(source_table="brand_new_table")\n', encoding="utf-8")
        monkeypatch.setattr(necessity, "_STRUCTURED_RETRIEVER_PATH", fake)
        necessity.structured_source_tables.cache_clear()
        assert necessity.structured_source_tables() == frozenset({"brand_new_table"})

    def test_structured_coverage_matches_the_literals_in_the_real_module(self) -> None:
        text = RETRIEVER_SOURCE.read_text(encoding="utf-8")
        declared = set(re.findall(r'source_table="([^"]+)"', text))
        assert declared, "the retriever must name a table on each result it builds"
        assert necessity.structured_source_tables() == declared

    def test_every_structured_table_is_one_the_build_indexes(self) -> None:
        stray = necessity.structured_source_tables() - necessity.indexed_source_tables()
        assert not stray, f"structured retriever answers from tables the build never writes: {sorted(stray)}"


class TestAnEmptyDerivationIsRefused:
    """An empty set would classify every case as uncovered and look like a finding."""

    def test_the_build_without_a_prose_list_is_an_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(necessity, "_MARKDOWN_SOURCE_TABLES", ())
        necessity.prose_source_tables.cache_clear()
        with pytest.raises(RuntimeError, match="yielded no source tables"):
            necessity.prose_source_tables()

    def test_the_build_without_a_source_map_is_an_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(necessity, "_SOURCE_TABLE_BY_SOURCE", {})
        necessity.indexed_source_tables.cache_clear()
        with pytest.raises(RuntimeError, match="yielded no source tables"):
            necessity.indexed_source_tables()

    def test_a_retriever_that_names_no_table_is_an_error(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake = tmp_path / "structured_retriever.py"
        fake.write_text("RESULT = build()\n", encoding="utf-8")
        monkeypatch.setattr(necessity, "_STRUCTURED_RETRIEVER_PATH", fake)
        necessity.structured_source_tables.cache_clear()
        with pytest.raises(RuntimeError, match="yielded no source tables"):
            necessity.structured_source_tables()


class TestClassification:
    """Which bucket a table falls in decides the verdict, so each one is pinned."""

    def test_a_markdown_table_is_prose(self) -> None:
        assert necessity.classify_source_table("kbo_regulations") is necessity.RetrievalNecessity.PROSE

    def test_a_table_with_a_retriever_branch_is_sql_covered(self) -> None:
        assert necessity.classify_source_table("player_season_batting") is necessity.RetrievalNecessity.SQL_COVERED

    def test_an_indexed_table_without_a_branch_needs_one(self) -> None:
        assert necessity.classify_source_table("team_history") is necessity.RetrievalNecessity.RELATIONAL_NO_TEMPLATE

    def test_a_table_the_build_never_writes_is_unknown(self) -> None:
        assert necessity.classify_source_table("not_a_real_table") is necessity.RetrievalNecessity.UNKNOWN

    @pytest.mark.parametrize("value", [None, "", "   "])
    def test_a_missing_table_is_unknown(self, value: str | None) -> None:
        assert necessity.classify_source_table(value) is necessity.RetrievalNecessity.UNKNOWN

    def test_prose_wins_when_a_table_is_in_both_sets(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(necessity, "prose_source_tables", lambda: frozenset({"kbo_regulations"}))
        monkeypatch.setattr(necessity, "structured_source_tables", lambda: frozenset({"kbo_regulations"}))
        assert necessity.classify_source_table("kbo_regulations") is necessity.RetrievalNecessity.PROSE

    def test_a_case_that_declares_nothing_is_unknown_not_prose(self) -> None:
        verdicts = necessity.classify_queries([_case(None, "no_table")])
        assert verdicts[0].source_table is None
        assert verdicts[0].necessity is necessity.RetrievalNecessity.UNKNOWN

    def test_a_case_that_declares_a_table_carries_it_through(self) -> None:
        verdicts = necessity.classify_queries([_case("awards", "award_1")])
        assert verdicts[0].source_table == "awards"
        assert verdicts[0].necessity is necessity.RetrievalNecessity.RELATIONAL_NO_TEMPLATE

    def test_a_case_without_an_id_still_gets_a_stable_one(self) -> None:
        verdicts = necessity.classify_queries([GoldenQuery(query="q", relevant_chunk_ids=())])
        assert verdicts[0].case_id == "case_000"


class TestTheDecisionBands:
    """The thresholds were agreed before the measurement, not after it."""

    @pytest.mark.parametrize(
        ("prose", "total", "expected"),
        [
            (0, 30, necessity.NecessityVerdict.SQL_EXPANSION_WARRANTED),
            (4, 30, necessity.NecessityVerdict.SQL_EXPANSION_WARRANTED),  # 13.3% is at or below the line
            (5, 30, necessity.NecessityVerdict.DECOMPOSE_BY_SOURCE_TABLE),  # 16.7%
            (15, 30, necessity.NecessityVerdict.KEEP_HYBRID),  # exactly 50%
            (30, 30, necessity.NecessityVerdict.KEEP_HYBRID),
        ],
    )
    def test_the_prose_share_selects_the_band(
        self,
        prose: int,
        total: int,
        expected: necessity.NecessityVerdict,
    ) -> None:
        verdict = necessity.decide_verdict(total=total, classified=total, prose=prose, unknown=0)
        assert verdict is expected

    def test_an_empty_dataset_has_no_verdict(self) -> None:
        assert necessity.decide_verdict(total=0, classified=0, prose=0, unknown=0) is (
            necessity.NecessityVerdict.UNDETERMINED
        )

    def test_too_many_unclassified_cases_suppress_the_verdict(self) -> None:
        # 3 of 30 unknown is 10%, which is the tolerance; 4 is over it.
        assert necessity.decide_verdict(total=30, classified=27, prose=27, unknown=3) is (
            necessity.NecessityVerdict.KEEP_HYBRID
        )
        assert necessity.decide_verdict(total=30, classified=26, prose=26, unknown=4) is (
            necessity.NecessityVerdict.UNDETERMINED
        )


class TestTheSharesAreTakenOverClassifiedCases:
    """A case nobody classified must not make prose look rarer than it is."""

    def test_an_unknown_case_does_not_dilute_the_prose_share(self) -> None:
        cases = necessity.classify_queries([*[_case("kbo_regulations", f"p{i}") for i in range(19)], _case(None, "u")])
        report = necessity.build_report(cases, dataset="mem")
        assert report.total == 20
        assert report.classified == 19
        assert report.prose_share == 1.0
        assert report.unknown_share == 0.05
        assert report.verdict is necessity.NecessityVerdict.KEEP_HYBRID

    def test_the_counts_always_partition_the_dataset(self) -> None:
        cases = necessity.classify_queries(
            [
                _case("kbo_regulations"),
                _case("player_basic"),
                _case("awards"),
                _case(None),
            ]
        )
        report = necessity.build_report(cases, dataset="mem")
        assert sum(report.counts.values()) == report.total == 4
        assert report.counts["unknown"] == 1

    def test_an_empty_report_does_not_claim_certainty(self) -> None:
        report = necessity.build_report((), dataset="mem")
        assert report.total == 0
        assert report.classified == 0
        assert report.prose_share == 0.0
        assert report.unknown_share == 1.0
        assert report.verdict is necessity.NecessityVerdict.UNDETERMINED

    def test_the_uncovered_tables_exclude_the_ones_already_covered(self) -> None:
        cases = necessity.classify_queries([_case("player_basic"), _case("awards"), _case("kbo_definitions")])
        report = necessity.build_report(cases, dataset="mem")
        assert report.uncovered_tables == ("awards",)

    def test_a_table_the_index_never_writes_is_not_called_uncovered(self) -> None:
        """``uncovered`` means "needs a branch", not "we have never heard of it".

        Subtracting the known sets would list a made-up table here, and the report
        would then recommend writing SQL for a table that does not exist.
        """
        report = necessity.build_report(necessity.classify_queries([_case("not_a_real_table")]), dataset="mem")
        assert report.counts["unknown"] == 1
        assert report.uncovered_tables == ()


class TestTheReportSerializes:
    """The JSON is the artifact that gets cited, so its shape is a contract."""

    def test_every_bucket_appears_even_at_zero(self) -> None:
        report = necessity.build_report(necessity.classify_queries([_case("kbo_regulations")]), dataset="mem")
        payload = report.to_dict()
        assert set(payload["counts"]) == {bucket.value for bucket in necessity.RetrievalNecessity}

    def test_the_thresholds_travel_with_the_numbers(self) -> None:
        payload = necessity.build_report((), dataset="mem").to_dict()
        assert payload["thresholds"] == {
            "sql_expansion_prose_share": necessity.SQL_EXPANSION_PROSE_SHARE,
            "keep_hybrid_prose_share": necessity.KEEP_HYBRID_PROSE_SHARE,
            "unknown_share_tolerance": necessity.UNKNOWN_SHARE_TOLERANCE,
        }

    def test_a_case_carries_its_verdict_and_its_table(self) -> None:
        report = necessity.build_report(necessity.classify_queries([_case("awards", "a1")]), dataset="mem")
        assert report.to_dict()["cases"] == [
            {"case_id": "a1", "query": "q", "source_table": "awards", "necessity": "relational_no_template"}
        ]

    def test_the_routing_summary_is_absent_unless_asked_for(self) -> None:
        assert "routing" not in necessity.build_report((), dataset="mem").to_dict()

    def test_an_undeclared_case_does_not_invent_a_table_name(self) -> None:
        """The breakdown names real tables only; unclassified is counted separately."""
        cases = necessity.classify_queries([_case("awards"), _case(None)])
        payload = necessity.build_report(cases, dataset="mem").to_dict()
        assert payload["by_source_table"] == {"awards": 1}
        assert payload["counts"]["unknown"] == 1


class TestRoutingSummary:
    """The router already bypasses retrieval; that share is worth keeping."""

    def test_routes_are_counted_and_the_retrieved_share_is_derived(self) -> None:
        queries = [
            GoldenQuery(query="q", relevant_chunk_ids=(), expected_route="STRUCTURED"),
            GoldenQuery(query="q", relevant_chunk_ids=(), expected_route="DOCUMENT"),
            GoldenQuery(query="q", relevant_chunk_ids=(), expected_route="DOCUMENT"),
            GoldenQuery(query="q", relevant_chunk_ids=(), expected_route="MIXED"),
        ]
        summary = necessity.summarize_routing(queries)
        assert summary is not None
        assert summary["total"] == 4
        assert summary["by_route"] == {"DOCUMENT": 2, "MIXED": 1, "STRUCTURED": 1}
        assert summary["retrieved_share"] == 0.75

    def test_a_dataset_without_routes_has_no_summary(self) -> None:
        assert necessity.summarize_routing([GoldenQuery(query="q", relevant_chunk_ids=())]) is None


class TestTheCommandLine:
    """Exit codes are the only thing a gate reads."""

    def test_the_summary_names_the_verdict(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        dataset = _write_dataset(
            tmp_path / "g.json",
            [
                {"id": "a", "query": "q", "filters": {"source_table": "player_basic"}},
                {"id": "b", "query": "q", "filters": {"source_table": "kbo_regulations"}},
            ],
        )
        assert necessity.main(["--dataset", str(dataset)]) == 0
        out = capsys.readouterr().out
        assert "sql_covered=1" in out
        assert "prose=1" in out
        assert "verdict=" in out

    def test_json_output_round_trips(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        dataset = _write_dataset(
            tmp_path / "g.json",
            [{"id": "a", "query": "q", "filters": {"source_table": "awards"}}],
        )
        assert necessity.main(["--dataset", str(dataset), "--json"]) == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["counts"]["relational_no_template"] == 1
        assert payload["dataset"] == str(dataset)

    def test_the_coverage_gate_fails_below_the_required_share(self, tmp_path: Path) -> None:
        dataset = _write_dataset(
            tmp_path / "g.json",
            [
                {"id": "a", "query": "q", "filters": {"source_table": "player_basic"}},
                {"id": "b", "query": "q", "filters": {"source_table": "kbo_regulations"}},
            ],
        )
        assert necessity.main(["--dataset", str(dataset), "--require-sql-covered", "0.4"]) == 0
        assert necessity.main(["--dataset", str(dataset), "--require-sql-covered", "0.6"]) == 1

    def test_the_coverage_gate_rejects_an_impossible_ratio(self, tmp_path: Path) -> None:
        dataset = _write_dataset(tmp_path / "g.json", [{"id": "a", "query": "q", "filters": {}}])
        assert necessity.main(["--dataset", str(dataset), "--require-sql-covered", "2"]) == 2

    def test_an_unclassified_case_fails_the_strict_gate(self, tmp_path: Path) -> None:
        dataset = _write_dataset(
            tmp_path / "g.json",
            [
                {"id": "a", "query": "q", "filters": {"source_table": "awards"}},
                {"id": "b", "query": "q"},
            ],
        )
        assert necessity.main(["--dataset", str(dataset)]) == 0
        assert necessity.main(["--dataset", str(dataset), "--fail-on-unknown"]) == 1

    def test_a_dataset_that_declares_no_tables_is_refused(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Pointing this at the routing set is a mistake worth naming."""
        dataset = _write_dataset(tmp_path / "routing.json", [{"id": "a", "query": "q", "expectedRoute": "DOCUMENT"}])
        assert necessity.main(["--dataset", str(dataset)]) == 2
        assert "routing dataset answers a different question" in capsys.readouterr().err

    def test_an_empty_dataset_is_refused(self, tmp_path: Path) -> None:
        dataset = _write_dataset(tmp_path / "empty.json", [])
        assert necessity.main(["--dataset", str(dataset)]) == 2

    def test_a_missing_dataset_is_refused(self, tmp_path: Path) -> None:
        assert necessity.main(["--dataset", str(tmp_path / "absent.json")]) == 2

    def test_the_routing_dataset_is_reported_alongside(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        dataset = _write_dataset(
            tmp_path / "g.json",
            [{"id": "a", "query": "q", "filters": {"source_table": "player_basic"}}],
        )
        routing = _write_dataset(
            tmp_path / "r.json",
            [
                {"id": "r1", "query": "q", "expectedRoute": "STRUCTURED"},
                {"id": "r2", "query": "q", "expectedRoute": "DOCUMENT"},
            ],
        )
        assert necessity.main(["--dataset", str(dataset), "--routing-dataset", str(routing), "--json"]) == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["routing"]["by_route"] == {"DOCUMENT": 1, "STRUCTURED": 1}
        assert payload["routing"]["retrieved_share"] == 0.5


class TestTheProductionDatasetHoldsUp:
    """The dataset is the evidence, so a stale table name in it is a defect."""

    def test_every_case_declares_the_table_it_came_from(self) -> None:
        report = _production_report()
        assert report.counts["unknown"] == 0, "a golden case stopped declaring filters.source_table"

    def test_every_declared_table_is_one_the_build_writes(self) -> None:
        declared = {case.source_table for case in _production_report().cases if case.source_table}
        unknown = declared - necessity.indexed_source_tables()
        assert not unknown, f"golden cases point at tables the build never writes: {sorted(unknown)}"

    def test_the_uncovered_tables_all_miss_the_retriever_not_the_index(self) -> None:
        report = _production_report()
        missing_from_index = set(report.uncovered_tables) - necessity.indexed_source_tables()
        assert not missing_from_index, f"uncovered tables that are not even indexed: {sorted(missing_from_index)}"

    @pytest.mark.skipif(not ROUTING_DATASET.is_file(), reason="routing golden set is not checked in")
    def test_the_routing_set_reports_the_share_it_actually_retrieves(self) -> None:
        summary = necessity.summarize_routing(load_golden_queries(ROUTING_DATASET))
        assert summary is not None
        assert summary["total"] > 0
        assert 0.0 <= float(summary["retrieved_share"]) <= 1.0


def _production_report() -> NecessityReport:
    """Build the report for the checked-in production golden set."""
    assert PRODUCTION_DATASET.is_file(), f"missing golden dataset at {PRODUCTION_DATASET}"
    return necessity.build_report(
        necessity.classify_queries(load_golden_queries(PRODUCTION_DATASET)),
        dataset=str(PRODUCTION_DATASET),
    )
