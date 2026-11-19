"""Measure how much of retrieval is standing in for a SQL lookup.

`evaluate_rag_retrieval` answers "is retrieval accurate". This asks a different
question: "would a SQL query have answered the same question anyway". A golden
case whose relevant chunk is one row of ``player_season_batting`` is a table
lookup that happened to arrive through vector search; a case whose relevant
chunk is a paragraph of the rulebook is the only kind of question a text index
is actually required for. Only the second kind is an argument for keeping one.

Nothing here is copied by hand. The prose tables come from the index build's own
``_MARKDOWN_SOURCE_TABLES``, the tables the structured retriever already answers
from come from scanning that module for the ``source_table=`` literals it puts
on each result, and the indexed set comes from the build's source map. A list
written out again in this file would be a fourth place to forget to update --
which is how every postseason game from 2018 to 2026 stayed out of the index for
four months while the audits reported green.

The dataset decides the buckets. A case that does not declare which table its
relevant chunk came from is counted as ``unknown`` and never folded into either
side, because "we did not ask" and "it is prose" are different claims.

Read-only: no database connection, no writes, no network.
"""

from __future__ import annotations

import argparse
import ast
import json
import sys
from collections import Counter
from dataclasses import dataclass
from enum import StrEnum
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING

from src.cli.rag.build_rag_index import _MARKDOWN_SOURCE_TABLES, _SOURCE_TABLE_BY_SOURCE
from src.services.retrieval_evaluation import GoldenQuery, load_golden_queries

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

#: Prose share at or below which retrieval is mostly a SQL detour.
SQL_EXPANSION_PROSE_SHARE = 0.15
#: Prose share at or above which a text index is genuinely doing the work.
KEEP_HYBRID_PROSE_SHARE = 0.50
#: Unclassified share above which no verdict is offered at all.
UNKNOWN_SHARE_TOLERANCE = 0.10

DEFAULT_DATASET = Path("Docs/references/rag_golden_queries.json")

_STRUCTURED_RETRIEVER_PATH = Path(__file__).resolve().parents[2] / "services" / "structured_retriever.py"


class RetrievalNecessity(StrEnum):
    """Say whether a retrieved chunk could have come from a SQL query."""

    #: The table already has a structured retriever branch, so retrieval is a detour.
    SQL_COVERED = "sql_covered"
    #: The chunk is a table row, but no structured retriever branch exists yet.
    RELATIONAL_NO_TEMPLATE = "relational_no_template"
    #: The chunk is written prose; similarity search is the only way to reach it.
    PROSE = "prose"
    #: The case did not say which table its relevant chunk came from.
    UNKNOWN = "unknown"


class NecessityVerdict(StrEnum):
    """Summarize what the measured prose share says about the text index."""

    #: Almost everything asked is a row lookup -- expand the SQL path.
    SQL_EXPANSION_WARRANTED = "sql_expansion_warranted"
    #: The share sits between the thresholds -- break it down per source table.
    DECOMPOSE_BY_SOURCE_TABLE = "decompose_by_source_table"
    #: Prose is the majority -- semantic retrieval is earning its cost.
    KEEP_HYBRID = "keep_hybrid"
    #: Too much of the dataset is unclassified for any of the above to hold.
    UNDETERMINED = "undetermined"


@dataclass(frozen=True)
class CaseVerdict:
    """Classify one golden case by the table its relevant chunk belongs to."""

    case_id: str
    query: str
    source_table: str | None
    necessity: RetrievalNecessity


@dataclass(frozen=True)
class NecessityReport:
    """Hold the counts, shares, and verdict for one necessity measurement."""

    dataset: str
    total: int
    classified: int
    counts: dict[str, int]
    prose_share: float
    sql_covered_share: float
    unknown_share: float
    verdict: NecessityVerdict
    sql_covered_tables: tuple[str, ...]
    uncovered_tables: tuple[str, ...]
    cases: tuple[CaseVerdict, ...]
    routing: dict[str, object] | None = None

    def to_dict(self) -> dict[str, object]:
        """Serialize the report to a JSON-compatible mapping."""
        payload: dict[str, object] = {
            "dataset": self.dataset,
            "total": self.total,
            "classified": self.classified,
            "counts": dict(self.counts),
            "prose_share": self.prose_share,
            "sql_covered_share": self.sql_covered_share,
            "unknown_share": self.unknown_share,
            "verdict": str(self.verdict),
            "sql_covered_tables": list(self.sql_covered_tables),
            "uncovered_tables": list(self.uncovered_tables),
            "thresholds": {
                "sql_expansion_prose_share": SQL_EXPANSION_PROSE_SHARE,
                "keep_hybrid_prose_share": KEEP_HYBRID_PROSE_SHARE,
                "unknown_share_tolerance": UNKNOWN_SHARE_TOLERANCE,
            },
            # Undeclared cases are counted under ``unknown`` above; giving them a
            # key here would invent a table name that does not exist.
            "by_source_table": dict(
                sorted(Counter(case.source_table for case in self.cases if case.source_table).items())
            ),
            "cases": [
                {
                    "case_id": case.case_id,
                    "query": case.query,
                    "source_table": case.source_table,
                    "necessity": str(case.necessity),
                }
                for case in self.cases
            ],
        }
        if self.routing is not None:
            payload["routing"] = dict(self.routing)
        return payload


def _require_tables(tables: frozenset[str], origin: str) -> frozenset[str]:
    """Refuse an empty derivation, which would quietly reclassify every case."""
    if not tables:
        message = (
            f"retrieval necessity: {origin} yielded no source tables; refusing to classify cases against an empty set"
        )
        raise RuntimeError(message)
    return tables


@lru_cache(maxsize=1)
def prose_source_tables() -> frozenset[str]:
    """Return the source tables whose chunks are written prose, per the index build."""
    return _require_tables(frozenset(_MARKDOWN_SOURCE_TABLES), "_MARKDOWN_SOURCE_TABLES")


@lru_cache(maxsize=1)
def indexed_source_tables() -> frozenset[str]:
    """Return every source table the index build can write a chunk for."""
    return _require_tables(frozenset(_SOURCE_TABLE_BY_SOURCE.values()), "_SOURCE_TABLE_BY_SOURCE")


@lru_cache(maxsize=1)
def structured_source_tables() -> frozenset[str]:
    """Return the tables the structured retriever answers from, read from its source.

    Each result the retriever builds names its table, so scanning for those
    literals asks the retriever itself. A new branch is picked up without editing
    this module, and a branch that disappears stops being claimed as covered.
    """
    module = ast.parse(_STRUCTURED_RETRIEVER_PATH.read_text(encoding="utf-8"))
    tables = set()
    for node in ast.walk(module):
        if not isinstance(node, ast.Call):
            continue
        for keyword in node.keywords:
            value = keyword.value
            if keyword.arg == "source_table" and isinstance(value, ast.Constant) and isinstance(value.value, str):
                tables.add(value.value)
    return _require_tables(frozenset(tables), f"{_STRUCTURED_RETRIEVER_PATH.name} source_table literals")


def classify_source_table(source_table: str | None) -> RetrievalNecessity:
    """Decide which bucket one source table falls in.

    Prose is tested before structured coverage so a table that is both -- a
    markdown table parsed into rows, say -- can never be counted as a SQL lookup.
    """
    if source_table is None or not source_table.strip():
        return RetrievalNecessity.UNKNOWN
    normalized = source_table.strip()
    if normalized in prose_source_tables():
        return RetrievalNecessity.PROSE
    if normalized in structured_source_tables():
        return RetrievalNecessity.SQL_COVERED
    if normalized in indexed_source_tables():
        return RetrievalNecessity.RELATIONAL_NO_TEMPLATE
    return RetrievalNecessity.UNKNOWN


def classify_queries(queries: Sequence[GoldenQuery]) -> tuple[CaseVerdict, ...]:
    """Classify every golden case by the table it says its answer came from."""
    verdicts = []
    for index, query in enumerate(queries):
        filters: Mapping[str, object] = query.filters or {}
        declared = filters.get("source_table")
        source_table = str(declared).strip() if declared is not None else None
        verdicts.append(
            CaseVerdict(
                case_id=query.case_id or f"case_{index:03d}",
                query=query.query,
                source_table=source_table or None,
                necessity=classify_source_table(source_table),
            )
        )
    return tuple(verdicts)


def decide_verdict(*, total: int, classified: int, prose: int, unknown: int) -> NecessityVerdict:
    """Turn the measured shares into the pre-agreed decision bands."""
    if classified == 0 or total == 0:
        return NecessityVerdict.UNDETERMINED
    if unknown / total > UNKNOWN_SHARE_TOLERANCE:
        return NecessityVerdict.UNDETERMINED
    prose_share = prose / classified
    if prose_share <= SQL_EXPANSION_PROSE_SHARE:
        return NecessityVerdict.SQL_EXPANSION_WARRANTED
    if prose_share >= KEEP_HYBRID_PROSE_SHARE:
        return NecessityVerdict.KEEP_HYBRID
    return NecessityVerdict.DECOMPOSE_BY_SOURCE_TABLE


def summarize_routing(queries: Sequence[GoldenQuery]) -> dict[str, object] | None:
    """Count the routes the routing dataset expects, or ``None`` when it declares none."""
    routes = [query.expected_route for query in queries if query.expected_route]
    if not routes:
        return None
    counts = Counter(routes)
    total = len(routes)
    return {
        "total": total,
        "by_route": dict(sorted(counts.items())),
        "retrieved_share": round((counts.get("DOCUMENT", 0) + counts.get("MIXED", 0)) / total, 4),
    }


def build_report(
    cases: Sequence[CaseVerdict],
    *,
    dataset: str,
    routing: dict[str, object] | None = None,
) -> NecessityReport:
    """Aggregate per-case verdicts into the shares and the decision band."""
    counts = Counter(str(case.necessity) for case in cases)
    total = len(cases)
    unknown = counts.get(str(RetrievalNecessity.UNKNOWN), 0)
    classified = total - unknown
    prose = counts.get(str(RetrievalNecessity.PROSE), 0)
    sql_covered = counts.get(str(RetrievalNecessity.SQL_COVERED), 0)
    # A table needs a branch only if a case landed in ``relational_no_template``.
    # Subtracting the two known sets instead would also list tables that are not
    # in the index at all, i.e. would recommend SQL for a name that does not exist.
    uncovered = {
        case.source_table
        for case in cases
        if case.necessity is RetrievalNecessity.RELATIONAL_NO_TEMPLATE and case.source_table
    }
    return NecessityReport(
        dataset=dataset,
        total=total,
        classified=classified,
        counts={bucket.value: counts.get(bucket.value, 0) for bucket in RetrievalNecessity},
        prose_share=round(prose / classified, 4) if classified else 0.0,
        sql_covered_share=round(sql_covered / classified, 4) if classified else 0.0,
        unknown_share=round(unknown / total, 4) if total else 1.0,
        verdict=decide_verdict(total=total, classified=classified, prose=prose, unknown=unknown),
        sql_covered_tables=tuple(sorted(structured_source_tables())),
        uncovered_tables=tuple(sorted(uncovered)),
        cases=tuple(cases),
        routing=routing,
    )


def _render_summary(report: NecessityReport) -> str:
    """Render the measurement as one screen of aligned key/value lines."""
    counts = report.counts
    lines = (
        (
            f"dataset={report.dataset} queries={report.total} classified={report.classified} "
            f"unknown={counts['unknown']} ({report.unknown_share:.1%})"
        ),
        (
            f"sql_covered={counts['sql_covered']} ({report.sql_covered_share:.1%}) "
            f"relational_no_template={counts['relational_no_template']} "
            f"prose={counts['prose']} ({report.prose_share:.1%})"
        ),
        (
            f"verdict={report.verdict} prose_share={report.prose_share:.4f} "
            f"(sql_expansion<={SQL_EXPANSION_PROSE_SHARE:.2f} keep_hybrid>={KEEP_HYBRID_PROSE_SHARE:.2f})"
        ),
        "sql_covered_tables=" + (",".join(report.sql_covered_tables) or "-"),
        "uncovered_tables=" + (",".join(report.uncovered_tables) or "-"),
    )
    return "\n".join(lines)


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    """Parse the read-only measurement options."""
    parser = argparse.ArgumentParser(description="Measure how much retrieval is standing in for a SQL lookup")
    parser.add_argument("--dataset", default=str(DEFAULT_DATASET), help="Golden query JSON array to classify")
    parser.add_argument(
        "--routing-dataset",
        default=None,
        help="Optional routing golden set whose expectedRoute distribution is also reported",
    )
    parser.add_argument("--json", action="store_true", dest="as_json", help="Render JSON output")
    parser.add_argument(
        "--require-sql-covered",
        type=float,
        default=None,
        metavar="RATIO",
        help="Exit 1 when the share of already SQL-servable cases falls below RATIO (0..1)",
    )
    parser.add_argument(
        "--fail-on-unknown",
        action="store_true",
        help="Exit 1 when any case omits the source_table that decides its bucket",
    )
    return parser.parse_args(argv)


class NecessityContractError(RuntimeError):
    """Report a dataset contract the command must not guess past."""


def _load_report(args: argparse.Namespace) -> NecessityReport:
    """Load both datasets and build the report, or explain why they cannot be used."""
    if args.require_sql_covered is not None and not 0.0 <= args.require_sql_covered <= 1.0:
        message = "--require-sql-covered must be between 0 and 1"
        raise NecessityContractError(message)
    queries = load_golden_queries(args.dataset)
    if not queries:
        message = "golden dataset is empty"
        raise NecessityContractError(message)
    routing = summarize_routing(load_golden_queries(args.routing_dataset)) if args.routing_dataset else None
    report = build_report(classify_queries(queries), dataset=args.dataset, routing=routing)
    if report.counts[str(RetrievalNecessity.UNKNOWN)] == report.total:
        message = (
            "no case declares filters.source_table; "
            "the routing dataset answers a different question (use --routing-dataset)"
        )
        raise NecessityContractError(message)
    return report


def _gate_failure(report: NecessityReport, args: argparse.Namespace) -> str | None:
    """Return the reason a requested gate failed, or ``None`` when every gate passed."""
    if args.require_sql_covered is not None and report.sql_covered_share < args.require_sql_covered:
        return f"sql_covered_share {report.sql_covered_share:.4f} is below the required {args.require_sql_covered:.4f}"
    unknown = report.counts[str(RetrievalNecessity.UNKNOWN)]
    if args.fail_on_unknown and unknown:
        return f"{unknown} case(s) omit filters.source_table"
    return None


def main(argv: Sequence[str] | None = None) -> int:
    """Measure retrieval necessity against a golden dataset without touching any store."""
    args = _parse_args(argv)
    try:
        report = _load_report(args)
    except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
        sys.stderr.write(f"necessity_error: {exc}\n")
        return 2

    if args.as_json:
        sys.stdout.write(json.dumps(report.to_dict(), ensure_ascii=False) + "\n")
    else:
        sys.stdout.write(_render_summary(report) + "\n")

    failure = _gate_failure(report, args)
    if failure is not None:
        sys.stderr.write(f"necessity_error: {failure}\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
