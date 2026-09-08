"""Unit and integration tests for RAG evaluation and query CLI commands."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from src.cli.rag.evaluate import build_arg_parser as build_eval_parser, main as eval_main
from src.cli.rag.query import build_arg_parser as build_query_parser, main as query_main

if TYPE_CHECKING:
    import pytest


def test_build_eval_parser() -> None:
    """Test argument parser for RAG evaluate CLI."""
    parser = build_eval_parser()
    args = parser.parse_args(["--offline", "--compare", "--top-k", "3", "--limit", "10", "--json"])
    assert args.offline is True
    assert args.compare is True
    assert args.top_k == 3
    assert args.limit == 10
    assert args.json is True


def test_build_query_parser() -> None:
    """Test argument parser for RAG query CLI."""
    parser = build_query_parser()
    args = parser.parse_args(["김도영", "--offline", "--retriever", "sparse", "--top-k", "3", "--json"])
    assert args.query == "김도영"
    assert args.offline is True
    assert args.retriever == "sparse"
    assert args.top_k == 3
    assert args.json is True


def test_evaluate_cli_offline_single_variant_json(capsys: pytest.CaptureFixture[str]) -> None:
    """Test single variant offline evaluation in JSON format."""
    exit_code = eval_main(["--offline", "--retriever", "hybrid", "-n", "3", "--json"])
    captured = capsys.readouterr()

    assert exit_code == 0
    json_str = captured.out[captured.out.find("{") :]
    data = json.loads(json_str)

    assert "metrics" in data
    assert "latency" in data
    assert data["total_evaluated"] == 3
    assert data["top_k"] == 5
    assert data["metrics"]["recall_at_k"] >= 0.0
    assert data["metrics"]["mrr"] >= 0.0


def test_evaluate_cli_offline_compare_text(capsys: pytest.CaptureFixture[str]) -> None:
    """Test 3-way offline comparison matrix in text format."""
    exit_code = eval_main(["--offline", "--compare", "-n", "3"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Retriever Comparison Matrix" in captured.out
    assert "Sparse (BM25)" in captured.out
    assert "Dense (Vector)" in captured.out
    assert "Hybrid (RRF)" in captured.out
    assert "Recall@5" in captured.out
    assert "SLA Status" in captured.out


def test_evaluate_cli_offline_compare_json(capsys: pytest.CaptureFixture[str]) -> None:
    """Test 3-way offline comparison in JSON format."""
    exit_code = eval_main(["--offline", "--compare", "-n", "3", "--json"])
    captured = capsys.readouterr()

    assert exit_code == 0
    json_str = captured.out[captured.out.find("{") :]
    data = json.loads(json_str)

    assert data["mode"] == "offline"
    assert "variants" in data
    assert "sparse" in data["variants"]
    assert "dense" in data["variants"]
    assert "hybrid" in data["variants"]


def test_evaluate_cli_offline_strict_sla_failure() -> None:
    """Test that impossible SLA requirement fails with exit code 1 when --strict is passed."""
    exit_code = eval_main(["--offline", "-n", "3", "--min-recall", "1.01", "--strict"])
    assert exit_code == 1


def test_query_cli_offline_text(capsys: pytest.CaptureFixture[str]) -> None:
    """Test interactive offline query in text output format."""
    exit_code = query_main(["ABS 스트라이크존", "--offline", "-k", "3"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "KBO RAG 검색" in captured.out
    assert "ABS" in captured.out


def test_query_cli_offline_json(capsys: pytest.CaptureFixture[str]) -> None:
    """Test interactive offline query in JSON format."""
    exit_code = query_main(["김도영", "--offline", "--retriever", "sparse", "-k", "2", "--json"])
    captured = capsys.readouterr()

    assert exit_code == 0
    json_str = captured.out[captured.out.find("{") :]
    data = json.loads(json_str)

    assert "candidates" in data
    assert len(data["candidates"]) > 0
    assert "query" in data


def test_query_cli_empty_query() -> None:
    """Test query CLI with empty input returns code 1."""
    exit_code = query_main([])
    assert exit_code == 1
