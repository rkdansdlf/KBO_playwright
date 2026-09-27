"""Tests for the guarded `kbo crawl replay` command."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from src.cli.crawl_replay import main as replay_main
from src.cli.kbo import main as kbo_main
from src.services.crawl_run_replay import CrawlReplayResult


def _run(*, status: str = "partial", crawler: str = "awards") -> SimpleNamespace:
    return SimpleNamespace(status=status, crawler=crawler)


def _patch_original(monkeypatch, run: object, *, executors: dict | None = None) -> None:
    monkeypatch.setattr("src.cli.crawl_replay._load_original", lambda _run_id: run)
    monkeypatch.setattr(
        "src.cli.crawl_replay.build_default_executors",
        lambda: executors if executors is not None else {"awards": object()},
    )


def test_preview_validates_without_execution(monkeypatch, capsys) -> None:
    monkeypatch.delenv("KBO_ALLOW_CRAWL_REPLAY", raising=False)
    _patch_original(monkeypatch, _run(status="partial"))
    called: list[int] = []
    monkeypatch.setattr("src.cli.crawl_replay.replay_crawl_run", lambda *a, **k: called.append(1))
    assert replay_main(["--run-id", "run-a"]) == 0
    assert called == []
    assert "would replay" in capsys.readouterr().out


def test_preview_missing_run(monkeypatch, capsys) -> None:
    _patch_original(monkeypatch, None)
    assert replay_main(["--run-id", "missing"]) == 1
    assert "not found" in capsys.readouterr().err


def test_running_run_is_rejected(monkeypatch, capsys) -> None:
    _patch_original(monkeypatch, _run(status="running"))
    assert replay_main(["--run-id", "run-a"]) == 2
    assert "still running" in capsys.readouterr().err


def test_unsupported_crawler_is_rejected(monkeypatch, capsys) -> None:
    _patch_original(monkeypatch, _run(crawler="boxscore"), executors={"awards": object()})
    assert replay_main(["--run-id", "run-a"]) == 2
    assert "no replay executor" in capsys.readouterr().err


def test_apply_without_env_is_denied(monkeypatch, capsys) -> None:
    monkeypatch.delenv("KBO_ALLOW_CRAWL_REPLAY", raising=False)
    _patch_original(monkeypatch, _run(status="failed"))
    called: list[int] = []
    monkeypatch.setattr("src.cli.crawl_replay.replay_crawl_run", lambda *a, **k: called.append(1))
    assert replay_main(["--run-id", "run-a", "--apply"]) == 3
    assert called == []
    assert "KBO_ALLOW_CRAWL_REPLAY" in capsys.readouterr().err


def test_apply_success_json(monkeypatch, capsys) -> None:
    monkeypatch.setenv("KBO_ALLOW_CRAWL_REPLAY", "1")
    _patch_original(monkeypatch, _run(status="partial"))
    monkeypatch.setattr(
        "src.cli.crawl_replay.replay_crawl_run",
        lambda *_a, **_k: CrawlReplayResult(
            original_run_id="run-a",
            replay_run_id="run-x",
            status="success",
            success=True,
        ),
    )
    assert replay_main(["--run-id", "run-a", "--apply", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "action": "replay",
        "original_run_id": "run-a",
        "replay_run_id": "run-x",
        "applied": True,
        "status": "success",
        "success": True,
    }


def test_master_cli_routes_crawl_replay(monkeypatch, capsys) -> None:
    monkeypatch.delenv("KBO_ALLOW_CRAWL_REPLAY", raising=False)
    _patch_original(monkeypatch, _run(status="partial"))
    assert kbo_main(["crawl", "replay", "--run-id", "run-a"]) == 0
    assert "would replay" in capsys.readouterr().out
