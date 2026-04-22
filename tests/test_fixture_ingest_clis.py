from __future__ import annotations

import argparse
from types import SimpleNamespace

from src.cli import ingest_mock_game_html, ingest_schedule_html, run_pipeline_demo


def test_ingest_schedule_html_uses_schedule_save_service(monkeypatch, tmp_path, capsys):
    html_file = tmp_path / "schedule.html"
    html_file.write_text("<html></html>", encoding="utf-8")
    parsed_games = [{"game_id": "20250322HTLG0", "game_date": "20250322"}]
    captured: dict[str, object] = {}

    def _fake_parse_schedule_html(html, *, default_year, season_type):
        captured["html"] = html
        captured["default_year"] = default_year
        captured["season_type"] = season_type
        return parsed_games

    def _fake_save_schedule_games(games):
        captured["games"] = list(games)
        return SimpleNamespace(saved=1, failed=0)

    monkeypatch.setattr(ingest_schedule_html, "parse_schedule_html", _fake_parse_schedule_html)
    monkeypatch.setattr(ingest_schedule_html, "save_schedule_games", _fake_save_schedule_games)

    ingest_schedule_html.ingest_schedule_html(
        argparse.Namespace(fixtures_dir=str(tmp_path), default_year=2025, season_type="regular")
    )

    out = capsys.readouterr().out
    assert "Ingested 1 games" in out
    assert captured["default_year"] == 2025
    assert captured["season_type"] == "regular"
    assert captured["games"] == parsed_games


def test_ingest_mock_game_html_uses_detail_save_function(monkeypatch, tmp_path, capsys):
    html_file = tmp_path / "20251001NCLG0.html"
    html_file.write_text("<html></html>", encoding="utf-8")
    payload = {"game_id": "20251001NCLG0"}
    captured: dict[str, object] = {}

    def _fake_parse_game_detail_html(html, game_id, game_date):
        captured["html"] = html
        captured["game_id"] = game_id
        captured["game_date"] = game_date
        return payload

    def _fake_save_game_detail(game_data):
        captured["game_data"] = game_data
        return True

    monkeypatch.setattr(ingest_mock_game_html, "parse_game_detail_html", _fake_parse_game_detail_html)
    monkeypatch.setattr(ingest_mock_game_html, "save_game_detail", _fake_save_game_detail)

    ingest_mock_game_html.ingest_mock_html(argparse.Namespace(fixtures_dir=str(tmp_path), limit=None))

    out = capsys.readouterr().out
    assert "Ingested mock game 20251001NCLG0" in out
    assert captured["game_id"] == "20251001NCLG0"
    assert captured["game_date"] == "20251001"
    assert captured["game_data"] == payload


def test_run_pipeline_demo_fixture_ingest_uses_shared_save_paths(monkeypatch, tmp_path):
    schedule_dir = tmp_path / "schedules"
    detail_dir = tmp_path / "details"
    schedule_dir.mkdir()
    detail_dir.mkdir()
    (schedule_dir / "schedule.html").write_text("<html></html>", encoding="utf-8")
    (detail_dir / "20251001NCLG0.html").write_text("<html></html>", encoding="utf-8")
    parsed_games = [{"game_id": "20250322HTLG0", "game_date": "20250322"}]
    detail_payload = {"game_id": "20251001NCLG0"}
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        run_pipeline_demo,
        "parse_schedule_html",
        lambda html, *, default_year, season_type: parsed_games,
    )

    def _fake_save_schedule_games(games):
        captured["schedule_games"] = list(games)
        return SimpleNamespace(saved=len(captured["schedule_games"]), failed=0)

    monkeypatch.setattr(run_pipeline_demo, "save_schedule_games", _fake_save_schedule_games)
    monkeypatch.setattr(
        run_pipeline_demo,
        "parse_game_detail_html",
        lambda html, game_id, game_date: detail_payload,
    )
    monkeypatch.setattr(run_pipeline_demo, "save_game_detail", lambda payload: captured.setdefault("detail", payload) is payload)

    schedule_count = run_pipeline_demo.ingest_schedule_fixtures(schedule_dir, "regular", 2025)
    detail_count = run_pipeline_demo.ingest_game_fixtures(detail_dir)

    assert schedule_count == 1
    assert detail_count == 1
    assert captured["schedule_games"] == parsed_games
    assert captured["detail"] == detail_payload
