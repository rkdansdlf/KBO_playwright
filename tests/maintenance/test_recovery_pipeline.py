"""Unit tests for the recovery pipeline orchestration."""

from __future__ import annotations

from scripts.maintenance import recovery_pipeline as rp


def _synthetic_report() -> dict:
    return {
        "defects": [
            {"year": 2024, "dimension": "missing_parent_games", "classification": "DEFECT"},
            {"year": 2024, "dimension": "coverage:game_lineups", "classification": "DEFECT"},
            {"year": 2023, "dimension": "coverage:game_batting_stats", "classification": "DEFECT"},
        ],
        "coverage_report": {
            "years": [
                {
                    "year": 2024,
                    "missing_game_ids": {
                        "game_lineups": ["20240405OBSK0", "20240505OBSK0"],
                        "game_batting_stats": ["20240405OBSK0"],
                        "game_pitching_stats": ["20240405OBSK0"],
                    },
                },
                {
                    "year": 2023,
                    "missing_game_ids": {
                        "game_lineups": [],
                        "game_batting_stats": ["20230705OBSK0"],
                        "game_pitching_stats": [],
                    },
                },
            ],
        },
    }


def test_defect_years_and_schedule_years() -> None:
    report = _synthetic_report()
    assert rp._defect_years(report) == {2023, 2024}
    assert rp._schedule_defect_years(report) == {2024}


def test_crawl_months_extracted_from_report() -> None:
    report = _synthetic_report()
    months = rp._crawl_months(report, {2023, 2024})
    assert months[2024] == {4, 5}
    assert months[2023] == {7}


def test_resolve_years_only_defect() -> None:
    class Args:
        start_year = 2009
        end_year = 2025
        only_defect_years = True

    years = rp._resolve_years(Args(), _synthetic_report())
    assert years == {2023, 2024}


def test_pipeline_dry_run_records_commands(monkeypatch) -> None:
    recorded: list[list[str]] = []

    def fake_run(cmd, *, dry_run, capture=True):
        recorded.append(list(cmd))
        return 0

    monkeypatch.setattr(rp, "_run_cli", fake_run)
    monkeypatch.setattr(rp, "_load_state", lambda: {"completed": []})
    monkeypatch.setattr(rp, "_save_state", lambda _state: None)
    monkeypatch.setattr(rp, "_load_audit_report", _synthetic_report)
    rc = rp.main(["--force", "--only-defect-years"])
    assert rc == 0
    joined = " ".join(" ".join(c) for c in recorded)
    assert "src.cli.crawl_schedule" in joined
    assert "src.cli.collect_games" in joined
    assert "src.cli.recalc_player_game_stats" in joined
    assert "backfill_player_ids" in joined
    assert "src.cli.recalc_player_stats" in joined
    assert "audit_pa_formula" in joined
    assert "audit_completeness_2009_2025" in joined


def test_pipeline_single_phase(monkeypatch) -> None:
    recorded: list[list[str]] = []

    def fake_run(cmd, *, dry_run, capture=True):
        recorded.append(list(cmd))
        return 0

    monkeypatch.setattr(rp, "_run_cli", fake_run)
    rc = rp.main(["--force", "--phase", "player_game"])
    assert rc == 0
    assert len(recorded) == (2025 - 2009 + 1)
    assert all("recalc_player_game_stats" in " ".join(c) for c in recorded)
