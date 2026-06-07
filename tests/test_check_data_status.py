import logging

import src.cli.check_data_status as module


class _FakeSession:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


def test_check_data_status_main_prints_summary_without_blank_logger_error(monkeypatch, caplog):
    monkeypatch.setattr(module, "SessionLocal", lambda: _FakeSession())
    monkeypatch.setattr(module, "check_schedules", lambda _session: {"total": 1, "warnings": []})
    monkeypatch.setattr(module, "check_players", lambda _session: {"total": 2})
    monkeypatch.setattr(module, "check_futures_data", lambda _session: {"batting": 3, "pitching": 4})
    monkeypatch.setattr(module, "check_game_data", lambda _session: {"batting": 5, "pitching": 6})
    monkeypatch.setattr(
        module,
        "check_pregame_pitcher_coverage",
        lambda _session, *, verbose=False: {
            "scheduled_total": 0,
            "both_ok": 0,
            "preview_rows": 0,
            "preview_missing_starters": 0,
            "sync_candidate_games": 0,
            "sync_complete_starters": 0,
            "oci_sync_ready": False,
            "coverage_pct": 0.0,
        },
    )

    caplog.set_level(logging.INFO, logger=module.logger.name)

    module.main([])

    assert "KBO Data Status Check" in caplog.text
    assert "Futures batting: 3" in caplog.text
    assert "Futures pitching: 4" in caplog.text
