from __future__ import annotations

import json
from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.game import Game, GamePitchingStat, GameSummary
from src.models.player import PlayerSeasonPitching
from src.services.context_aggregator import ContextAggregator
from src.utils.game_status import GAME_STATUS_COMPLETED

# ============================================================
# Fixtures
# ============================================================


def _build_session_factory():
    engine = create_engine("sqlite:///:memory:")
    for table in (
        Game.__table__,
        GamePitchingStat.__table__,
        GameSummary.__table__,
        PlayerSeasonPitching.__table__,
    ):
        table.create(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def _seed_full_pitching_game(session):
    """Seed a completed game with 4 pitching rows (2 starters + 2 bullpen) + season stats."""
    session.add(
        Game(
            game_id="20250401LGSS0",
            game_date=date(2025, 4, 1),
            away_team="LG",
            home_team="SS",
            away_score=4,
            home_score=2,
            game_status=GAME_STATUS_COMPLETED,
        )
    )
    session.add_all(
        [
            GamePitchingStat(
                game_id="20250401LGSS0",
                team_side="away",
                team_code="LG",
                player_id=1001,
                player_name="원정선발",
                is_starting=True,
                appearance_seq=1,
                innings_outs=18,
                pitches=92,
                earned_runs=1,
                strikeouts=6,
            ),
            GamePitchingStat(
                game_id="20250401LGSS0",
                team_side="away",
                team_code="LG",
                player_id=1002,
                player_name="원정불펜",
                is_starting=False,
                appearance_seq=2,
                innings_outs=9,
                pitches=31,
                earned_runs=1,
                strikeouts=2,
            ),
            GamePitchingStat(
                game_id="20250401LGSS0",
                team_side="home",
                team_code="SS",
                player_id=2001,
                player_name="홈선발",
                is_starting=True,
                appearance_seq=1,
                innings_outs=15,
                pitches=81,
                earned_runs=3,
                strikeouts=4,
            ),
            GamePitchingStat(
                game_id="20250401LGSS0",
                team_side="home",
                team_code="SS",
                player_id=2002,
                player_name="홈불펜",
                is_starting=False,
                appearance_seq=2,
                innings_outs=12,
                pitches=44,
                earned_runs=1,
                strikeouts=3,
            ),
        ]
    )
    session.add(
        PlayerSeasonPitching(
            player_id=1001,
            season=2025,
            league="REGULAR",
            level="KBO1",
            source="CRAWLER",
            team_code="LG",
            games=20,
            games_started=20,
            innings_pitched=120.0,
            era=3.15,
        )
    )
    session.add(
        PlayerSeasonPitching(
            player_id=1002,
            season=2025,
            league="REGULAR",
            level="KBO1",
            source="CRAWLER",
            team_code="LG",
            games=45,
            games_started=0,
            innings_pitched=42.0,
            era=2.95,
            holds=12,
        )
    )
    session.commit()


# ============================================================
# 1. Fixture creation validation
# ============================================================


class TestFixture:
    def test_seed_data_produces_valid_breakdown(self):
        SessionLocal = _build_session_factory()
        with SessionLocal() as session:
            _seed_full_pitching_game(session)
            payload = ContextAggregator(session).get_completed_game_pitching_breakdown("20250401LGSS0")

        assert payload["raw_counts"]["game_pitching_rows"] == 4
        assert payload["raw_counts"]["starter_rows"] == 2
        assert payload["raw_counts"]["bullpen_rows"] == 2
        assert payload["starters"]["away"]["player_name"] == "원정선발"
        assert payload["starters"]["home"]["player_name"] == "홈선발"
        assert payload["starters"]["away"]["season_stats_found"] is True
        assert payload["starters"]["home"]["season_stats_found"] is False
        assert payload["bullpen"]["away"]["totals"]["innings_outs"] == 9
        assert payload["bullpen"]["home"]["totals"]["pitches"] == 44


# ============================================================
# 2. API response shape validation
# ============================================================


class TestApiResponseShape:
    def test_diagnose_returns_expected_keys(self):
        SessionLocal = _build_session_factory()
        with SessionLocal() as session:
            _seed_full_pitching_game(session)
            session.add(
                GameSummary(
                    game_id="20250401LGSS0",
                    summary_type="리뷰_WPA",
                    detail_text=json.dumps(
                        {
                            "pitching_breakdown": {
                                "starters": {"away": {"player_name": "원정선발"}, "home": {"player_name": "홈선발"}},
                                "bullpen": {
                                    "away": {"pitchers": [{"player_name": "원정불펜"}]},
                                    "home": {"pitchers": [{"player_name": "홈불펜"}]},
                                },
                            }
                        },
                        ensure_ascii=False,
                    ),
                )
            )
            session.commit()
            result = ContextAggregator(session).diagnose_completed_game_coach_pitching("20250401LGSS0")

        assert set(result.keys()) == {"game_id", "drop_stage", "warnings", "raw_tables", "repository", "final_payload"}
        assert result["game_id"] == "20250401LGSS0"
        assert isinstance(result["raw_tables"], dict)
        assert isinstance(result["repository"], dict)
        assert isinstance(result["final_payload"], dict)

    def test_raw_tables_contains_expected_metrics(self):
        SessionLocal = _build_session_factory()
        with SessionLocal() as session:
            _seed_full_pitching_game(session)
            result = ContextAggregator(session).diagnose_completed_game_coach_pitching("20250401LGSS0")

        raw = result["raw_tables"]
        assert set(raw.keys()) == {
            "game_pitching_rows",
            "starter_rows",
            "bullpen_rows",
            "player_id_missing_rows",
            "season_pitching_matches",
        }
        assert raw["game_pitching_rows"] == 4
        assert raw["player_id_missing_rows"] == 0


# ============================================================
# 3. Drop stage coverage (rendering validation)
# ============================================================


class TestDropStages:
    def test_drop_stage_raw_missing(self):
        SessionLocal = _build_session_factory()
        with SessionLocal() as session:
            session.add(
                Game(
                    game_id="20250401HHLG0",
                    game_date=date(2025, 4, 1),
                    away_team="HH",
                    home_team="LG",
                    away_score=0,
                    home_score=0,
                    game_status=GAME_STATUS_COMPLETED,
                )
            )
            session.commit()
            result = ContextAggregator(session).diagnose_completed_game_coach_pitching("20250401HHLG0")

        assert result["drop_stage"] == "raw_game_pitching_stats_missing"

    def test_drop_stage_starter_missing(self):
        SessionLocal = _build_session_factory()
        with SessionLocal() as session:
            session.add(
                Game(
                    game_id="20250401HHLG0",
                    game_date=date(2025, 4, 1),
                    away_team="HH",
                    home_team="LG",
                    away_score=0,
                    home_score=0,
                    game_status=GAME_STATUS_COMPLETED,
                )
            )
            session.add(
                GamePitchingStat(
                    game_id="20250401HHLG0",
                    team_side="away",
                    team_code="HH",
                    player_id=3001,
                    player_name="불펜만",
                    is_starting=False,
                    appearance_seq=1,
                    innings_outs=9,
                )
            )
            session.commit()
            result = ContextAggregator(session).diagnose_completed_game_coach_pitching("20250401HHLG0")

        assert result["drop_stage"] == "raw_starter_flags_missing"

    def test_drop_stage_review_payload_missing(self):
        SessionLocal = _build_session_factory()
        with SessionLocal() as session:
            _seed_full_pitching_game(session)
            result = ContextAggregator(session).diagnose_completed_game_coach_pitching("20250401LGSS0")

        assert result["drop_stage"] == "final_review_payload_missing"

    def test_drop_stage_review_payload_missing_pitching(self):
        SessionLocal = _build_session_factory()
        with SessionLocal() as session:
            _seed_full_pitching_game(session)
            session.add(
                GameSummary(
                    game_id="20250401LGSS0",
                    summary_type="리뷰_WPA",
                    detail_text=json.dumps({"crucial_moments": []}, ensure_ascii=False),
                )
            )
            session.commit()
            result = ContextAggregator(session).diagnose_completed_game_coach_pitching("20250401LGSS0")

        assert result["drop_stage"] == "final_review_payload_missing_pitching"

    def test_drop_stage_ok(self):
        SessionLocal = _build_session_factory()
        with SessionLocal() as session:
            _seed_full_pitching_game(session)
            session.add(
                GameSummary(
                    game_id="20250401LGSS0",
                    summary_type="리뷰_WPA",
                    detail_text=json.dumps(
                        {
                            "pitching_breakdown": {
                                "starters": {"away": {"player_name": "원정선발"}, "home": {"player_name": "홈선발"}},
                                "bullpen": {
                                    "away": {"pitchers": [{"player_name": "원정불펜"}]},
                                    "home": {"pitchers": [{"player_name": "홈불펜"}]},
                                },
                            }
                        },
                        ensure_ascii=False,
                    ),
                )
            )
            session.commit()
            result = ContextAggregator(session).diagnose_completed_game_coach_pitching("20250401LGSS0")

        assert result["drop_stage"] == "ok"
        assert result["raw_tables"]["season_pitching_matches"] == 2


# ============================================================
# 4. Regression tests
# ============================================================


class TestRegression:
    def test_empty_game_id_returns_missing(self):
        SessionLocal = _build_session_factory()
        with SessionLocal() as session:
            result = ContextAggregator(session).diagnose_completed_game_coach_pitching("nonexistent")

        assert result["drop_stage"] == "raw_game_pitching_stats_missing"

    def test_duplicate_review_summaries_resolves(self):
        SessionLocal = _build_session_factory()
        with SessionLocal() as session:
            _seed_full_pitching_game(session)
            session.add_all(
                [
                    GameSummary(
                        game_id="20250401LGSS0",
                        summary_type="리뷰_WPA",
                        detail_text=json.dumps({"old": True}, ensure_ascii=False),
                    ),
                    GameSummary(
                        game_id="20250401LGSS0",
                        summary_type="리뷰_WPA",
                        detail_text=json.dumps(
                            {
                                "pitching_breakdown": {
                                    "starters": {
                                        "away": {"player_name": "원정선발"},
                                        "home": {"player_name": "홈선발"},
                                    },
                                    "bullpen": {
                                        "away": {"pitchers": [{"player_name": "원정불펜"}]},
                                        "home": {"pitchers": [{"player_name": "홈불펜"}]},
                                    },
                                }
                            },
                            ensure_ascii=False,
                        ),
                    ),
                ]
            )
            session.commit()
            result = ContextAggregator(session).diagnose_completed_game_coach_pitching("20250401LGSS0")

        assert result["drop_stage"] == "ok"

    def test_season_stats_mismatch_generates_warning(self):
        SessionLocal = _build_session_factory()
        with SessionLocal() as session:
            _seed_full_pitching_game(session)
            result = ContextAggregator(session).diagnose_completed_game_coach_pitching("20250401LGSS0")

        assert len(result["repository"]["unmatched_season_stats"]) == 2  # home players have no season stats
        assert any("season_pitching_join_incomplete" in w for w in result["warnings"])

    def test_diagnostic_without_season_stats(self):
        SessionLocal = _build_session_factory()
        with SessionLocal() as session:
            session.add(
                Game(
                    game_id="20250401HHLG0",
                    game_date=date(2025, 4, 1),
                    away_team="HH",
                    home_team="LG",
                    away_score=3,
                    home_score=1,
                    game_status=GAME_STATUS_COMPLETED,
                )
            )
            session.add_all(
                [
                    GamePitchingStat(
                        game_id="20250401HHLG0",
                        team_side="away",
                        team_code="HH",
                        player_id=4001,
                        player_name="선발",
                        is_starting=True,
                        appearance_seq=1,
                        innings_outs=21,
                        pitches=95,
                    ),
                    GamePitchingStat(
                        game_id="20250401HHLG0",
                        team_side="home",
                        team_code="LG",
                        player_id=4002,
                        player_name="선발홈",
                        is_starting=True,
                        appearance_seq=1,
                        innings_outs=18,
                        pitches=88,
                    ),
                ]
            )
            session.commit()
            result = ContextAggregator(session).diagnose_completed_game_coach_pitching("20250401HHLG0")

        assert result["drop_stage"] == "final_review_payload_missing"
        assert result["raw_tables"]["season_pitching_matches"] == 0


# ============================================================
# 5. CLI smoke tests
# ============================================================


class TestCli:
    def test_cli_diagnose_requires_date_or_game_id(self):
        """No args → SystemExit."""
        from src.cli.diagnose_coach_pitching import main

        with pytest.raises(SystemExit):
            main([])

    def test_cli_diagnose_with_seeded_data(self, capsys, monkeypatch):
        """Seed + --date → drop_stage in stdout."""
        from src.cli.diagnose_coach_pitching import main

        factory = _build_session_factory()
        monkeypatch.setattr("src.cli.diagnose_coach_pitching.SessionLocal", factory)

        with factory() as session:
            _seed_full_pitching_game(session)

        main(["--date", "20250401"])
        captured = capsys.readouterr()
        assert "final_review_payload_missing" in captured.out

    def test_cli_diagnose_with_json_output(self, capsys, monkeypatch):
        """--json flag → parseable JSON."""
        from src.cli.diagnose_coach_pitching import main

        factory = _build_session_factory()
        monkeypatch.setattr("src.cli.diagnose_coach_pitching.SessionLocal", factory)

        with factory() as session:
            _seed_full_pitching_game(session)

        main(["--date", "20250401", "--json"])
        captured = capsys.readouterr()
        rows = json.loads(captured.out)
        assert isinstance(rows, list)
        assert len(rows) == 1
        assert rows[0]["game_id"] == "20250401LGSS0"
        assert rows[0]["drop_stage"] == "final_review_payload_missing"
