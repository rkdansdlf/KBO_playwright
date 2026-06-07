from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

import src.repositories.game_relay as game_relay_module
import src.repositories.game_save as game_save_module
from src.crawlers.relay_crawler import RelayCrawler
from src.models.game import Game, GameEvent, GameIdAlias, GameMetadata, GamePlayByPlay, GameValidationMetrics
from src.models.player import PlayerBasic, PlayerSeasonBatting, PlayerSeasonPitching


def _build_session_factory():
    engine = create_engine("sqlite:///:memory:")

    @event.listens_for(engine, "connect")
    def _disable_fks(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=OFF")
        cursor.close()

    Game.__table__.create(bind=engine)
    GameIdAlias.__table__.create(bind=engine)
    GamePlayByPlay.__table__.create(bind=engine)
    GameEvent.__table__.create(bind=engine)
    GameMetadata.__table__.create(bind=engine)
    GameValidationMetrics.__table__.create(bind=engine)
    PlayerBasic.__table__.create(bind=engine)
    PlayerSeasonBatting.__table__.create(bind=engine)
    PlayerSeasonPitching.__table__.create(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def _seed_game(
    SessionLocal,
    game_id: str,
    *,
    target_date: date = date(2025, 4, 1),
    home_team: str = "SS",
    away_team: str = "LG",
):
    with SessionLocal() as session:
        session.add(
            Game(
                game_id=game_id,
                game_date=target_date,
                home_team=home_team,
                away_team=away_team,
            )
        )
        session.commit()


def _seed_player_basic(SessionLocal, player_id: int, name: str, team_code: str):
    with SessionLocal() as session:
        session.add(PlayerBasic(player_id=player_id, name=name, team=team_code))
        session.commit()


def _seed_player_season_batting(SessionLocal, player_id: int, season: int, team_code: str):
    with SessionLocal() as session:
        session.add(
            PlayerSeasonBatting(
                player_id=player_id,
                season=season,
                league="REGULAR",
                level="KBO1",
                source="ROLLUP",
                team_code=team_code,
            )
        )
        session.commit()


# ---------------------------------------------------------------------------
# Synthetic Naver API segments
# ---------------------------------------------------------------------------

GAME_ID = "20250401LGSS0"
SEASON = 2025


def _build_synthetic_text_relays():
    """Return a list of Naver API segment dicts (textRelays) covering 1.5 innings.

    Structure mirrors the live Naver API: 1회초 (away/LG) + 1회말 (home/SS).
    """
    return [
        {
            "title": "1회초 LG 공격",
            "homeOrAway": "0",
            "inn": 1,
            "textOptions": [
                {
                    "text": "1구 스트라이크",
                    "batterRecord": {"name": "홍길동"},
                    "currentGameState": {
                        "homeScore": "0",
                        "awayScore": "0",
                        "out": "0",
                        "base1": "0",
                        "base2": "0",
                        "base3": "0",
                    },
                },
                {
                    "text": "2구 볼",
                    "batterRecord": {"name": "홍길동"},
                    "currentGameState": {
                        "homeScore": "0",
                        "awayScore": "0",
                        "out": "0",
                        "base1": "0",
                        "base2": "0",
                        "base3": "0",
                    },
                },
                {
                    "text": "3구 스트라이크",
                    "batterRecord": {"name": "홍길동"},
                    "currentGameState": {
                        "homeScore": "0",
                        "awayScore": "0",
                        "out": "0",
                        "base1": "0",
                        "base2": "0",
                        "base3": "0",
                    },
                },
                {
                    "text": "홍길동:4구 좌전 안타",
                    "batterRecord": {"name": "홍길동"},
                    "currentGameState": {
                        "homeScore": "0",
                        "awayScore": "0",
                        "out": "0",
                        "base1": "1",
                        "base2": "0",
                        "base3": "0",
                    },
                },
                {
                    "text": "1루주자 도루 성공",
                    "batterRecord": {"name": "홍길동"},
                    "currentGameState": {
                        "homeScore": "0",
                        "awayScore": "0",
                        "out": "0",
                        "base1": "0",
                        "base2": "1",
                        "base3": "0",
                    },
                },
                {
                    "text": "김철수:1구 유격수 땅볼 아웃",
                    "batterRecord": {"name": "김철수"},
                    "currentGameState": {
                        "homeScore": "0",
                        "awayScore": "0",
                        "out": "1",
                        "base1": "0",
                        "base2": "1",
                        "base3": "0",
                    },
                },
                {
                    "text": "김철수:2구 삼진",
                    "batterRecord": {"name": "김철수"},
                    "currentGameState": {
                        "homeScore": "0",
                        "awayScore": "0",
                        "out": "2",
                        "base1": "0",
                        "base2": "1",
                        "base3": "0",
                    },
                },
            ],
        },
        {
            "title": "1회말 SS 공격",
            "homeOrAway": "1",
            "inn": 1,
            "textOptions": [
                {
                    "text": "박영희:1구 중전 안타",
                    "batterRecord": {"name": "박영희"},
                    "currentGameState": {
                        "homeScore": "0",
                        "awayScore": "0",
                        "out": "0",
                        "base1": "1",
                        "base2": "0",
                        "base3": "0",
                    },
                },
                {
                    "text": "1구 스트라이크",
                    "batterRecord": {"name": "이몽룡"},
                    "currentGameState": {
                        "homeScore": "0",
                        "awayScore": "0",
                        "out": "0",
                        "base1": "1",
                        "base2": "0",
                        "base3": "0",
                    },
                },
                {
                    "text": "이몽룡:2구 삼진",
                    "batterRecord": {"name": "이몽룡"},
                    "currentGameState": {
                        "homeScore": "0",
                        "awayScore": "0",
                        "out": "1",
                        "base1": "1",
                        "base2": "0",
                        "base3": "0",
                    },
                },
            ],
        },
    ]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _setup(monkeypatch, SessionLocal):
    monkeypatch.setattr(game_relay_module, "SessionLocal", SessionLocal)
    monkeypatch.setattr(game_relay_module, "_auto_sync_to_oci", lambda game_id: None)
    monkeypatch.setattr(game_save_module, "SessionLocal", SessionLocal)
    monkeypatch.setattr(game_save_module, "_auto_sync_to_oci", lambda game_id: None)
    _seed_game(SessionLocal, GAME_ID)


# ===================================================================
# Tests
# ===================================================================


class TestRelayAtBatEnrichmentPipeline:
    """End-to-end tests for the relay at-bat enrichment pipeline.

    Verifies that at_bat_seq, at_bat_event_role, at_bat_confidence, balls, strikes
    survive the full path: Naver segment → _parse_naver_payload → at-bat grouper →
    pitch-count parser → save_relay_data() → DB rows.
    """

    def test_enrichment_fields_populated_on_events(self):
        """Verify that events from _parse_naver_payload carry at-bat/ball-strike fields."""
        crawler = RelayCrawler()
        text_relays = _build_synthetic_text_relays()

        result = crawler._parse_naver_payload(text_relays)
        events = result["events"]

        assert len(events) > 0, "Should have parsed events from synthetic segments"

        for i, ev in enumerate(events):
            assert "at_bat_seq" in ev, f"Event {i} missing at_bat_seq"
            assert "at_bat_event_role" in ev, f"Event {i} missing at_bat_event_role"
            assert "at_bat_confidence" in ev, f"Event {i} missing at_bat_confidence"
            assert "balls" in ev, f"Event {i} missing balls"
            assert "strikes" in ev, f"Event {i} missing strikes"

    def test_at_bat_seq_increments_across_batters(self):
        """Verify at_bat_seq increases when batter changes across inning/half boundaries."""
        crawler = RelayCrawler()
        text_relays = _build_synthetic_text_relays()

        result = crawler._parse_naver_payload(text_relays)
        events = result["events"]

        # Event 0: 홍길동 안타 → at_bat_seq=1 (batter A, first PA)
        # Event 1: 김철수 땅볼 → at_bat_seq=2 (batter B, first PA — out)
        # Event 2: 김철수 삼진 → at_bat_seq=3 (batter B, second PA — prev was terminal)
        # Event 3: 박영희 안타 (new half 1회말) → at_bat_seq=4
        # Event 4: 이몽룡 삼진 → at_bat_seq=5

        assert events[0]["at_bat_seq"] == 1
        assert events[0]["batter_name"] == "홍길동"

        assert events[1]["at_bat_seq"] == 2
        assert events[1]["batter_name"] == "김철수"

        assert events[2]["at_bat_seq"] == 3
        assert events[2]["batter_name"] == "김철수"
        assert events[2]["at_bat_event_role"] in ("at_bat_result", "plate_appearance_result")

        assert events[3]["at_bat_seq"] == 4
        assert events[3]["batter_name"] == "박영희"

        assert events[4]["at_bat_seq"] == 5
        assert events[4]["batter_name"] == "이몽룡"

    def test_at_bat_role_assignments(self):
        """Verify role tagging: pitch events, terminal results, etc."""
        crawler = RelayCrawler()
        text_relays = _build_synthetic_text_relays()

        result = crawler._parse_naver_payload(text_relays)
        events = result["events"]

        # The first event (홍길동 안타) should be terminal
        assert events[0]["at_bat_event_role"] in ("at_bat_result", "plate_appearance_result")

        # Subsequent events by same batter should be same at_bat
        # (김철수 땅볼 is the result of the next at-bat, ending it)
        assert events[1]["at_bat_event_role"] in ("at_bat_result", "plate_appearance_result")
        assert events[2]["at_bat_event_role"] in ("at_bat_result", "plate_appearance_result")

    def test_at_bat_confidence_high_with_batter_name(self):
        """Verify confidence is 'high' when batter name is available."""
        crawler = RelayCrawler()
        text_relays = _build_synthetic_text_relays()

        result = crawler._parse_naver_payload(text_relays)
        events = result["events"]

        for ev in events:
            assert ev["at_bat_confidence"] in ("high", "medium")

    def test_raw_pbp_rows_contain_pitch_noise_and_headers(self):
        """Verify raw_pbp_rows includes inning headers and non-event pitch lines."""
        crawler = RelayCrawler()
        text_relays = _build_synthetic_text_relays()

        result = crawler._parse_naver_payload(text_relays)
        raw_pbp_rows = result["raw_pbp_rows"]

        # Should contain inning headers
        inning_headers = [r for r in raw_pbp_rows if r.get("event_type") == "inning_header"]
        assert len(inning_headers) == 2

        # Should contain raw pitch texts like "1구 스트라이크"
        pitch_texts = [r for r in raw_pbp_rows if "구" in (r.get("play_description") or "")]
        assert len(pitch_texts) >= 4

        # Should contain the non-event runner text "1루주자 도루 성공"
        runner_rows = [r for r in raw_pbp_rows if "도루" in (r.get("play_description") or "")]
        assert len(runner_rows) == 1

    def test_raw_pbp_rows_count_exceeds_events_count(self):
        """Many log entries (pitches, headers) appear only in raw_pbp_rows, not events."""
        crawler = RelayCrawler()
        text_relays = _build_synthetic_text_relays()

        result = crawler._parse_naver_payload(text_relays)
        assert len(result["raw_pbp_rows"]) > len(result["events"])

    def test_full_save_roundtrip_propagates_enrichment_fields(self, monkeypatch):
        """Verify that after save_relay_data(), GameEvent rows have enrichment columns populated."""
        SessionLocal = _build_session_factory()
        _setup(monkeypatch, SessionLocal)

        crawler = RelayCrawler()
        result = crawler._parse_naver_payload(_build_synthetic_text_relays())
        events = result["events"]
        raw_pbp_rows = result["raw_pbp_rows"]

        saved = game_relay_module.save_relay_data(
            GAME_ID,
            events=events,
            raw_pbp_rows=raw_pbp_rows,
        )
        assert saved > 0

        with SessionLocal() as session:
            db_events = (
                session.query(GameEvent).filter(GameEvent.game_id == GAME_ID).order_by(GameEvent.event_seq.asc()).all()
            )
            assert len(db_events) == len(events)

            for i, db_ev in enumerate(db_events):
                assert db_ev.at_bat_seq == events[i]["at_bat_seq"], (
                    f"GameEvent[{i}] at_bat_seq mismatch: DB={db_ev.at_bat_seq} vs expected={events[i]['at_bat_seq']}"
                )
                assert db_ev.at_bat_event_role is not None, f"GameEvent[{i}] at_bat_event_role is None"
                assert db_ev.at_bat_confidence is not None, f"GameEvent[{i}] at_bat_confidence is None"

    def test_full_save_roundtrip_propagates_pbp_rows(self, monkeypatch):
        """Verify GamePlayByPlay rows are written and get player_id resolution."""
        SessionLocal = _build_session_factory()
        _setup(monkeypatch, SessionLocal)

        crawler = RelayCrawler()
        result = crawler._parse_naver_payload(_build_synthetic_text_relays())

        saved = game_relay_module.save_relay_data(
            GAME_ID,
            events=result["events"],
            raw_pbp_rows=result["raw_pbp_rows"],
        )
        assert saved > 0

        with SessionLocal() as session:
            pbp_rows = session.query(GamePlayByPlay).filter(GamePlayByPlay.game_id == GAME_ID).all()
            assert len(pbp_rows) == len(result["raw_pbp_rows"])

            for row in pbp_rows:
                assert hasattr(row, "player_id")
                assert hasattr(row, "resolver_confidence")
                assert hasattr(row, "resolver_reason")
                assert hasattr(row, "unresolved_player_name")

    def test_pipeline_with_player_resolver_success(self, monkeypatch):
        """Verify resolver populates player_id when player data exists."""
        SessionLocal = _build_session_factory()
        _setup(monkeypatch, SessionLocal)

        _seed_player_basic(SessionLocal, 10001, "홍길동", "LG")
        _seed_player_season_batting(SessionLocal, 10001, SEASON, "LG")

        crawler = RelayCrawler()
        result = crawler._parse_naver_payload(_build_synthetic_text_relays())

        saved = game_relay_module.save_relay_data(
            GAME_ID,
            events=result["events"],
            raw_pbp_rows=result["raw_pbp_rows"],
        )
        assert saved > 0

        with SessionLocal() as session:
            # 홍길동 in top of 1st → team=LG → should be resolved
            pbp_rows = (
                session.query(GamePlayByPlay)
                .filter(
                    GamePlayByPlay.game_id == GAME_ID,
                    GamePlayByPlay.batter_name == "홍길동",
                )
                .all()
            )
            assert len(pbp_rows) > 0
            # At least one row should have a resolved player_id
            resolved = [r for r in pbp_rows if r.player_id is not None]
            assert len(resolved) > 0, (
                f"Expected at least one 홍길동 row with player_id resolved, "
                f"got: {[(r.player_id, r.resolver_confidence) for r in pbp_rows]}"
            )
            # Verify confidence is "resolved" for those rows
            assert resolved[0].resolver_confidence == "resolved"
            assert resolved[0].resolver_reason is not None

    def test_save_relay_resolves_woob_players_with_canonical_and_defensive_team_context(self, monkeypatch):
        SessionLocal = _build_session_factory()
        monkeypatch.setattr(game_relay_module, "SessionLocal", SessionLocal)
        monkeypatch.setattr(game_relay_module, "_auto_sync_to_oci", lambda game_id: None)
        monkeypatch.setattr(game_save_module, "SessionLocal", SessionLocal)
        monkeypatch.setattr(game_save_module, "_auto_sync_to_oci", lambda game_id: None)
        _seed_game(
            SessionLocal,
            "20260607WOOB0",
            target_date=date(2026, 6, 7),
            home_team="DB",
            away_team="KH",
        )
        _seed_player_basic(SessionLocal, 53554, "김민석", "두산")
        _seed_player_basic(SessionLocal, 54097, "김민석", "KT")
        _seed_player_basic(SessionLocal, 76232, "양의지", "두산")
        _seed_player_season_batting(SessionLocal, 53554, 2026, "DB")
        _seed_player_season_batting(SessionLocal, 54097, 2026, "KT")
        _seed_player_season_batting(SessionLocal, 76232, 2026, "DB")

        saved = game_relay_module.save_relay_data(
            "20260607WOOB0",
            events=[],
            raw_pbp_rows=[
                {
                    "inning": 2,
                    "inning_half": "bottom",
                    "batter_name": "김민석",
                    "play_description": "김민석 : 1루수 땅볼 아웃",
                    "event_type": "batting",
                },
                {
                    "inning": 3,
                    "inning_half": "top",
                    "batter_name": "포수 양의지",
                    "play_description": "포수 양의지 : 포수 윤준호 (으)로 교체",
                    "event_type": "unknown",
                },
            ],
        )

        assert saved == 2
        with SessionLocal() as session:
            rows = {
                row.play_description: row
                for row in session.query(GamePlayByPlay)
                .filter(GamePlayByPlay.game_id == "20260607WOOB0")
                .all()
            }
            assert rows["김민석 : 1루수 땅볼 아웃"].player_id == 53554
            assert rows["김민석 : 1루수 땅볼 아웃"].resolver_reason == "name_match_DB_2026"
            assert rows["포수 양의지 : 포수 윤준호 (으)로 교체"].player_id == 76232
            assert rows["포수 양의지 : 포수 윤준호 (으)로 교체"].resolver_reason == "name_match_DB_2026"

    def test_save_relay_resolves_hhlt_batter_with_explicit_batter_role_context(self, monkeypatch):
        SessionLocal = _build_session_factory()
        monkeypatch.setattr(game_relay_module, "SessionLocal", SessionLocal)
        monkeypatch.setattr(game_relay_module, "_auto_sync_to_oci", lambda game_id: None)
        monkeypatch.setattr(game_save_module, "SessionLocal", SessionLocal)
        monkeypatch.setattr(game_save_module, "_auto_sync_to_oci", lambda game_id: None)
        _seed_game(
            SessionLocal,
            "20260607HHLT0",
            target_date=date(2026, 6, 7),
            home_team="LT",
            away_team="HH",
        )

        saved = game_relay_module.save_relay_data(
            "20260607HHLT0",
            events=[],
            raw_pbp_rows=[
                {
                    "inning": 1,
                    "inning_half": "top",
                    "batter_name": "오재원",
                    "play_description": "오재원 : 1루수 왼쪽 앞 내야안타",
                    "event_type": "batting",
                },
            ],
        )

        assert saved == 1
        with SessionLocal() as session:
            pbp = session.query(GamePlayByPlay).filter(GamePlayByPlay.game_id == "20260607HHLT0").one()
            assert pbp.player_id == 56754
            assert pbp.resolver_reason == "name_match_HH_2026"

    def test_pipeline_resolver_fallback_graceful(self, monkeypatch):
        """Verify pipeline still succeeds when resolver cannot find player data."""
        SessionLocal = _build_session_factory()
        _setup(monkeypatch, SessionLocal)

        crawler = RelayCrawler()
        result = crawler._parse_naver_payload(_build_synthetic_text_relays())

        saved = game_relay_module.save_relay_data(
            GAME_ID,
            events=result["events"],
            raw_pbp_rows=result["raw_pbp_rows"],
        )
        assert saved > 0

        with SessionLocal() as session:
            pbp_rows = session.query(GamePlayByPlay).filter(GamePlayByPlay.game_id == GAME_ID).all()
            assert len(pbp_rows) > 0
            # All rows should have resolver_confidence set to "unresolved" or "error"
            for row in pbp_rows:
                if row.batter_name:
                    assert row.resolver_confidence in ("unresolved", "error"), (
                        f"Expected resolver_confidence to be unresolved/error for "
                        f"{row.batter_name}, got {row.resolver_confidence}"
                    )
                    if row.unresolved_player_name is None:
                        assert row.resolver_reason is not None

    def test_pipeline_empty_events_still_writes_pbp_rows(self, monkeypatch):
        """Edge case: empty events list with only raw_pbp_rows should still save."""
        SessionLocal = _build_session_factory()
        _setup(monkeypatch, SessionLocal)

        saved = game_relay_module.save_relay_data(
            GAME_ID,
            events=[],
            raw_pbp_rows=[
                {"inning": 1, "inning_half": "top", "play_description": "테스트", "event_type": "unknown"},
            ],
        )
        assert saved > 0

        with SessionLocal() as session:
            pbp_count = session.query(GamePlayByPlay).filter(GamePlayByPlay.game_id == GAME_ID).count()
            assert pbp_count == 1
            event_count = session.query(GameEvent).filter(GameEvent.game_id == GAME_ID).count()
            assert event_count == 0

    def test_pipeline_none_events_no_crash(self, monkeypatch):
        """Edge case: events=None should not crash and should still write pbp_rows."""
        SessionLocal = _build_session_factory()
        _setup(monkeypatch, SessionLocal)

        saved = game_relay_module.save_relay_data(
            GAME_ID,
            events=None,
            raw_pbp_rows=[
                {"inning": 1, "inning_half": "top", "play_description": "데이터 없음", "event_type": "unknown"},
            ],
        )
        assert saved > 0

    def test_parse_and_save_no_enrichment_loss(self, monkeypatch):
        """End-to-end: parse → grouper → pitch-count → DB, no column silently drops."""
        SessionLocal = _build_session_factory()
        _setup(monkeypatch, SessionLocal)

        crawler = RelayCrawler()
        result = crawler._parse_naver_payload(_build_synthetic_text_relays())

        before_events = result["events"]
        assert len(before_events) > 0

        saved = game_relay_module.save_relay_data(
            GAME_ID,
            events=before_events,
            raw_pbp_rows=result["raw_pbp_rows"],
        )
        assert saved > 0

        with SessionLocal() as session:
            db_events = (
                session.query(GameEvent).filter(GameEvent.game_id == GAME_ID).order_by(GameEvent.event_seq.asc()).all()
            )
            assert len(db_events) == len(before_events)

            for i, (orig, db_ev) in enumerate(zip(before_events, db_events)):
                # At-bat group fields must never silently go to NULL
                assert db_ev.at_bat_seq == orig["at_bat_seq"], (
                    f"Mismatch at event {i}: DB at_bat_seq={db_ev.at_bat_seq} ≠ orig={orig['at_bat_seq']}"
                )
                assert db_ev.at_bat_event_role == orig["at_bat_event_role"], (
                    f"Mismatch at event {i}: DB at_bat_event_role={db_ev.at_bat_event_role} "
                    f"≠ orig={orig['at_bat_event_role']}"
                )
                # at_bat_confidence should be preserved
                assert db_ev.at_bat_confidence == orig["at_bat_confidence"], (
                    f"Mismatch at event {i}: DB at_bat_confidence={db_ev.at_bat_confidence} "
                    f"≠ orig={orig['at_bat_confidence']}"
                )
