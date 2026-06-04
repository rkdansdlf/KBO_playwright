import datetime
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import scripts.verification.audit_fallback_stats as audit_module
import src.repositories.player_season_pitching_repository as safe_pitch_repo
import src.repositories.safe_batting_repository as safe_bat_repo
import src.repositories.team_stats_repository as team_stats_repo
from src.models.game import Game, GameBattingStat, GamePitchingStat
from src.models.player import PlayerBasic, PlayerSeasonBatting, PlayerSeasonPitching
from src.models.season import KboSeason
from src.models.team import Team


def test_audit_batting_remediation_within_threshold(tmp_path):
    # Setup test database
    engine = create_engine(f"sqlite:///{tmp_path / 'test_audit.db'}")
    for table in (
        Team.__table__,
        KboSeason.__table__,
        Game.__table__,
        GameBattingStat.__table__,
        PlayerBasic.__table__,
        PlayerSeasonBatting.__table__,
    ):
        table.create(bind=engine)

    TestSessionLocal = sessionmaker(bind=engine)

    with TestSessionLocal() as session:
        # Seed Team
        team = Team(team_id="OB", team_name="두산 베어스", team_short_name="두산", city="서울")
        session.add(team)

        # Seed Season
        season = KboSeason(season_id=1, season_year=2025, league_type_code=1, league_type_name="정규시즌")
        session.add(season)

        # Seed Player
        player = PlayerBasic(player_id=1001, name="홍길동")
        session.add(player)

        # Seed Game
        game = Game(game_id="20250401OBWO0", season_id=1, game_date=datetime.date(2025, 4, 1))
        session.add(game)

        # Seed Game Batting Stat (calculated should be: games=1, hits=2, at_bats=4)
        game_bat = GameBattingStat(
            game_id="20250401OBWO0",
            player_id=1001,
            player_name="홍길동",
            team_code="OB",
            team_side="away",
            appearance_seq=1,
            plate_appearances=4,
            at_bats=4,
            hits=2,
            home_runs=0,
            rbi=1,
            walks=0,
        )
        session.add(game_bat)

        # Seed Official Player Season Batting with mismatch (hits=5 instead of 2)
        off_bat = PlayerSeasonBatting(
            player_id=1001,
            season=2025,
            league="REGULAR",
            level="KBO1",
            source="CRAWLER",
            team_code="OB",
            games=1,
            plate_appearances=4,
            at_bats=4,
            hits=5,  # Mismatch!
            home_runs=0,
            rbi=1,
            walks=0,
        )
        session.add(off_bat)
        session.commit()

    # Now mock SessionLocal in the audit module and repositories to use our TestSessionLocal
    with (
        patch.object(audit_module, "SessionLocal", TestSessionLocal),
        patch.object(safe_bat_repo, "SessionLocal", TestSessionLocal),
        patch.object(safe_pitch_repo, "SessionLocal", TestSessionLocal),
        patch.object(team_stats_repo, "SessionLocal", TestSessionLocal),
    ):
        with patch("src.utils.alerting.SlackWebhookClient.send_alert") as mock_send_alert:
            # 1. Run audit with fix=False -> Should NOT fix it, but send warning alert
            audit_module.StatAudit.audit_batting(2025, "regular", fix=False)

            with TestSessionLocal() as session:
                db_record = session.query(PlayerSeasonBatting).filter_by(player_id=1001).first()
                assert db_record.hits == 5  # Unchanged
                assert mock_send_alert.call_count == 1  # warning alert for mismatches

            # 2. Run audit with fix=True -> Should fix it since it's within thresholds
            audit_module.StatAudit.audit_batting(2025, "regular", fix=True, max_mismatches=2, max_game_diff=5)

            with TestSessionLocal() as session:
                records = session.query(PlayerSeasonBatting).all()
                for r in records:
                    print(
                        f"DEBUG RECORD: player_id={r.player_id}, season={r.season}, league={r.league}, level={r.level}, hits={r.hits}, source={r.source}"
                    )
                db_record = session.query(PlayerSeasonBatting).filter_by(player_id=1001).first()
                assert db_record.hits == 2  # Fixed!
                assert db_record.source == "AUDIT_FIX"
                assert mock_send_alert.call_count == 2  # warning + success alerts


def test_audit_batting_remediation_aborted_by_max_mismatches(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'test_audit_abort.db'}")
    for table in (
        Team.__table__,
        KboSeason.__table__,
        Game.__table__,
        GameBattingStat.__table__,
        PlayerBasic.__table__,
        PlayerSeasonBatting.__table__,
    ):
        table.create(bind=engine)

    TestSessionLocal = sessionmaker(bind=engine)

    with TestSessionLocal() as session:
        team = Team(team_id="OB", team_name="두산 베어스", team_short_name="두산", city="서울")
        session.add(team)

        season = KboSeason(season_id=1, season_year=2025, league_type_code=1, league_type_name="정규시즌")
        session.add(season)

        # Seed 2 players with mismatches
        for pid, name in [(1001, "홍길동"), (1002, "임꺽정")]:
            player = PlayerBasic(player_id=pid, name=name)
            session.add(player)

            game = Game(game_id=f"20250401OBWO{pid}", season_id=1, game_date=datetime.date(2025, 4, 1))
            session.add(game)

            game_bat = GameBattingStat(
                game_id=f"20250401OBWO{pid}",
                player_id=pid,
                player_name=name,
                team_code="OB",
                team_side="away",
                appearance_seq=1,
                plate_appearances=4,
                at_bats=4,
                hits=2,
                home_runs=0,
                rbi=1,
                walks=0,
            )
            session.add(game_bat)

            off_bat = PlayerSeasonBatting(
                player_id=pid,
                season=2025,
                league="REGULAR",
                level="KBO1",
                source="CRAWLER",
                team_code="OB",
                games=1,
                plate_appearances=4,
                at_bats=4,
                hits=5,  # Mismatch!
                home_runs=0,
                rbi=1,
                walks=0,
            )
            session.add(off_bat)
        session.commit()

    with (
        patch.object(audit_module, "SessionLocal", TestSessionLocal),
        patch.object(safe_bat_repo, "SessionLocal", TestSessionLocal),
        patch.object(safe_pitch_repo, "SessionLocal", TestSessionLocal),
        patch.object(team_stats_repo, "SessionLocal", TestSessionLocal),
    ):
        with patch("src.utils.alerting.SlackWebhookClient.send_alert") as mock_send_alert:
            # Run audit with fix=True, but max_mismatches=1 -> Should abort!
            audit_module.StatAudit.audit_batting(2025, "regular", fix=True, max_mismatches=1, max_game_diff=5)

            with TestSessionLocal() as session:
                records = session.query(PlayerSeasonBatting).all()
                for r in records:
                    assert r.hits == 5  # Unchanged (remediation aborted)

                # Verify mock_send_alert was called once to notify manager of abortion
                assert mock_send_alert.call_count == 1
                args, kwargs = mock_send_alert.call_args
                assert (
                    "Auto-Remediation Aborted" in args[0]
                    or "Auto-Remediation Aborted" in str(args)
                    or "Aborted" in str(kwargs)
                )


def test_audit_batting_remediation_aborted_by_max_game_diff(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'test_audit_game_diff.db'}")
    for table in (
        Team.__table__,
        KboSeason.__table__,
        Game.__table__,
        GameBattingStat.__table__,
        PlayerBasic.__table__,
        PlayerSeasonBatting.__table__,
    ):
        table.create(bind=engine)

    TestSessionLocal = sessionmaker(bind=engine)

    with TestSessionLocal() as session:
        team = Team(team_id="OB", team_name="두산 베어스", team_short_name="두산", city="서울")
        session.add(team)

        season = KboSeason(season_id=1, season_year=2025, league_type_code=1, league_type_name="정규시즌")
        session.add(season)

        player = PlayerBasic(player_id=1001, name="홍길동")
        session.add(player)

        game = Game(game_id="20250401OBWO0", season_id=1, game_date=datetime.date(2025, 4, 1))
        session.add(game)

        # calculated games = 1
        game_bat = GameBattingStat(
            game_id="20250401OBWO0",
            player_id=1001,
            player_name="홍길동",
            team_code="OB",
            team_side="away",
            appearance_seq=1,
            plate_appearances=4,
            at_bats=4,
            hits=2,
            home_runs=0,
            rbi=1,
            walks=0,
        )
        session.add(game_bat)

        # Official games = 20 (game difference of 19 games!)
        off_bat = PlayerSeasonBatting(
            player_id=1001,
            season=2025,
            league="REGULAR",
            level="KBO1",
            source="CRAWLER",
            team_code="OB",
            games=20,  # Huge mismatch!
            plate_appearances=4,
            at_bats=4,
            hits=2,
            home_runs=0,
            rbi=1,
            walks=0,
        )
        session.add(off_bat)
        session.commit()

    with (
        patch.object(audit_module, "SessionLocal", TestSessionLocal),
        patch.object(safe_bat_repo, "SessionLocal", TestSessionLocal),
        patch.object(safe_pitch_repo, "SessionLocal", TestSessionLocal),
        patch.object(team_stats_repo, "SessionLocal", TestSessionLocal),
    ):
        with patch("src.utils.alerting.SlackWebhookClient.send_alert") as mock_send_alert:
            # Run audit with fix=True, but max_game_diff=15 -> Should abort!
            audit_module.StatAudit.audit_batting(2025, "regular", fix=True, max_mismatches=5, max_game_diff=15)

            with TestSessionLocal() as session:
                db_record = session.query(PlayerSeasonBatting).filter_by(player_id=1001).first()
                assert db_record.games == 20  # Unchanged (remediation aborted)
                assert mock_send_alert.call_count == 1


def test_audit_pitching_remediation_within_threshold(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'test_audit_pitch.db'}")
    for table in (
        Team.__table__,
        KboSeason.__table__,
        Game.__table__,
        GamePitchingStat.__table__,
        PlayerBasic.__table__,
        PlayerSeasonPitching.__table__,
    ):
        table.create(bind=engine)

    TestSessionLocal = sessionmaker(bind=engine)

    with TestSessionLocal() as session:
        team = Team(team_id="OB", team_name="두산 베어스", team_short_name="두산", city="서울")
        session.add(team)

        season = KboSeason(season_id=1, season_year=2025, league_type_code=1, league_type_name="정규시즌")
        session.add(season)

        player = PlayerBasic(player_id=5001, name="김투수")
        session.add(player)

        game = Game(game_id="20250401OBWO0", season_id=1, game_date=datetime.date(2025, 4, 1))
        session.add(game)

        # Seed game-level pitching stat (source of truth for "calculated" values)
        game_pitch = GamePitchingStat(
            game_id="20250401OBWO0",
            player_id=5001,
            player_name="김투수",
            team_code="OB",
            team_side="home",
            appearance_seq=1,
            is_starting=True,
            decision="W",
            innings_outs=27,
            earned_runs=2,
            hits_allowed=8,
            runs_allowed=2,
            walks_allowed=2,
            strikeouts=8,
        )
        session.add(game_pitch)
        session.commit()

        # Seed official season pitching with mismatched values
        off_pitch = PlayerSeasonPitching(
            player_id=5001,
            season=2025,
            league="REGULAR",
            level="KBO1",
            source="CRAWLER",
            team_code="OB",
            games=1,
            wins=3,  # Mismatch! (should be 1 from game data)
            losses=0,
            saves=0,
            holds=0,
            innings_pitched=9.0,
            innings_outs=27,
            earned_runs=5,  # Mismatch! (should be 2)
            hits_allowed=8,
            runs_allowed=2,
            home_runs_allowed=0,
            walks_allowed=2,
            strikeouts=8,
        )
        session.add(off_pitch)
        session.commit()

    with (
        patch.object(audit_module, "SessionLocal", TestSessionLocal),
        patch.object(safe_bat_repo, "SessionLocal", TestSessionLocal),
        patch.object(safe_pitch_repo, "SessionLocal", TestSessionLocal),
        patch.object(team_stats_repo, "SessionLocal", TestSessionLocal),
    ):
        with patch("src.utils.alerting.SlackWebhookClient.send_alert") as mock_send_alert:
            audit_module.StatAudit.audit_pitching(2025, "regular", fix=True, max_mismatches=2, max_game_diff=5)

            with TestSessionLocal() as session:
                db_record = session.query(PlayerSeasonPitching).filter_by(player_id=5001).first()
                assert db_record.wins == 1  # Fixed!
                assert db_record.earned_runs == 2  # Fixed!
                assert mock_send_alert.call_count == 1  # success alert


def test_audit_pitching_remediation_aborted_by_max_innings_outs_diff(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'test_audit_pitch_outs.db'}")
    for table in (
        Team.__table__,
        KboSeason.__table__,
        Game.__table__,
        GamePitchingStat.__table__,
        PlayerBasic.__table__,
        PlayerSeasonPitching.__table__,
    ):
        table.create(bind=engine)

    TestSessionLocal = sessionmaker(bind=engine)

    with TestSessionLocal() as session:
        team = Team(team_id="OB", team_name="두산 베어스", team_short_name="두산", city="서울")
        session.add(team)

        season = KboSeason(season_id=1, season_year=2025, league_type_code=1, league_type_name="정규시즌")
        session.add(season)

        player = PlayerBasic(player_id=6001, name="박투수")
        session.add(player)

        game = Game(game_id="20250401OBWO0", season_id=1, game_date=datetime.date(2025, 4, 1))
        session.add(game)

        # Seed game-level pitching stat with 27 innings outs
        game_pitch = GamePitchingStat(
            game_id="20250401OBWO0",
            player_id=6001,
            player_name="박투수",
            team_code="OB",
            team_side="home",
            appearance_seq=1,
            is_starting=True,
            innings_outs=27,
            earned_runs=2,
            hits_allowed=8,
            runs_allowed=2,
            walks_allowed=2,
            strikeouts=8,
        )
        session.add(game_pitch)
        session.commit()

        # Seed official season pitching with huge innings_outs mismatch
        off_pitch = PlayerSeasonPitching(
            player_id=6001,
            season=2025,
            league="REGULAR",
            level="KBO1",
            source="CRAWLER",
            team_code="OB",
            games=1,
            wins=1,
            losses=0,
            saves=0,
            holds=0,
            innings_pitched=9.0,
            innings_outs=120,  # Huge mismatch (27 vs 120)!
            earned_runs=2,
            hits_allowed=8,
            runs_allowed=2,
            home_runs_allowed=0,
            walks_allowed=2,
            strikeouts=8,
        )
        session.add(off_pitch)
        session.commit()

    with (
        patch.object(audit_module, "SessionLocal", TestSessionLocal),
        patch.object(safe_bat_repo, "SessionLocal", TestSessionLocal),
        patch.object(safe_pitch_repo, "SessionLocal", TestSessionLocal),
        patch.object(team_stats_repo, "SessionLocal", TestSessionLocal),
    ):
        with patch("src.utils.alerting.SlackWebhookClient.send_alert") as mock_send_alert:
            audit_module.StatAudit.audit_pitching(
                2025, "regular", fix=True, max_mismatches=5, max_game_diff=5, max_innings_outs_diff=30
            )

            with TestSessionLocal() as session:
                db_record = session.query(PlayerSeasonPitching).filter_by(player_id=6001).first()
                assert db_record.innings_outs == 120  # Unchanged (aborted)
                assert mock_send_alert.call_count == 1  # abort alert
