import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.crawlers.team_batting_stats_crawler import TeamBattingStatsCrawler
from src.crawlers.team_pitching_stats_crawler import TeamPitchingStatsCrawler
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching
from src.models.standings import TeamStandingsDaily
from src.models.team import Team
from src.models.team_stats import TeamSeasonBatting, TeamSeasonPitching


@pytest.fixture
def test_db():
    engine = create_engine("sqlite:///:memory:")
    # Create tables
    for table in (
        Team.__table__,
        TeamStandingsDaily.__table__,
        PlayerSeasonBatting.__table__,
        PlayerSeasonPitching.__table__,
        TeamSeasonBatting.__table__,
        TeamSeasonPitching.__table__,
    ):
        table.create(bind=engine)
    TestSession = sessionmaker(bind=engine)
    session = TestSession()

    # Seed team
    session.add(Team(team_id="OB", team_name="두산 베어스", team_short_name="두산", city="서울"))
    session.commit()

    yield session
    session.close()


def test_team_batting_stats_crawler_fallback(test_db, monkeypatch):
    monkeypatch.setattr("src.crawlers.team_batting_stats_crawler.get_team_mapping_for_year", lambda _year: {"OB": "OB"})
    session = test_db
    # Seed player batting data
    session.add(
        PlayerSeasonBatting(
            id=1,
            player_id=101,
            season=2025,
            league="REGULAR",
            level="KBO1",
            source="CRAWLER",
            team_code="OB",
            games=10,
            plate_appearances=40,
            at_bats=35,
            runs=5,
            hits=10,
            doubles=2,
            triples=0,
            home_runs=1,
            rbi=5,
            walks=4,
            intentional_walks=0,
            hbp=1,
            strikeouts=5,
            stolen_bases=1,
            caught_stealing=0,
            sacrifice_hits=0,
            sacrifice_flies=0,
            gdp=1,
        ),
    )
    session.commit()

    # Mock SessionLocal and _collect_from_site
    monkeypatch.setattr("src.crawlers.team_batting_stats_crawler.SessionLocal", lambda: session)

    crawler = TeamBattingStatsCrawler()
    # Force _collect_from_site to fail
    monkeypatch.setattr(crawler, "_collect_from_site", lambda *args, **kwargs: [])

    # Run crawl with persist=False
    results = crawler.crawl(2025, persist=False)

    assert len(results) == 1
    assert results[0]["team_id"] == "OB"
    assert results[0]["hits"] == 10

    # Since persist=False, nothing should be saved to database
    db_records = session.query(TeamSeasonBatting).all()
    assert len(db_records) == 0

    # Run crawl with persist=True (default)
    results_persist = crawler.crawl(2025, persist=True)
    assert len(results_persist) == 1

    # Now it should be saved to database
    db_records_persist = session.query(TeamSeasonBatting).all()
    assert len(db_records_persist) == 1
    assert db_records_persist[0].team_id == "OB"
    assert db_records_persist[0].hits == 10


def test_team_pitching_stats_crawler_fallback(test_db, monkeypatch):
    monkeypatch.setattr(
        "src.crawlers.team_pitching_stats_crawler.get_team_mapping_for_year",
        lambda _year: {"OB": "OB"},
    )
    session = test_db
    # Seed player pitching data
    session.add(
        PlayerSeasonPitching(
            id=1,
            player_id=101,
            season=2025,
            league="REGULAR",
            level="KBO1",
            source="CRAWLER",
            team_code="OB",
            games=5,
            wins=2,
            losses=1,
            saves=1,
            holds=0,
            innings_pitched=15.0,
            innings_outs=45,
            hits_allowed=10,
            runs_allowed=4,
            earned_runs=3,
            home_runs_allowed=1,
            walks_allowed=5,
            strikeouts=15,
            tbf=60,
            complete_games=0,
            shutouts=0,
            wild_pitches=0,
            balks=0,
            sacrifices_allowed=0,
            sacrifice_flies_allowed=0,
        ),
    )
    session.commit()

    # Mock SessionLocal and _collect_from_site
    monkeypatch.setattr("src.crawlers.team_pitching_stats_crawler.SessionLocal", lambda: session)

    crawler = TeamPitchingStatsCrawler()
    # Force _collect_from_site to fail
    monkeypatch.setattr(crawler, "_collect_from_site", lambda *args, **kwargs: [])

    # Run crawl with persist=False
    results = crawler.crawl(2025, persist=False)

    assert len(results) == 1
    assert results[0]["team_id"] == "OB"
    assert results[0]["wins"] == 2

    # Since persist=False, nothing should be saved to database
    db_records = session.query(TeamSeasonPitching).all()
    assert len(db_records) == 0

    # Run crawl with persist=True (default)
    results_persist = crawler.crawl(2025, persist=True)
    assert len(results_persist) == 1

    # Now it should be saved to database
    db_records_persist = session.query(TeamSeasonPitching).all()
    assert len(db_records_persist) == 1
    assert db_records_persist[0].team_id == "OB"
    assert db_records_persist[0].wins == 2
