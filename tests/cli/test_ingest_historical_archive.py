"""Unit tests for ingest_historical_archive CLI."""

from __future__ import annotations

import hashlib
import json

import pytest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.cli.ingest_historical_archive import HistoricalArchiveIngestor, main
from src.models.base import Base
from src.models.game import Game, GameBattingStat, GameInningScore, GameMetadata, GamePitchingStat
from src.models.player import PlayerBasic, PlayerSeasonBatting, PlayerSeasonPitching
from src.models.team_stats import TeamSeasonBatting, TeamSeasonPitching
from scripts.converters.convert_kbo_archive_records import generate_season_dataset


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine, expire_on_commit=False)
    sess = session_factory()
    try:
        yield sess
    finally:
        sess.close()


def test_ingest_season_archive_provenance(session) -> None:
    """HistoricalArchiveIngestor should parse structured historical payload and save provenance metadata."""
    payload = {
        "games": [
            {
                "game_id": "19800327SSMB0",
                "game_date": "1980-03-27",
                "away_team": "SS",
                "home_team": "MB",
                "away_score": 7,
                "home_score": 11,
                "stadium": "동대문",
                "game_status": "COMPLETED",
            }
        ],
        "game_inning_scores": [
            {"game_id": "19800327SSMB0", "team_side": "away", "inning": 1, "runs": 2},
            {"game_id": "19800327SSMB0", "team_side": "home", "inning": 1, "runs": 3},
        ],
        "game_batting_stats": [
            {
                "game_id": "19800327SSMB0",
                "team_side": "home",
                "player_id": 198001,
                "player_name": "백인천",
                "team_code": "MB",
                "appearance_seq": 1,
                "pa": 5,
                "ab": 4,
                "hits": 3,
                "hr": 1,
                "rbi": 4,
                "bb": 1,
            }
        ],
        "game_pitching_stats": [
            {
                "game_id": "19800327SSMB0",
                "team_side": "home",
                "player_id": 198011,
                "player_name": "하기룡",
                "team_code": "MB",
                "appearance_seq": 1,
                "ip": 9.0,
                "r": 7,
                "er": 5,
                "so": 8,
                "bb": 3,
                "wins": 1,
            }
        ],
        "player_season_batting": [
            {
                "player_id": 198001,
                "player_name": "백인천",
                "team_code": "MB",
                "games": 72,
                "pa": 298,
                "ab": 250,
                "hits": 103,
                "hr": 19,
                "rbi": 64,
                "bb": 42,
                "avg": 0.412,
            }
        ],
        "player_season_pitching": [
            {
                "player_id": 198011,
                "player_name": "하기룡",
                "team_code": "MB",
                "games": 36,
                "era": 2.30,
                "wins": 13,
                "losses": 10,
                "saves": 9,
                "ip": "191.2",
                "so": 107,
            }
        ],
        "team_season_batting": [
            {
                "team_code": "OB",
                "games": 80,
                "avg": 0.283,
                "pa": 3100,
                "ab": 2700,
                "hits": 764,
                "hr": 57,
            }
        ],
        "team_season_pitching": [
            {
                "team_code": "OB",
                "games": 80,
                "ip": 720.0,
                "r": 300,
                "er": 250,
                "so": 500,
            }
        ],
    }

    ingestor = HistoricalArchiveIngestor(session)
    summary = ingestor.ingest_season_archive(1980, payload, source_name="kbo_official_archive")

    assert summary.season == 1980
    assert summary.games_ingested == 1
    assert summary.innings_records_ingested == 2
    assert summary.game_batting_records_ingested == 1
    assert summary.game_pitching_records_ingested == 1
    assert summary.batting_records_ingested == 1
    assert summary.pitching_records_ingested == 1
    assert summary.team_batting_records_ingested == 1
    assert summary.team_pitching_records_ingested == 1
    assert summary.source_name == "kbo_official_archive"
    assert summary.provenance_verified is True

    # Verify DB records
    game = session.query(Game).filter(Game.game_id == "19800327SSMB0").first()
    assert game is not None
    assert game.home_score == 11
    assert game.away_score == 7

    meta = session.query(GameMetadata).filter(GameMetadata.game_id == "19800327SSMB0").first()
    assert meta is not None
    assert meta.source_payload["source_name"] == "kbo_official_archive"
    assert meta.source_payload["provenance"] == {}

    # Verify Inning Score
    innings = session.query(GameInningScore).filter(GameInningScore.game_id == "19800327SSMB0").all()
    assert len(innings) == 2

    # Verify Game Batting Stat
    gb = session.query(GameBattingStat).filter(GameBattingStat.game_id == "19800327SSMB0").first()
    assert gb is not None
    assert gb.hits == 3

    # Verify Game Pitching Stat
    gp = session.query(GamePitchingStat).filter(GamePitchingStat.game_id == "19800327SSMB0").first()
    assert gp is not None
    assert gp.strikeouts == 8

    season_bat = session.query(PlayerSeasonBatting).filter(PlayerSeasonBatting.player_id == 198001).first()
    assert season_bat is not None
    assert season_bat.season == 1980
    assert season_bat.plate_appearances == 298
    assert season_bat.source == "OFFICIAL_ARCHIVE"

    season_pit = session.query(PlayerSeasonPitching).filter(PlayerSeasonPitching.player_id == 198011).first()
    assert season_pit is not None
    assert season_pit.innings_pitched == pytest.approx(191.2)
    assert season_pit.strikeouts == 107
    assert season_pit.source == "OFFICIAL_ARCHIVE"

    team_bat = session.query(TeamSeasonBatting).filter(TeamSeasonBatting.team_id == "OB").first()
    assert team_bat is not None
    assert team_bat.season == 1980
    assert team_bat.games == 80
    assert team_bat.team_name == "OB"

    team_pit = session.query(TeamSeasonPitching).filter(TeamSeasonPitching.team_id == "OB").first()
    assert team_pit is not None
    assert team_pit.innings_pitched == pytest.approx(720.0)
    assert team_pit.strikeouts == 500

    ingestor.ingest_season_archive(1980, payload, source_name="kbo_official_archive")
    assert session.query(PlayerSeasonBatting).filter(PlayerSeasonBatting.player_id == 198001).count() == 1
    assert session.query(PlayerSeasonPitching).filter(PlayerSeasonPitching.player_id == 198011).count() == 1
    assert session.query(TeamSeasonBatting).filter(TeamSeasonBatting.team_id == "OB").count() == 1
    assert session.query(TeamSeasonPitching).filter(TeamSeasonPitching.team_id == "OB").count() == 1


def test_ingestor_stores_verified_manifest_provenance(session) -> None:
    payload = {
        "games": [
            {
                "game_id": "19820326OBLT0",
                "game_date": "1982-03-26",
                "away_team": "LT",
                "home_team": "OB",
            }
        ]
    }
    manifest = {
        "source_name": "approved_archive",
        "source_url": "https://example.invalid/1982",
        "authorization_ref": "approval-1982",
        "sha256": "a" * 64,
        "season": 1982,
    }

    HistoricalArchiveIngestor(session).ingest_season_archive(1982, payload, provenance=manifest)

    metadata = session.query(GameMetadata).filter(GameMetadata.game_id == "19820326OBLT0").one()
    assert metadata.source_payload["source_name"] == "approved_archive"
    assert metadata.source_payload["provenance"] == manifest


def test_ingest_historical_archive_cli_execution(tmp_path, capsys) -> None:
    """CLI should execute successfully given a valid JSON file."""
    test_data = {
        "games": [],
        "game_inning_scores": [],
        "game_batting_stats": [],
        "game_pitching_stats": [],
        "player_season_batting": [],
        "player_season_pitching": [],
        "team_season_batting": [],
    }
    file_path = tmp_path / "test_1980.json"
    file_path.write_text(json.dumps(test_data), encoding="utf-8")

    exit_code = main(["--file", str(file_path), "--season", "1980", "--json"])
    assert exit_code == 0

    captured = capsys.readouterr()
    result = json.loads(captured.out)
    assert result["season"] == 1980
    assert result["status"] == "SUCCESS"


def test_historical_archive_requires_manifest(tmp_path) -> None:
    archive_path = tmp_path / "archive_1982.json"
    archive_path.write_text(json.dumps({"games": []}), encoding="utf-8")

    assert main(["--file", str(archive_path), "--season", "1982"]) == 2


def test_ingestor_rejects_historical_write_without_provenance(session) -> None:
    ingestor = HistoricalArchiveIngestor(session)
    with pytest.raises(ValueError, match="provenance is required"):
        ingestor.ingest_season_archive(1982, {"games": []})


def test_ingestor_rejects_synthetic_archive_payload(session) -> None:
    """Generated fixture payloads must never be stored as historical facts."""
    ingestor = HistoricalArchiveIngestor(session)
    fixture_payload = generate_season_dataset(1982)
    with pytest.raises(ValueError, match="Synthetic archive payload"):
        ingestor.ingest_season_archive(
            1982,
            fixture_payload,
            provenance=fixture_payload["provenance"],
        )


def test_historical_archive_manifest_checksum_and_dry_run(tmp_path, capsys) -> None:
    archive_path = tmp_path / "archive_1982.json"
    archive_path.write_text(
        json.dumps({"games": [{"game_id": "19820326OBLT0"}]}),
        encoding="utf-8",
    )
    manifest_path = tmp_path / "archive_1982.manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "source_name": "kbo_official_archive",
                "source_url": "https://example.invalid/kbo-archive",
                "authorization_ref": "approval-1982-fixture",
                "sha256": hashlib.sha256(archive_path.read_bytes()).hexdigest(),
                "season": 1982,
            }
        ),
        encoding="utf-8",
    )

    assert (
        main(
            [
                "--file",
                str(archive_path),
                "--season",
                "1982",
                "--manifest",
                str(manifest_path),
                "--dry-run",
                "--json",
            ],
        )
        == 0
    )
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "DRY_RUN"
    assert result["provenance_verified"] is True
    assert result["games_ingested"] == 1
    assert result["source_name"] == "kbo_official_archive"


def test_historical_archive_rejects_checksum_mismatch(tmp_path) -> None:
    archive_path = tmp_path / "archive_1982.json"
    archive_path.write_text(json.dumps({"games": []}), encoding="utf-8")
    manifest_path = tmp_path / "archive_1982.manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "source_name": "kbo_official_archive",
                "source_url": "https://example.invalid/kbo-archive",
                "authorization_ref": "approval-1982-fixture",
                "sha256": "0" * 64,
                "season": 1982,
            }
        ),
        encoding="utf-8",
    )

    assert main(["--file", str(archive_path), "--season", "1982", "--manifest", str(manifest_path)]) == 2


@pytest.mark.parametrize(
    ("manifest_update", "payload"),
    [
        ({"source_url": ""}, {"games": []}),
        ({"season": 1983}, {"games": []}),
        ({}, {"games": [{"game_id": "19820326OBLT0"}, {"game_id": "19820326OBLT0"}]}),
        ({}, {"games": [{"game_id": "19830326OBLT0"}]}),
    ],
)
def test_historical_archive_rejects_invalid_manifest_or_game_ids(tmp_path, manifest_update, payload) -> None:
    archive_path = tmp_path / "archive_1982.json"
    archive_path.write_text(json.dumps(payload), encoding="utf-8")
    manifest = {
        "source_name": "kbo_official_archive",
        "source_url": "https://example.invalid/kbo-archive",
        "authorization_ref": "approval-1982-fixture",
        "sha256": hashlib.sha256(archive_path.read_bytes()).hexdigest(),
        "season": 1982,
    }
    manifest.update(manifest_update)
    manifest_path = tmp_path / "archive_1982.manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    assert main(["--file", str(archive_path), "--season", "1982", "--manifest", str(manifest_path)]) == 2
