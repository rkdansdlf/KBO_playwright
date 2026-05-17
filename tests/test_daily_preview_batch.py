from __future__ import annotations

import asyncio

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import src.cli.daily_preview_batch as daily_preview_batch
import src.repositories.game_repository as game_repository
from src.models.game import Game, GameIdAlias, GameLineup, GameMetadata, GameSummary
from src.models.player import PlayerBasic, PlayerSeasonBatting, PlayerSeasonPitching
from src.models.team import Team


def _build_session_factory():
    engine = create_engine("sqlite:///:memory:")
    for table in (
        Team.__table__,
        PlayerBasic.__table__,
        PlayerSeasonBatting.__table__,
        PlayerSeasonPitching.__table__,
        Game.__table__,
        GameIdAlias.__table__,
        GameMetadata.__table__,
        GameLineup.__table__,
        GameSummary.__table__,
    ):
        table.create(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


class _FakePreviewCrawler:
    def __init__(self, request_delay: float):
        self.request_delay = request_delay

    async def crawl_preview_for_date(self, target_date: str):
        return [
            {
                "game_id": f"{target_date}NCLT0",
                "game_date": target_date,
                "stadium": "창원",
                "start_time": "18:30",
                "away_lineup": [
                    {
                        "player_name": "신규타자",
                        "batting_order": 1,
                        "position": "중견수",
                        "uniform_no": "9",
                    }
                ],
                "home_lineup": [],
            }
        ]


class _ForeignKeyCheckingSyncer:
    created = []

    def __init__(self, oci_url, sqlite_session):
        self.oci_url = oci_url
        self.sqlite_session = sqlite_session
        self.calls = []
        self.synced_player_ids = set()
        self.closed = False
        _ForeignKeyCheckingSyncer.created.append(self)

    def sync_player_basic(self):
        self.calls.append("player_basic")
        rows = self.sqlite_session.query(PlayerBasic.player_id).all()
        self.synced_player_ids.update(row[0] for row in rows)
        return len(rows)

    def sync_players(self):
        self.calls.append("players")
        return 0

    def sync_pregame_game(self, game_id: str):
        self.calls.append(f"pregame:{game_id}")
        referenced_player_ids = {
            row[0]
            for row in self.sqlite_session.query(GameLineup.player_id)
            .filter(GameLineup.game_id == game_id, GameLineup.player_id.isnot(None))
            .all()
        }
        missing_player_ids = referenced_player_ids - self.synced_player_ids
        if missing_player_ids:
            raise AssertionError(f"game_lineups -> player_basic missing {sorted(missing_player_ids)}")
        return {"lineups": len(referenced_player_ids)}

    def close(self):
        self.closed = True


def test_preview_sync_leaves_unresolved_player_lineup_null(monkeypatch):
    SessionLocal = _build_session_factory()
    _ForeignKeyCheckingSyncer.created = []

    monkeypatch.setattr(daily_preview_batch, "PreviewCrawler", _FakePreviewCrawler)
    monkeypatch.setattr(daily_preview_batch, "SessionLocal", SessionLocal)
    monkeypatch.setattr(daily_preview_batch, "OCISync", _ForeignKeyCheckingSyncer)
    monkeypatch.setattr(daily_preview_batch, "write_refresh_manifest", lambda **_kwargs: "manifest.json")
    monkeypatch.setattr(game_repository, "SessionLocal", SessionLocal)
    monkeypatch.setattr(game_repository, "_auto_sync_to_oci", lambda _game_id: None)
    monkeypatch.setenv("OCI_DB_URL", "postgresql://example")

    saved_ids = asyncio.run(daily_preview_batch.run_preview_batch("20260515", sync_to_oci=True))

    assert saved_ids == ["20260515NCLT0"]
    assert len(_ForeignKeyCheckingSyncer.created) == 1
    syncer = _ForeignKeyCheckingSyncer.created[0]
    assert syncer.calls == ["player_basic", "players", "pregame:20260515NCLT0"]
    assert syncer.closed is True

    with SessionLocal() as session:
        lineup = session.query(GameLineup).filter(GameLineup.game_id == "20260515NCLT0").one()
        assert lineup.player_id is None
        assert session.query(PlayerBasic).filter(PlayerBasic.name == "신규타자").count() == 0
        assert syncer.synced_player_ids == set()
