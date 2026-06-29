from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.player import PlayerBasic, PlayerSeasonBatting
from src.models.team import Team
from src.sync.sync_stats import StatsSyncMixin


def test_player_season_batting_sync_filters_missing_player_basic_refs():
    engine = create_engine("sqlite:///:memory:")
    for table in (Team.__table__, PlayerBasic.__table__, PlayerSeasonBatting.__table__):
        table.create(bind=engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)

    class _Syncer(StatsSyncMixin):
        def __init__(self, session):
            self.sqlite_session = session
            self.synced_player_ids = []

        def sync_simple_table(self, model, *, filters=None, **_kwargs):
            self.synced_player_ids = [
                row.player_id for row in self.sqlite_session.query(model).filter(*(filters or [])).all()
            ]
            return len(self.synced_player_ids)

    with SessionLocal() as session:
        session.add(PlayerBasic(player_id=1001, name="홍길동"))
        session.add_all(
            [
                PlayerSeasonBatting(player_id=1001, season=2026, league="REGULAR", level="KBO1", source="CRAWLER"),
                PlayerSeasonBatting(player_id=2002, season=2026, league="REGULAR", level="KBO1", source="CRAWLER"),
            ],
        )
        session.commit()

        syncer = _Syncer(session)
        synced = syncer.sync_player_season_batting(year=2026, force=True)

    assert synced == 1
    assert syncer.synced_player_ids == [1001]
