from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.sync.runtime_hydrator import HydrationSpec, RuntimeHydrator

pytestmark = pytest.mark.usefixtures("_db_engine")


@pytest.fixture
def _hydrator_engine_pair():
    engine1 = create_engine("sqlite:///:memory:", echo=False)
    engine2 = create_engine("sqlite:///:memory:", echo=False)
    Base = pytest.importorskip("src.models.base").Base
    Base.metadata.create_all(bind=engine1)
    Base.metadata.create_all(bind=engine2)
    s1 = sessionmaker(bind=engine1)()
    s2 = sessionmaker(bind=engine2)()
    hydrator = RuntimeHydrator(s1, s2)
    yield hydrator, engine1, engine2, s1, s2
    s1.close()
    s2.close()
    engine1.dispose()
    engine2.dispose()


class TestHydrationSpec:
    def test_create_spec(self):
        from src.models.game import Game

        spec = HydrationSpec(
            label="test",
            model=Game,
            source_filters=(),
            target_filters=(),
        )
        assert spec.label == "test"
        assert spec.model is Game
        assert spec.replace_scope is True


class TestRuntimeHydratorInit:
    def test_init(self):
        s1 = MagicMock(spec=Session)
        s2 = MagicMock(spec=Session)
        h = RuntimeHydrator(s1, s2)
        assert h.source_session is s1
        assert h.target_session is s2


class TestRuntimeHydratorHydrateYear:
    def test_hydrate_year_empty(self, _hydrator_engine_pair):
        hydrator, engine1, engine2, s1, s2 = _hydrator_engine_pair
        result = hydrator.hydrate_year(2025)
        assert isinstance(result, dict)

    def test_hydrate_year_with_game(self, _hydrator_engine_pair):
        from src.models.game import Game

        hydrator, engine1, engine2, s1, s2 = _hydrator_engine_pair

        s1.add(
            Game(
                game_id="20250601_01",
                game_date=date(2025, 6, 1),
                home_team="SSG",
                away_team="LG",
            ),
        )
        s1.commit()

        result = hydrator.hydrate_year(2025)
        assert result.get("game", 0) >= 1

        target_games = s2.query(Game).all()
        assert len(target_games) >= 1

    def test_hydrate_year_with_preserve_aliases(self, _hydrator_engine_pair):
        from src.models.game import Game, GameIdAlias

        hydrator, engine1, engine2, s1, s2 = _hydrator_engine_pair

        s1.add(
            Game(
                game_id="20250601_01",
                game_date=date(2025, 6, 1),
                home_team="SSG",
                away_team="LG",
            ),
        )
        s1.commit()
        s2.add(
            GameIdAlias(
                alias_game_id="old_20250601_01",
                canonical_game_id="20250601_01",
            ),
        )
        s2.commit()

        result = hydrator.hydrate_year(2025, preserve_aliases=True)
        assert result.get("game_id_aliases_preserved", 0) >= 1

    def test_hydrate_year_with_target_date(self, _hydrator_engine_pair):
        from src.models.game import Game

        hydrator, engine1, engine2, s1, s2 = _hydrator_engine_pair

        s1.add(
            Game(
                game_id="20250601_01",
                game_date=date(2025, 6, 1),
                home_team="SSG",
                away_team="LG",
            ),
        )
        s1.commit()

        result = hydrator.hydrate_year(2025, target_date=date(2025, 6, 15))
        assert "game" in result


class TestRuntimeHydratorPlayerRefs:
    def test_collect_player_refs(self):
        class FakeRow:
            player_id = 123
            player_name = "Kim"

        refs = RuntimeHydrator._collect_player_refs([FakeRow()])
        assert refs == {123: "Kim"}

    def test_collect_player_refs_none_id(self):
        class FakeRow:
            player_id = None
            player_name = "Kim"

        refs = RuntimeHydrator._collect_player_refs([FakeRow()])
        assert refs == {}

    def test_collect_player_refs_missing_name(self):
        class FakeRow:
            player_id = 456
            player_name = None

        refs = RuntimeHydrator._collect_player_refs([FakeRow()])
        assert refs == {456: "Unknown 456"}

    def test_resolve_player_refs_no_missing(self):
        s1 = MagicMock(spec=Session)
        s2 = MagicMock(spec=Session)
        s2.query.return_value.filter.return_value.all.return_value = [(123,)]
        hydrator = RuntimeHydrator(s1, s2)
        hydrator._resolve_player_refs({123: "Kim"})
        s2.query.assert_called()

    def test_resolve_player_refs_with_missing(self):
        from src.models.player import PlayerBasic

        engine1 = create_engine("sqlite:///:memory:", echo=False)
        engine2 = create_engine("sqlite:///:memory:", echo=False)
        Base = pytest.importorskip("src.models.base").Base
        Base.metadata.create_all(bind=engine1)
        Base.metadata.create_all(bind=engine2)
        s1 = sessionmaker(bind=engine1)()
        s2 = sessionmaker(bind=engine2)()

        s1.add(PlayerBasic(player_id=999, name="Kim", status="Active"))
        s1.commit()

        hydrator = RuntimeHydrator(s1, s2)
        hydrator._resolve_player_refs({999: "Kim"})

        existing = s2.query(PlayerBasic).filter_by(player_id=999).first()
        assert existing is not None
        s1.close()
        s2.close()
        engine1.dispose()
        engine2.dispose()
