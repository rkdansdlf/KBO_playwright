from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from src.models.game import (
    Game,
    GameBattingStat,
    GameIdAlias,
    GameInningScore,
    GameLineup,
    GameMetadata,
    GamePitchingStat,
)
from src.models.player import PlayerBasic
from src.models.team import Team
from src.sync.runtime_hydrator import HydrationSpec, RuntimeHydrator


def _build_session_factory():
    engine = create_engine("sqlite:///:memory:")

    @event.listens_for(engine, "connect")
    def _sqlite_foreign_keys(dbapi_con, _):
        cursor = dbapi_con.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    for table in (
        Team.__table__,
        PlayerBasic.__table__,
        Game.__table__,
        GameIdAlias.__table__,
        GameMetadata.__table__,
        GameInningScore.__table__,
        GameLineup.__table__,
        GameBattingStat.__table__,
        GamePitchingStat.__table__,
    ):
        table.create(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def _build_session_factory_no_fk():
    engine = create_engine("sqlite:///:memory:")

    @event.listens_for(engine, "connect")
    def _sqlite_foreign_keys_off(dbapi_con, _):
        cursor = dbapi_con.cursor()
        cursor.execute("PRAGMA foreign_keys=OFF")
        cursor.close()

    for table in (
        Team.__table__,
        PlayerBasic.__table__,
        Game.__table__,
        GameIdAlias.__table__,
        GameMetadata.__table__,
        GameInningScore.__table__,
        GameLineup.__table__,
        GameBattingStat.__table__,
        GamePitchingStat.__table__,
    ):
        table.create(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def test_hydration_specs_returns_all_models():
    source_factory = _build_session_factory()
    target_factory = _build_session_factory()

    with source_factory() as source_session, target_factory() as target_session:
        specs = RuntimeHydrator(source_session, target_session)._hydration_specs(
            2025, target_date=None, preserve_aliases=False
        )

    labels = [s.label for s in specs]
    assert "player_basic" in labels
    assert "game" in labels
    assert "game_id_aliases" in labels
    assert "game_validation_metrics" in labels
    assert "player_game_batting" in labels
    assert "player_game_pitching" in labels


def test_hydration_specs_with_target_date_adjusts_roster_window():
    source_factory = _build_session_factory()
    target_factory = _build_session_factory()

    with source_factory() as source_session, target_factory() as target_session:
        specs = RuntimeHydrator(source_session, target_session)._hydration_specs(
            2025, target_date=date(2025, 6, 15), preserve_aliases=False
        )

    roster_spec = next(s for s in specs if s.label == "team_daily_roster")
    assert roster_spec.source_filters


def test_hydration_specs_preserve_aliases_skips_alias_spec():
    source_factory = _build_session_factory()
    target_factory = _build_session_factory()

    with source_factory() as source_session, target_factory() as target_session:
        specs = RuntimeHydrator(source_session, target_session)._hydration_specs(
            2025, target_date=None, preserve_aliases=True
        )

    labels = [s.label for s in specs]
    assert "game_id_aliases" not in labels


def test_hydrate_year_rolls_back_on_error():
    source_factory = _build_session_factory()
    target_factory = _build_session_factory()

    with target_factory() as session:
        session.add(Team(team_id="LG", team_name="LG", team_short_name="LG", city="서울"))
        session.commit()

    with source_factory() as source_session, target_factory() as target_session:
        hydrator = RuntimeHydrator(source_session, target_session)
        hydrator._hydration_specs = lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("boom"))
        try:
            hydrator.hydrate_year(2025)
        except RuntimeError:
            pass


def test_hydrate_spec_empty_source_returns_zero():
    source_factory = _build_session_factory()
    target_factory = _build_session_factory()

    with source_factory() as source_session, target_factory() as target_session:
        hydrator = RuntimeHydrator(source_session, target_session)
        spec = HydrationSpec(
            label="empty",
            model=Game,
            source_filters=[],
            target_filters=[],
        )
        count, refs = hydrator._hydrate_spec(spec)

    assert count == 0
    assert refs == {}


def test_hydrate_spec_filters_child_rows_without_parent_game():
    source_factory = _build_session_factory_no_fk()
    target_factory = _build_session_factory_no_fk()

    with source_factory() as source_session:
        source_session.add(
            GameBattingStat(
                game_id="20250401LGSS0",
                team_side="away",
                team_code="LG",
                player_id=1001,
                player_name="홍길동",
                batting_order=1,
                appearance_seq=1,
                standard_position="CF",
                hits=2,
                at_bats=4,
            )
        )
        source_session.commit()

    with source_factory() as source_session, target_factory() as target_session:
        hydrator = RuntimeHydrator(source_session, target_session)
        spec = HydrationSpec(
            label="game_batting_stats",
            model=GameBattingStat,
            source_filters=[],
            target_filters=[],
        )
        count, refs = hydrator._hydrate_spec(spec)

    assert count == 0


def test_hydrate_spec_with_parent_game_includes_child_rows():
    source_factory = _build_session_factory_no_fk()
    target_factory = _build_session_factory_no_fk()

    with source_factory() as source_session:
        source_session.add(
            Game(
                game_id="20250401LGSS0",
                game_date=date(2025, 4, 1),
                away_team="LG",
                home_team="SS",
                game_status="COMPLETED",
                season_id=2025,
            )
        )
        source_session.add(
            GameBattingStat(
                game_id="20250401LGSS0",
                team_side="away",
                team_code="LG",
                player_id=1001,
                player_name="홍길동",
                batting_order=1,
                appearance_seq=1,
                standard_position="CF",
                hits=2,
                at_bats=4,
            )
        )
        source_session.commit()

    with target_factory() as session:
        session.add(
            Game(
                game_id="20250401LGSS0",
                game_date=date(2025, 4, 1),
                away_team="LG",
                home_team="SS",
                game_status="SCHEDULED",
                season_id=2025,
            )
        )
        session.commit()

    with source_factory() as source_session, target_factory() as target_session:
        hydrator = RuntimeHydrator(source_session, target_session)
        spec = HydrationSpec(
            label="game_batting_stats",
            model=GameBattingStat,
            source_filters=[],
            target_filters=[],
        )
        count, refs = hydrator._hydrate_spec(spec)

    assert count == 1
    assert 1001 in refs


def test_restore_aliases_returns_zero_when_empty():
    source_factory = _build_session_factory()
    target_factory = _build_session_factory()

    with source_factory() as source_session, target_factory() as target_session:
        hydrator = RuntimeHydrator(source_session, target_session)
        result = hydrator._restore_aliases([])

    assert result == 0


def test_restore_aliases_filters_missing_canonical_games():
    source_factory = _build_session_factory()
    target_factory = _build_session_factory()

    with source_factory() as source_session, target_factory() as target_session:
        hydrator = RuntimeHydrator(source_session, target_session)
        result = hydrator._restore_aliases(
            [
                {
                    "alias_game_id": "20250401LGSSG0",
                    "canonical_game_id": "20250401LGSS0",
                    "source": "test",
                    "reason": "preserve",
                }
            ]
        )

    assert result == 0


def test_restore_aliases_inserts_matching_canonical_games():
    source_factory = _build_session_factory()
    target_factory = _build_session_factory()

    with target_factory() as session:
        session.add(
            Game(
                game_id="20250401LGSS0",
                game_date=date(2025, 4, 1),
                away_team="LG",
                home_team="SS",
                game_status="COMPLETED",
                season_id=2025,
            )
        )
        session.add(
            GameIdAlias(
                alias_game_id="20250401LGSSG0",
                canonical_game_id="20250401LGSS0",
                source="test",
                reason="preserve",
            )
        )
        session.commit()

    with source_factory() as source_session, target_factory() as target_session:
        hydrator = RuntimeHydrator(source_session, target_session)
        result = hydrator._restore_aliases(
            [
                {
                    "alias_game_id": "20250401LGSSG0",
                    "canonical_game_id": "20250401LGSS0",
                    "source": "test",
                    "reason": "preserve",
                }
            ]
        )

    assert result == 1


def test_snapshot_aliases_excludes_timestamps():
    source_factory = _build_session_factory()
    target_factory = _build_session_factory()

    with target_factory() as session:
        session.add(
            Game(
                game_id="20250401LGSS0",
                game_date=date(2025, 4, 1),
                away_team="LG",
                home_team="SS",
                game_status="COMPLETED",
                season_id=2025,
            )
        )
        session.flush()
        session.add(
            GameIdAlias(
                alias_game_id="20250401LGSSG0",
                canonical_game_id="20250401LGSS0",
                source="test",
                reason="preserve",
            )
        )
        session.commit()

    with source_factory() as source_session, target_factory() as target_session:
        hydrator = RuntimeHydrator(source_session, target_session)
        aliases = hydrator._snapshot_aliases(2025)

    assert len(aliases) == 1
    assert "created_at" not in aliases[0]
    assert "updated_at" not in aliases[0]
    assert aliases[0]["alias_game_id"] == "20250401LGSSG0"


def test_delete_alias_scope_removes_matching_rows():
    source_factory = _build_session_factory()
    target_factory = _build_session_factory()

    with target_factory() as session:
        session.add(
            Game(
                game_id="20250401LGSS0",
                game_date=date(2025, 4, 1),
                away_team="LG",
                home_team="SS",
                game_status="COMPLETED",
                season_id=2025,
            )
        )
        session.flush()
        session.add(
            GameIdAlias(
                alias_game_id="20250401LGSSG0",
                canonical_game_id="20250401LGSS0",
                source="test",
                reason="preserve",
            )
        )
        session.commit()

    with source_factory() as source_session, target_factory() as target_session:
        hydrator = RuntimeHydrator(source_session, target_session)
        hydrator._delete_alias_scope(2025)
        target_session.commit()

    with target_factory() as session:
        assert session.query(GameIdAlias).count() == 0


def test_collect_player_refs_skips_none_and_invalid():
    source_factory = _build_session_factory()
    target_factory = _build_session_factory()

    class FakeRow:
        def __init__(self, player_id, player_name=None):
            self.player_id = player_id
            self.player_name = player_name

    rows = [
        FakeRow(None),
        FakeRow("invalid"),
        FakeRow(1001, "홍길동"),
        FakeRow(1002),
    ]

    with source_factory() as source_session, target_factory() as target_session:
        hydrator = RuntimeHydrator(source_session, target_session)
        refs = hydrator._collect_player_refs(rows)

    assert refs == {1001: "홍길동", 1002: "Unknown 1002"}


def test_resolve_player_refs_creates_missing_stubs():
    source_factory = _build_session_factory()
    target_factory = _build_session_factory()

    with source_factory() as session:
        session.add(PlayerBasic(player_id=1001, name="홍길동", team="LG"))
        session.commit()

    with source_factory() as source_session, target_factory() as target_session:
        hydrator = RuntimeHydrator(source_session, target_session)
        hydrator._resolve_player_refs({1001: "홍길동", 9999: "Unknown 9999"})
        target_session.commit()

    with target_factory() as session:
        assert session.query(PlayerBasic).filter(PlayerBasic.player_id == 9999).count() == 1


def test_resolve_player_refs_skips_when_all_exist():
    source_factory = _build_session_factory()
    target_factory = _build_session_factory()

    with target_factory() as session:
        session.add(PlayerBasic(player_id=1001, name="홍길동", team="LG"))
        session.commit()

    with source_factory() as source_session, target_factory() as target_session:
        hydrator = RuntimeHydrator(source_session, target_session)
        hydrator._resolve_player_refs({1001: "홍길동"})

    with target_factory() as session:
        assert session.query(PlayerBasic).count() == 1


def test_delete_existing_game_id_rows_skips_game_model():
    source_factory = _build_session_factory()
    target_factory = _build_session_factory()

    with source_factory() as source_session, target_factory() as target_session:
        hydrator = RuntimeHydrator(source_session, target_session)
        spec = HydrationSpec(label="game", model=Game, source_filters=[], target_filters=[])
        hydrator._delete_existing_game_id_rows(spec, [])


def test_insert_mappings_uses_upsert_for_sqlite():
    source_factory = _build_session_factory()
    target_factory = _build_session_factory()

    with target_factory() as session:
        session.add(PlayerBasic(player_id=1001, name="홍길동", team="LG"))
        session.add(
            Game(
                game_id="20250401LGSS0",
                game_date=date(2025, 4, 1),
                away_team="LG",
                home_team="SS",
                game_status="COMPLETED",
                season_id=2025,
            )
        )
        session.commit()

    with source_factory() as source_session, target_factory() as target_session:
        hydrator = RuntimeHydrator(source_session, target_session)
        spec = HydrationSpec(
            label="game_batting_stats",
            model=GameBattingStat,
            source_filters=[],
            target_filters=[],
        )
        hydrator._insert_mappings(
            spec,
            [
                {
                    "game_id": "20250401LGSS0",
                    "team_side": "away",
                    "team_code": "LG",
                    "player_id": 1001,
                    "player_name": "홍길동",
                    "batting_order": 1,
                    "appearance_seq": 1,
                    "standard_position": "CF",
                    "is_starter": False,
                    "at_bats": 0,
                    "hits": 0,
                    "doubles": 0,
                    "triples": 0,
                    "home_runs": 0,
                    "rbi": 0,
                    "runs": 0,
                    "walks": 0,
                    "intentional_walks": 0,
                    "hbp": 0,
                    "strikeouts": 0,
                    "stolen_bases": 0,
                    "caught_stealing": 0,
                    "sacrifice_hits": 0,
                    "sacrifice_flies": 0,
                    "gdp": 0,
                    "plate_appearances": 0,
                }
            ],
            [
                "game_id",
                "team_side",
                "team_code",
                "player_id",
                "player_name",
                "batting_order",
                "is_starter",
                "appearance_seq",
                "standard_position",
                "plate_appearances",
                "at_bats",
                "runs",
                "hits",
                "doubles",
                "triples",
                "home_runs",
                "rbi",
                "walks",
                "intentional_walks",
                "hbp",
                "strikeouts",
                "stolen_bases",
                "caught_stealing",
                "sacrifice_hits",
                "sacrifice_flies",
                "gdp",
            ],
        )
        target_session.commit()

    with target_factory() as session:
        assert session.query(GameBattingStat).count() == 1


def test_hydrate_spec_non_replace_scope_uses_upsert():
    source_factory = _build_session_factory()
    target_factory = _build_session_factory()

    with source_factory() as source_session:
        source_session.add(PlayerBasic(player_id=1001, name="홍길동", team="LG"))
        source_session.commit()

    with source_factory() as source_session, target_factory() as target_session:
        hydrator = RuntimeHydrator(source_session, target_session)
        spec = HydrationSpec(
            label="player_basic",
            model=PlayerBasic,
            source_filters=[],
            target_filters=[],
            replace_scope=False,
            exclude_columns=("created_at", "updated_at"),
        )
        count, refs = hydrator._hydrate_spec(spec)

    assert count == 1
    assert refs == {}
