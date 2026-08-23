"""Schema and ORM Model Invariant test suite.

Ensures that:
1. All SQLAlchemy models in src/models/ inherit from Base and are exported.
2. Every table has primary keys and valid column definitions.
3. Foreign keys and indexes reference existing tables and columns.
4. Mapper configurations and in-memory DDL creation succeed without errors.
5. Base helper methods (to_dict, __repr__) function consistently.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import configure_mappers

import src.models
from src.models.award import Award
from src.models.base import Base
from src.models.player import PlayerBasic


class TestSchemaInvariants:
    """Validate ORM schema invariants across all models."""

    def test_all_model_modules_importable(self) -> None:
        """Verify that every python module under src/models/ can be imported cleanly."""
        models_dir = Path(__file__).resolve().parents[2] / "src" / "models"
        py_files = sorted(models_dir.glob("*.py"))
        assert len(py_files) >= 30, f"Expected at least 30 model files, found {len(py_files)}"

        for py_file in py_files:
            mod_name = f"src.models.{py_file.stem}"
            mod = importlib.import_module(mod_name)
            assert mod is not None

    def test_metadata_tables_populated(self) -> None:
        """Verify that Base.metadata has registered all domain tables."""
        tables = Base.metadata.tables
        assert len(tables) >= 40, f"Expected at least 40 registered tables, found {len(tables)}"

        essential_tables = {
            "player_basic",
            "game",
            "game_metadata",
            "game_batting_stats",
            "game_pitching_stats",
            "game_play_by_play",
            "team_season_batting",
            "team_season_pitching",
            "team_standings_daily",
            "ticket_prices",
            "stadium_info",
            "awards",
            "data_sources",
            "rag_chunks",
        }
        missing = essential_tables - set(tables.keys())
        assert not missing, f"Missing essential tables in metadata: {missing}"

    def test_every_table_has_primary_key(self) -> None:
        """Verify that every registered table has at least one primary key column."""
        for table_name, table in Base.metadata.tables.items():
            pk_cols = list(table.primary_key.columns)
            assert len(pk_cols) > 0, f"Table '{table_name}' has no primary key defined"

    def test_foreign_keys_reference_valid_targets(self) -> None:
        """Verify that foreign keys reference tables and columns present in Base.metadata."""
        tables = Base.metadata.tables
        for table_name, table in tables.items():
            for fk in table.foreign_keys:
                target_table = fk.column.table.name
                target_col = fk.column.name
                assert target_table in tables, f"Table '{table_name}' FK references unknown table '{target_table}'"
                assert target_col in tables[target_table].columns, (
                    f"Table '{table_name}' FK references unknown column '{target_table}.{target_col}'"
                )

    def test_indexes_reference_valid_columns(self) -> None:
        """Verify that indexes reference valid columns on their respective tables."""
        for table_name, table in Base.metadata.tables.items():
            table_col_names = {c.name for c in table.columns}
            for idx in table.indexes:
                idx_col_names = {c.name for c in idx.columns}
                invalid_cols = idx_col_names - table_col_names
                assert not invalid_cols, (
                    f"Index '{idx.name}' on table '{table_name}' references invalid columns: {invalid_cols}"
                )

    def test_configure_mappers_succeeds(self) -> None:
        """Verify that SQLAlchemy mapper configuration succeeds without relationship errors."""
        configure_mappers()

    def test_sqlite_in_memory_ddl_creation(self) -> None:
        """Verify that all model tables can be created in SQLite without DDL syntax errors."""
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)

        inspector = inspect(engine)
        created_tables = set(inspector.get_table_names())
        assert len(created_tables) >= len(Base.metadata.tables)


class TestBaseModelHelpers:
    """Test Base model helper methods: to_dict() and __repr__()."""

    def test_player_basic_to_dict_and_repr(self) -> None:
        player = PlayerBasic(
            player_id=65432,
            name="테스트선수",
            uniform_no="7",
            team="LG",
            position="외야수",
            height_cm=180,
            weight_kg=80,
        )
        data = player.to_dict()
        assert isinstance(data, dict)
        assert data["player_id"] == 65432
        assert data["name"] == "테스트선수"
        assert data["team"] == "LG"

        data_excluded = player.to_dict(exclude={"height_cm", "weight_kg"})
        assert "height_cm" not in data_excluded
        assert "weight_kg" not in data_excluded
        assert data_excluded["name"] == "테스트선수"

        repr_str = repr(player)
        assert "PlayerBasic" in repr_str
        assert "player_id=65432" in repr_str

    def test_award_to_dict_and_repr(self) -> None:
        award = Award(
            year=2025,
            award_type="MVP",
            player_name="김도영",
            team_name="KIA",
        )
        data = award.to_dict()
        assert data["year"] == 2025
        assert data["award_type"] == "MVP"
        assert data["player_name"] == "김도영"

        repr_str = repr(award)
        assert "Award" in repr_str
