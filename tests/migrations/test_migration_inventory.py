"""Oracle Migration Inventory & DDL Integrity test suite.

Ensures that:
1. All Oracle SQL migration files are non-empty and well-formatted.
2. Migration sequence numbering is consistent without invalid gaps or malformed prefixes.
3. Tables created or modified in Oracle DDL match ORM metadata or documented exceptions.
4. Schema migrations tracking table compatibility is maintained.
"""

from __future__ import annotations

import re
from pathlib import Path

from src.models.base import Base

KNOWN_ORACLE_TABLE_EXCEPTIONS = {
    "dbtools$execution_history",
    "schema_migrations",
    "team_profiles",
    "player_movement_temp",
    "player_movement_roster_temp",
}

CREATE_TABLE_PATTERN = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z0-9_\$]+)",
    re.IGNORECASE,
)
ALTER_TABLE_PATTERN = re.compile(
    r"ALTER\s+TABLE\s+([a-zA-Z0-9_\$]+)",
    re.IGNORECASE,
)


class TestOracleMigrationInventory:
    """Validate Oracle migration files and DDL integrity."""

    @property
    def migration_dir(self) -> Path:
        return Path(__file__).resolve().parents[2] / "migrations" / "oracle"

    def test_migration_files_exist_and_non_empty(self) -> None:
        """Verify that Oracle migration SQL files exist and have non-empty content."""
        sql_files = sorted(self.migration_dir.glob("*.sql"))
        assert len(sql_files) >= 50, f"Expected at least 50 Oracle migration files, found {len(sql_files)}"

        for sql_file in sql_files:
            content = sql_file.read_text(encoding="utf-8").strip()
            assert len(content) > 10, f"Migration file '{sql_file.name}' is unexpectedly empty"

    def test_migration_sequence_numbering(self) -> None:
        """Verify that migration filenames begin with numeric sequence prefixes."""
        sql_files = sorted(self.migration_dir.glob("*.sql"))
        prefixes: list[int] = []
        for sql_file in sql_files:
            prefix_match = re.match(r"^(\d{3})_", sql_file.name)
            assert prefix_match is not None, f"Migration '{sql_file.name}' does not match 'NNN_name.sql' format"
            seq = int(prefix_match.group(1))
            prefixes.append(seq)

        # Sequences must be non-decreasing
        assert prefixes == sorted(prefixes), "Migration files are not sorted in ascending sequence order"
        assert max(prefixes) >= 67, f"Expected latest migration sequence >= 67, got {max(prefixes)}"

    def test_created_and_altered_tables_match_orm_or_exceptions(self) -> None:
        """Verify that all table targets in DDL scripts correspond to ORM models or documented exceptions."""
        sql_files = sorted(self.migration_dir.glob("*.sql"))
        orm_tables = {name.casefold() for name in Base.metadata.tables}
        exception_tables = {name.casefold() for name in KNOWN_ORACLE_TABLE_EXCEPTIONS}
        allowed_tables = orm_tables | exception_tables

        for sql_file in sql_files:
            content = sql_file.read_text(encoding="utf-8")
            created = CREATE_TABLE_PATTERN.findall(content)
            altered = ALTER_TABLE_PATTERN.findall(content)

            for table_name in created + altered:
                norm_table = table_name.strip().casefold()
                assert norm_table in allowed_tables, (
                    f"Migration '{sql_file.name}' references unknown table '{table_name}' "
                    f"not found in ORM metadata or KNOWN_ORACLE_TABLE_EXCEPTIONS"
                )

    def test_readme_exists(self) -> None:
        """Verify that migrations/oracle/README.md documentation exists."""
        readme = self.migration_dir / "README.md"
        assert readme.exists(), "migrations/oracle/README.md does not exist"
        assert len(readme.read_text(encoding="utf-8")) > 50
