"""One splitter, so a migration means the same thing to every runner.

There were three. The production failure behind this is recorded in
``060_parking_fee_kinds.sql``: its header comment reads "these kinds never had a
table of their own; they lived only in the raw snapshot text", and a splitter
that cut on every ``;`` tore the file inside that comment -- sending the driver a
comment-only chunk it rejects as an empty query, and welding the comment's tail
onto the next statement as code.

The PostgreSQL runner was fixed. ``src.db.migration_engine`` was not, and it is
the runner that reads the SQLite chain, so the same file would have failed there
with no error anyone would have read. These tests pin the scanner's rules where
the rules live, and pin that both runners reach it by identity -- a second
correct-by-accident copy is the failure mode being removed.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.db.migration_engine import _split_sql_statements
from src.db.sql_statements import UnterminatedBlockCommentError, split_sql_statements

MIGRATIONS_ROOT = Path(__file__).resolve().parents[2] / "migrations"

#: The runner that reads the PostgreSQL chain, and the engine that reads the
#: SQLite one. Both must be this function, not a lookalike.
from src.cli.sync.apply_postgres_migrations import split_sql_statements as postgres_splitter


class TestTheSemicolonIsOnlySyntaxWhereItIsSyntax:
    def test_a_header_comment_holding_a_semicolon_is_not_a_split(self) -> None:
        source = "-- these kinds never had a table of their own; they lived only\nCREATE TABLE t (id INT);\n"

        statements = split_sql_statements(source)

        assert len(statements) == 1
        assert "CREATE TABLE t (id INT)" in statements[0]

    def test_a_semicolon_inside_a_string_literal_does_not_split(self) -> None:
        source = "INSERT INTO t (name) VALUES ('a;b');\nSELECT 1;\n"

        statements = split_sql_statements(source)

        assert len(statements) == 2
        assert "'a;b'" in statements[0]

    def test_a_doubled_quote_keeps_the_literal_whole(self) -> None:
        r"""Behaviour, asserted without pretending the escape branch is load-bearing.

        ``''`` inside a literal is an escaped quote. The scanner handles it
        explicitly, and a mutation that drops the branch changes no output: both
        readings stop at the same closing quote, so a test written to "catch"
        that mutation would catch nothing. Exhaustively checked over every
        combination of ``' ; a \\n`` up to length five -- 1364 inputs, zero
        differences. The branch stays because it states the rule rather than
        relying on two quotes coinciding to cancel out, and this test pins the
        behaviour it produces instead.
        """
        source = "INSERT INTO t (name) VALUES ('it''s; fine');\nSELECT 1;\n"

        statements = split_sql_statements(source)

        assert len(statements) == 2
        assert statements[0] == "INSERT INTO t (name) VALUES ('it''s; fine')"
        assert statements[1] == "SELECT 1"

    def test_a_semicolon_inside_a_block_comment_does_not_split(self) -> None:
        """The defect ``migration_engine`` still had after the Postgres fix."""
        source = "/* first; second */\nCREATE TABLE t (id INT);\n"

        statements = split_sql_statements(source)

        assert len(statements) == 1
        assert "CREATE TABLE t (id INT)" in statements[0]

    def test_a_block_comment_inside_a_statement_survives_intact(self) -> None:
        source = "CREATE TABLE t (\n    /* why: two keys; see ADR 7 */\n    a INT,\n    b INT\n);\n"

        statements = split_sql_statements(source)

        assert len(statements) == 1
        assert "ADR 7" in statements[0]
        assert "a INT," in statements[0]


class TestACommentOnlyChunkIsNotAStatement:
    def test_a_leading_comment_is_dropped_rather_than_sent(self) -> None:
        statements = split_sql_statements("-- header; with a semicolon\nCREATE TABLE t (id INT);\n")

        assert len(statements) == 1

    def test_a_trailing_comment_without_a_terminator_is_dropped(self) -> None:
        statements = split_sql_statements("CREATE TABLE t (id INT);\n-- trailing; note\n")

        assert len(statements) == 1

    def test_a_file_of_only_comments_yields_nothing(self) -> None:
        """Otherwise it would apply as nothing and still record its version."""
        assert split_sql_statements("-- nothing; here\n-- at all\n") == []


class TestTheScannerDoesNotRewriteWhatItSplits:
    def test_a_comment_inside_a_statement_keeps_its_line(self) -> None:
        """The defect that deleted newlines.

        Filtering comment lines out and re-joining the rest would turn
        ``SELECT 1 -- note`` plus ``FROM t`` into ``SELECT 1 FROM t`` on one
        line. That is a different statement, and for a dialect-sensitive one it
        is a syntax error the operator sees with no idea where it came from.
        """
        source = "SELECT 1 -- the count; see below\nFROM t;\n"

        statements = split_sql_statements(source)

        assert len(statements) == 1
        assert "\n" in statements[0]
        assert "SELECT 1 -- the count; see below" in statements[0]

    def test_statements_keep_the_comment_that_explains_them(self) -> None:
        statements = split_sql_statements("-- why this exists\nCREATE TABLE t (id INT);\n")

        assert statements == ["-- why this exists\nCREATE TABLE t (id INT)"]


class TestUnterminatedInputIsNotSilentlyTruncated:
    def test_an_unterminated_string_runs_to_the_end_of_the_file(self) -> None:
        """Cutting short here would split the middle of a literal."""
        statements = split_sql_statements("INSERT INTO t VALUES ('oops);\nSELECT 1;\n")

        assert len(statements) == 1
        assert "'oops" in statements[0]

    def test_an_unterminated_block_comment_is_refused_rather_than_truncated(self) -> None:
        """A half-written comment must fail loudly, not apply as nothing.

        The scanner cannot tell how far an unclosed ``/*`` reaches, so everything
        after it is comment. Dropping that silently would leave the migration
        applying as nothing while still recording its version -- the exact
        half-apply this scanner exists to prevent, reached by a different road.
        """
        with pytest.raises(UnterminatedBlockCommentError):
            split_sql_statements("/* never closed\nCREATE TABLE t (id INT);\n")

    def test_a_closed_block_comment_is_ordinary_input(self) -> None:
        """The refusal above must not make block comments unusable."""
        statements = split_sql_statements("/* closed */\nCREATE TABLE t (id INT);\n")

        assert len(statements) == 1
        assert "CREATE TABLE t (id INT)" in statements[0]


class TestEveryRunnerUsesThisScanner:
    @pytest.mark.parametrize(
        "runner",
        [
            pytest.param(postgres_splitter, id="apply_postgres_migrations"),
            pytest.param(_split_sql_statements, id="migration_engine"),
        ],
    )
    def test_the_runner_is_the_scanner_not_a_copy_of_it(self, runner: object) -> None:
        """A second correct-by-accident copy is the failure mode being removed.

        Identity rather than behaviour: two implementations can agree on today's
        fixtures and diverge on the next migration, and nothing would say so.
        """
        assert runner is split_sql_statements

    def test_a_slash_on_its_own_line_does_not_disable_semicolon_splitting(self) -> None:
        """``migration_engine`` split on ``/`` first and only then on ``;``.

        A PostgreSQL migration that happened to carry a bare ``/`` line -- a
        stray character, or a path in a comment -- would silently turn off
        semicolon splitting for the whole file and hand the driver several
        statements as one.
        """
        source = "CREATE TABLE a (id INT);\n/\nCREATE TABLE b (id INT);\n"

        statements = split_sql_statements(source)

        assert len(statements) == 2
        assert statements == ["CREATE TABLE a (id INT)", "CREATE TABLE b (id INT)"]

    def test_a_slash_alone_on_a_line_is_not_left_in_the_statement(self) -> None:
        """The Oracle chain's PL/SQL terminator reads as a divide if it stays.

        A bare ``/`` welded to a statement makes it ``...)/``, which is not valid
        SQL. The old ``migration_engine`` splitter happened to remove this as a
        side effect of splitting on ``/``; keeping the removal explicit means it
        does not depend on that accident recurring.

        A plain statement is the fixture on purpose. This scanner still splits
        PL/SQL bodies on their semicolons -- that is what the Oracle chain's own
        runner is for, and routing it here would tear the blocks in half.
        """
        statements = split_sql_statements("CREATE TABLE t (id INT);\n/\n")

        assert statements == ["CREATE TABLE t (id INT)"]

    def test_a_slash_inside_sql_is_not_mistaken_for_a_terminator(self) -> None:
        """Only a line that is nothing but ``/`` is a terminator."""
        statements = split_sql_statements("SELECT a/b FROM t;\nSELECT * FROM '/usr/bin';\n")

        assert statements == ["SELECT a/b FROM t", "SELECT * FROM '/usr/bin'"]


class TestEveryShippedMigrationSplitsIntoExecutableStatements:
    @pytest.mark.parametrize("dialect", ["postgresql", "sqlite"])
    def test_no_migration_reaches_a_runner_as_nothing_or_as_comments(self, dialect: str) -> None:
        """The check that would have caught ``060`` before production.

        Both chains, through the scanner both chains now use. A migration that
        splits into nothing applies as nothing and still records its version --
        the same silent half-apply this scanner exists to prevent.
        """
        paths = sorted((MIGRATIONS_ROOT / dialect).glob("*.sql"))
        assert paths, f"no {dialect} migrations found"

        for path in paths:
            statements = split_sql_statements(path.read_text(encoding="utf-8"))
            assert statements, f"{dialect}/{path.name} split into nothing"
            for statement in statements:
                code = [
                    line
                    for line in statement.splitlines()
                    if line.strip() and not line.strip().startswith("--") and not line.strip().startswith("/*")
                ]
                assert code, f"{dialect}/{path.name} produced a comment-only statement"

    def test_the_migration_that_failed_in_production_is_still_shipped(self) -> None:
        """Keep the regression anchored to the real file, not a paraphrase of it.

        If the file is renamed the failure stops being reproducible, and the
        scanner's rules would quietly stop being about anything real.
        """
        assert (MIGRATIONS_ROOT / "postgresql" / "060_parking_fee_kinds.sql").is_file()

        source = (MIGRATIONS_ROOT / "postgresql" / "060_parking_fee_kinds.sql").read_text(encoding="utf-8")

        assert "their own; they lived" in source, "the semicolon in this header is the case under test"
        statements = split_sql_statements(source)

        # Two statements, and the header comment rides along with the first one
        # rather than becoming a statement of its own -- which is the whole
        # failure: a comment-only chunk reached psycopg2 as an empty query.
        assert len(statements) == 2
        # The header rides along with the CREATE TABLE rather than becoming a
        # statement of its own -- which is the whole failure: a comment-only
        # chunk reached psycopg2 as an empty query, and the comment's orphaned
        # tail was welded onto the CREATE as though it were code.
        assert statements[0].startswith("-- 060_parking_fee_kinds.sql")
        assert "CREATE TABLE IF NOT EXISTS parking_fee_kinds" in statements[0]
        assert statements[0].endswith("UNIQUE(parking_lot_id, fee_kind)\n)")
        assert statements[1].startswith("CREATE INDEX IF NOT EXISTS idx_parking_fee_kinds_lot")
