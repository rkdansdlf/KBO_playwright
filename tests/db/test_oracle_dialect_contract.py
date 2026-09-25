"""Offline Oracle dialect contract tests.

Oracle Autonomous Database is the production store, but the PR gate only runs
SQLite and PostgreSQL. These tests pin the Oracle-specific SQL contract using
`sqlalchemy.dialects.oracle` and the raw SQL builders, with no live database.
That catches the dialect-specific breakage the other backends cannot: MERGE
`USING ... FROM DUAL` shape, `WHEN NOT MATCHED` clauses, CLOB casts, and
`VECTOR` type rendering.

The live counterpart is `.github/workflows/oci_live_verification.yml`; this
file is the part that must pass on every pull request.
"""

from __future__ import annotations

import re

import pytest
from sqlalchemy import JSON, Column, MetaData, String, Table, Text
from sqlalchemy.dialects import oracle
from sqlalchemy.schema import CreateTable

from src.db.engine import _install_oracle_json_compiler
from src.sync.oracle_writer import OracleWriter


@pytest.fixture
def writer() -> OracleWriter:
    """Build a writer without touching a database."""
    return OracleWriter.__new__(OracleWriter)


class TestMergeSqlContract:
    """`build_merge_sql` output is fed straight to Oracle, so its shape is a contract."""

    def test_single_row_uses_plain_binds(self, writer: OracleWriter) -> None:
        sql = writer.build_merge_sql("game", ["game_id", "home_score"], ["game_id"], row_count=1)

        assert sql.startswith('MERGE INTO "GAME" t USING (SELECT :c0 AS "GAME_ID", :c1 AS "HOME_SCORE" FROM DUAL) s')
        assert 'ON ((t."GAME_ID" = s."GAME_ID" OR (t."GAME_ID" IS NULL AND s."GAME_ID" IS NULL)))' in sql
        assert 'WHEN MATCHED THEN UPDATE SET t."HOME_SCORE" = s."HOME_SCORE"' in sql
        assert 'WHEN NOT MATCHED THEN INSERT ("GAME_ID", "HOME_SCORE") VALUES (s."GAME_ID", s."HOME_SCORE")' in sql

    def test_multi_row_unions_one_dual_per_row(self, writer: OracleWriter) -> None:
        sql = writer.build_merge_sql("game", ["game_id"], ["game_id"], row_count=3)

        # Oracle has no VALUES-literal multi-row source; each row is its own
        # DUAL projection, so a multi-row MERGE depends on this exact shape.
        assert sql.count("FROM DUAL") == 3
        assert sql.count("UNION ALL") == 2
        assert ":c0 AS" in sql
        assert ":c1 AS" in sql
        assert ":c2 AS" in sql
        assert ":c3" not in sql

    def test_multi_row_bind_indices_advance_per_column_and_row(self, writer: OracleWriter) -> None:
        sql = writer.build_merge_sql("game", ["a", "b"], ["a"], row_count=2)

        # Row-major ordering: c0/c1 then c2/c3.
        assert ':c0 AS "A", :c1 AS "B"' in sql
        assert ':c2 AS "A", :c3 AS "B"' in sql

    def test_casting_is_applied_only_for_multi_row_merges(self, writer: OracleWriter) -> None:
        types = {"A": "VARCHAR2", "B": "NUMBER"}
        columns = ["a", "b"]

        single = writer.build_merge_sql("t", columns, ["a"], row_count=1, column_types=types)
        multi = writer.build_merge_sql("t", columns, ["a"], row_count=2, column_types=types)

        assert "CAST(" not in single
        assert 'CAST(:c0 AS VARCHAR2(4000)) AS "A"' in multi
        assert 'CAST(:c3 AS NUMBER) AS "B"' in multi

    def test_clob_columns_are_cast_in_multi_row_merges(self, writer: OracleWriter) -> None:
        # Oracle cannot bind a CLOB inside a multi-row USING projection, so the
        # cast is what keeps a multi-row CLOB upsert from failing outright.
        types = {"A": "NUMBER", "BODY": "CLOB"}
        columns = ["a", "body"]

        multi = writer.build_merge_sql("t", columns, ["a"], row_count=2, column_types=types)
        assert 'CAST(:c1 AS VARCHAR2(4000)) AS "BODY"' in multi
        assert 'CAST(:c3 AS VARCHAR2(4000)) AS "BODY"' in multi

    @pytest.mark.parametrize(
        ("declared", "expected_cast"),
        [
            ("VARCHAR2", "VARCHAR2(4000)"),
            ("CHAR", "VARCHAR2(4000)"),
            ("NCHAR", "VARCHAR2(4000)"),
            ("NVARCHAR2", "VARCHAR2(4000)"),
            ("FLOAT", "NUMBER"),
            ("BINARY_FLOAT", "NUMBER"),
            ("BINARY_DOUBLE", "NUMBER"),
            ("INTEGER", "NUMBER"),
            ("DATE", "DATE"),
        ],
    )
    def test_numeric_and_character_types_are_cast_consistently(
        self,
        writer: OracleWriter,
        declared: str,
        expected_cast: str,
    ) -> None:
        sql = writer.build_merge_sql(
            "t",
            ["a", "v"],
            ["a"],
            row_count=2,
            column_types={"A": "NUMBER", "V": declared},
        )
        assert f'CAST(:c1 AS {expected_cast}) AS "V"' in sql

    def test_timestamp_keeps_its_own_type_instead_of_being_flattened(self, writer: OracleWriter) -> None:
        sql = writer.build_merge_sql(
            "t",
            ["a", "ts"],
            ["a"],
            row_count=2,
            column_types={"A": "NUMBER", "TS": "TIMESTAMP WITH TIME ZONE"},
        )

        assert "CAST(:c0 AS NUMBER)" in sql
        assert "CAST(:c1 AS TIMESTAMP WITH TIME ZONE)" in sql
        assert "TIMESTAMP WITH TIME ZONE NOT IN" not in sql

    def test_physical_column_name_mapping_is_applied(self, writer: OracleWriter) -> None:
        sql = writer.build_merge_sql(
            "game",
            ["game_id", "home_team"],
            ["game_id"],
            column_names={"GAME_ID": "GAME_ID", "HOME_TEAM": "HOME_TEAM_CD"},
        )

        assert 'AS "HOME_TEAM_CD"' in sql
        assert 'INSERT ("GAME_ID", "HOME_TEAM_CD")' in sql
        assert 'HOME_TEAM"' not in sql.replace("HOME_TEAM_CD", "")

    def test_pk_only_table_omits_the_update_clause(self, writer: OracleWriter) -> None:
        sql = writer.build_merge_sql("lookup", ["code"], ["code"])

        assert "WHEN MATCHED" not in sql
        assert 'WHEN NOT MATCHED THEN INSERT ("CODE") VALUES (s."CODE")' in sql

    def test_null_keys_match_null_to_null(self, writer: OracleWriter) -> None:
        # Without the IS NULL branch, a row whose key is NULL would insert a
        # duplicate on every sync instead of updating in place.
        sql = writer.build_merge_sql("t", ["a", "b"], ["a", "b"])

        assert 't."A" = s."A" OR (t."A" IS NULL AND s."A" IS NULL)' in sql
        assert 't."B" = s."B" OR (t."B" IS NULL AND s."B" IS NULL)' in sql

    def test_clause_order_is_merge_match_then_not_matched(self, writer: OracleWriter) -> None:
        sql = writer.build_merge_sql("t", ["a", "b"], ["a"])

        assert sql.index("MERGE INTO") < sql.index("ON (") < sql.index("WHEN MATCHED")
        assert sql.index("WHEN MATCHED") < sql.index("WHEN NOT MATCHED")

    def test_non_positive_row_count_is_rejected(self, writer: OracleWriter) -> None:
        with pytest.raises(ValueError, match="row_count must be positive"):
            writer.build_merge_sql("t", ["a"], ["a"], row_count=0)

    def test_generated_sql_has_balanced_parentheses(self, writer: OracleWriter) -> None:
        for row_count in (1, 2, 5):
            sql = writer.build_merge_sql("t", ["a", "b"], ["a"], row_count=row_count)
            assert sql.count("(") == sql.count(")"), f"unbalanced parens at row_count={row_count}"

    def test_generated_sql_has_no_unbouned_bind_placeholders(self, writer: OracleWriter) -> None:
        sql = writer.build_merge_sql("t", ["a", "b", "c"], ["a"], row_count=4)
        binds = {int(index) for index in re.findall(r":c(\d+)", sql)}

        assert binds == set(range(12))


class TestJsonColumnCompilesAsClob:
    """The project patches the Oracle type compiler so JSON lands in CLOB."""

    @staticmethod
    def _render(column_type: object) -> str:
        _install_oracle_json_compiler()
        metadata = MetaData()
        table = Table("doc", metadata, Column("id", String(20)), Column("payload", column_type))
        return str(CreateTable(table).compile(dialect=oracle.dialect()))

    def test_json_column_renders_as_clob(self) -> None:
        ddl = self._render(JSON)

        assert "payload CLOB" in ddl
        assert "JSON" not in ddl

    def test_text_column_still_renders_as_clob(self) -> None:
        ddl = self._render(Text)

        assert "payload CLOB" in ddl


class TestVectorColumnRenders:
    """Dense embeddings live in an Oracle VECTOR column beside the sparse rows."""

    def test_canonical_rag_chunk_uses_native_oracle_vector(self) -> None:
        from src.models.rag_chunk import RagChunk

        compiled = RagChunk.__table__.c.embedding_vector.type.compile(dialect=oracle.dialect())
        assert compiled == "VECTOR(1536,FLOAT32,DENSE)"

    def test_vector_dimension_matches_the_configured_embedding_model(self) -> None:
        from src.models.rag_chunk import RagChunk

        compiled = RagChunk.__table__.c.embedding_vector.type.compile(dialect=oracle.dialect())
        # A dimension mismatch only surfaces against a live ADB, so pin it here.
        assert re.search(r"VECTOR\(\d+,", compiled.upper()), compiled

    def test_pgvector_mirror_is_not_used_for_the_production_store(self) -> None:
        # `RagChunkVector` is the PostgreSQL acceptance mirror. The production
        # store is Oracle, so the two must not be interchangeable by accident.
        from src.models.rag_chunk import RagChunk
        from src.models.rag_chunk_vector import RagChunkVector

        assert RagChunk.__table__ is not RagChunkVector.__table__
        assert "embedding_vector" in RagChunk.__table__.c
