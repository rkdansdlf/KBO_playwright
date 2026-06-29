from sqlalchemy import create_engine, text

from src.cli.verify_sync_consistency import check_deep_ids, check_table_counts, get_row_count


def test_get_row_count():
    engine = create_engine("sqlite:///:memory:")
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)"))
        conn.execute(text("INSERT INTO test_table (name) VALUES ('a'), ('b'), ('c')"))

        count = get_row_count(conn, "test_table")
        assert count == 3


def test_check_table_counts_and_deep_ids(monkeypatch):
    # Create mock local SQLite and mock remote OCI engines
    sqlite_engine = create_engine("sqlite:///:memory:")
    oci_engine = create_engine("sqlite:///:memory:")

    with sqlite_engine.connect() as sqlite_conn, oci_engine.connect() as oci_conn:
        # Create identical tables
        for conn in (sqlite_conn, oci_conn):
            conn.execute(text("CREATE TABLE rag_chunks (id INTEGER PRIMARY KEY, content TEXT)"))
            conn.execute(text("CREATE TABLE stadium_foods (id INTEGER PRIMARY KEY, restaurant_name TEXT)"))

        # Populate sqlite with 5 rows
        for i in range(1, 6):
            sqlite_conn.execute(text(f"INSERT INTO rag_chunks (id, content) VALUES ({i}, 'chunk {i}')"))
        # Populate OCI with only 3 rows (simulating mismatch)
        for i in range(1, 4):
            oci_conn.execute(text(f"INSERT INTO rag_chunks (id, content) VALUES ({i}, 'chunk {i}')"))

        # Stadium foods has matching counts
        sqlite_conn.execute(text("INSERT INTO stadium_foods (id, restaurant_name) VALUES (1, 'rest A')"))
        oci_conn.execute(text("INSERT INTO stadium_foods (id, restaurant_name) VALUES (1, 'rest A')"))

        # We monkeypatch TABLES_TO_VERIFY to only check our test tables
        import src.cli.verify_sync_consistency

        monkeypatch.setattr(
            src.cli.verify_sync_consistency,
            "TABLES_TO_VERIFY",
            [("rag_chunks", ["id"]), ("stadium_foods", ["id"])],
        )

        results = check_table_counts(sqlite_conn, oci_conn)

        # Verify count checks
        rag_res = next(r for r in results if r["table_name"] == "rag_chunks")
        assert rag_res["sqlite_count"] == 5
        assert rag_res["oci_count"] == 3
        assert rag_res["delta"] == 2
        assert rag_res["status"] == "MISMATCH"

        food_res = next(r for r in results if r["table_name"] == "stadium_foods")
        assert food_res["sqlite_count"] == 1
        assert food_res["oci_count"] == 1
        assert food_res["delta"] == 0
        assert food_res["status"] == "OK"

        # Verify deep ID checks
        match_rate, missing_keys = check_deep_ids(sqlite_conn, oci_conn, "rag_chunks", ["id"])
        assert match_rate == 60.0  # 3 matched out of 5 sqlite rows (1, 2, 3 in both, 4, 5 missing in OCI)
        assert set(missing_keys) == {"4", "5"}
