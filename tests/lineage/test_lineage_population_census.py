"""Tests for Multi-Table Lineage Census, Transitive Provenance, and DAG Integrity."""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import create_engine, text

from src.lineage.engine import LineageEngine
from src.lineage.models import (
    LineageEdge,
    LineageEdgeType,
    LineageGraph,
    LineageNode,
    LineageNodeType,
    OriginType,
)
from src.models.base import Base


def test_lineage_graph_cycle_detection() -> None:
    """Test that LineageGraph correctly detects acyclicity and cyclical paths."""
    # Acyclic graph
    dag = LineageGraph(root_node_id="A")
    dag.add_node(LineageNode("A", LineageNodeType.SOURCE_SNAPSHOT, "A", "source", "1"))
    dag.add_node(LineageNode("B", LineageNodeType.CRAWL_RUN, "B", "run", "1"))
    dag.add_node(LineageNode("C", LineageNodeType.STORED_ROW, "C", "game", "1"))
    dag.add_edge(LineageEdge("A", "B", LineageEdgeType.CRAWLED_FROM))
    dag.add_edge(LineageEdge("B", "C", LineageEdgeType.STORED_AS))

    assert dag.has_cycle() is False

    # Introduce cycle: C -> A
    dag.add_edge(LineageEdge("C", "A", LineageEdgeType.DERIVED_FROM))
    assert dag.has_cycle() is True


def test_lineage_graph_orphan_node_detection() -> None:
    """Test that LineageGraph flags completely disconnected nodes."""
    graph = LineageGraph(root_node_id="A")
    graph.add_node(LineageNode("A", LineageNodeType.SOURCE_SNAPSHOT, "A", "source", "1"))
    graph.add_node(LineageNode("B", LineageNodeType.CRAWL_RUN, "B", "run", "1"))
    graph.add_node(LineageNode("ORPHAN_NODE", LineageNodeType.STORED_ROW, "Orphan", "game", "99"))
    graph.add_edge(LineageEdge("A", "B", LineageEdgeType.CRAWLED_FROM))

    orphans = graph.get_orphaned_nodes()
    assert "ORPHAN_NODE" in orphans
    assert len(orphans) == 1


def test_full_lineage_census_all_clean_tables(tmp_path: Path) -> None:
    """Test exhaustive lineage census across all 8 tables on clean database."""
    db_file = tmp_path / "census_test.db"
    test_engine = create_engine(f"sqlite:///{db_file}")
    Base.metadata.create_all(bind=test_engine)

    with test_engine.begin() as conn:
        conn.execute(
            text("""
            INSERT INTO player_basic (player_id, name) VALUES (75847, '최정');
        """)
        )
        conn.execute(
            text("""
            INSERT INTO game (game_id, season_id, game_date, home_team, away_team, home_score, away_score, game_status, stadium)
            VALUES ('20240401SKLT0', 2024, '2024-04-01', 'SK', 'LT', 5, 2, 'COMPLETED', 'Incheon');
        """)
        )
        conn.execute(
            text("""
            INSERT INTO game_batting_stats (game_id, player_id, player_name, team_side, appearance_seq, runs, hits, at_bats, home_runs)
            VALUES ('20240401SKLT0', 75847, '최정', 'home', 1, 1, 2, 4, 1);
        """)
        )
        conn.execute(
            text("""
            INSERT INTO game_pitching_stats (game_id, player_id, player_name, team_side, appearance_seq, innings_pitched, earned_runs)
            VALUES ('20240401SKLT0', 201, 'Pitcher1', 'home', 1, '6.0', 0);
        """)
        )
        conn.execute(
            text("""
            INSERT INTO game_play_by_play (game_id, inning, inning_half, play_description)
            VALUES ('20240401SKLT0', 1, '초', '1구 스트라이크');
        """)
        )
        conn.execute(
            text("""
            INSERT INTO game_lineups (game_id, team_side, team_code, batting_order, appearance_seq, player_name, position)
            VALUES ('20240401SKLT0', 'home', 'SK', 3, 1, '최정', '3B');
        """)
        )
        conn.execute(
            text("""
            INSERT INTO player_season_batting (player_id, season, league, level, source, hits, home_runs, at_bats)
            VALUES (75847, 2024, 'KBO', '1군', 'kbo', 2, 1, 4);
        """)
        )
        conn.execute(
            text("""
            INSERT INTO player_season_pitching (player_id, season, league, level, source, wins, losses, era)
            VALUES (201, 2024, 'KBO', '1군', 'kbo', 1, 0, 0.0);
        """)
        )

    engine = LineageEngine(test_engine)

    # 1. Full census mode
    report_full = engine.audit_lineage(full=True)
    assert report_full.audit_mode == "FULL"
    assert report_full.compliance_status == "FULLY TRACEABLE"
    assert report_full.is_compliant is True
    assert report_full.broken_lineage_count == 0
    assert report_full.traceability_ratio == 1.0
    assert len(report_full.table_breakdowns) == 8
    assert report_full.table_breakdowns["game"].eligible_rows == 1
    assert report_full.table_breakdowns["game_batting_stats"].eligible_rows == 1
    assert report_full.table_breakdowns["remediation_records"].eligible_rows == 7

    # 2. Sample mode
    report_sample = engine.audit_lineage(sample=5)
    assert report_sample.audit_mode == "SAMPLE"
    assert "SAMPLE AUDIT PASS" in report_sample.compliance_status
    assert report_sample.is_compliant is True
    test_engine.dispose()


def test_lineage_census_detects_broken_child_fk(tmp_path: Path) -> None:
    """Test that census detects broken foreign key relationships (child rows with missing game parent)."""
    db_file = tmp_path / "broken_fk.db"
    test_engine = create_engine(f"sqlite:///{db_file}")
    Base.metadata.create_all(bind=test_engine)

    with test_engine.begin() as conn:
        conn.execute(
            text("""
            INSERT INTO game (game_id, season_id, game_date, home_team, away_team, home_score, away_score, game_status, stadium)
            VALUES ('20240401SKLT0', 2024, '2024-04-01', 'SK', 'LT', 5, 2, 'COMPLETED', 'Incheon');
        """)
        )
        # Valid child row
        conn.execute(
            text("""
            INSERT INTO game_batting_stats (game_id, player_id, player_name, team_side, appearance_seq, runs, hits, at_bats, home_runs)
            VALUES ('20240401SKLT0', 101, 'Batter1', 'home', 1, 1, 2, 4, 0);
        """)
        )
        # ORPHAN child row (game parent '99999999NONEXISTENT' does not exist)
        conn.execute(
            text("""
            INSERT INTO game_batting_stats (game_id, player_id, player_name, team_side, appearance_seq, runs, hits, at_bats, home_runs)
            VALUES ('99999999NONEXISTENT', 999, 'OrphanBatter', 'away', 1, 0, 0, 1, 0);
        """)
        )

    engine = LineageEngine(test_engine)
    report = engine.audit_lineage(full=True)

    assert report.is_compliant is False
    assert report.broken_lineage_count == 1
    assert report.compliance_status == "DEFECTS DETECTED"
    assert report.table_breakdowns["game_batting_stats"].broken_rows == 1
    assert report.table_breakdowns["game_batting_stats"].traceable_rows == 1
    test_engine.dispose()


def test_transitive_provenance_contract_types() -> None:
    """Test OriginType assignments across different node types."""
    n_src = LineageNode(
        "S", LineageNodeType.SOURCE_SNAPSHOT, "Src", "source", "1", origin_type=OriginType.EXTERNAL_SOURCE
    )
    n_metric = LineageNode(
        "M", LineageNodeType.DERIVED_METRIC, "AVG", "metric", "1", origin_type=OriginType.DERIVED_INPUTS
    )
    n_rem = LineageNode(
        "R", LineageNodeType.REMEDIATION_ACTION, "Fix", "correction", "1", origin_type=OriginType.DECLARED_REMEDIATION
    )
    n_stub = LineageNode(
        "T", LineageNodeType.STORED_ROW, "Stub", "player", "1", origin_type=OriginType.SYSTEM_GENERATED
    )

    assert n_src.origin_type == OriginType.EXTERNAL_SOURCE
    assert n_metric.origin_type == OriginType.DERIVED_INPUTS
    assert n_rem.origin_type == OriginType.DECLARED_REMEDIATION
    assert n_stub.origin_type == OriginType.SYSTEM_GENERATED
