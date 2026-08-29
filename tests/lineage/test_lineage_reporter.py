"""Tests for LineageReporter Tree, Mermaid, and JSON Renderers."""

from __future__ import annotations

from src.lineage.models import (
    CorrectionRecord,
    GameLineageReport,
    LineageAuditReport,
    LineageEdge,
    LineageEdgeType,
    LineageGraph,
    LineageNode,
    LineageNodeType,
    PlayerMetricLineageReport,
    TableLineageCensus,
)
from src.lineage.reporter import LineageReporter


def test_render_game_tree_and_mermaid() -> None:
    """Test ASCII tree and Mermaid generation for game reports."""
    graph = LineageGraph(root_node_id="game:20240401LGNC0")
    node_game = LineageNode(
        node_id="game:20240401LGNC0",
        node_type=LineageNodeType.STORED_ROW,
        label="Game 20240401LGNC0",
        entity_type="game",
        entity_id="20240401LGNC0",
    )
    node_src = LineageNode(
        node_id="source:20240401LGNC0",
        node_type=LineageNodeType.SOURCE_SNAPSHOT,
        label="Official Boxscore",
        entity_type="source",
        entity_id="20240401LGNC0",
    )
    graph.add_node(node_game)
    graph.add_node(node_src)
    graph.add_edge(LineageEdge(node_src.node_id, node_game.node_id, LineageEdgeType.CRAWLED_FROM, "Scraped HTML"))

    report = GameLineageReport(
        game_id="20240401LGNC0",
        game_date="2024-04-01",
        home_team="NC",
        away_team="LG",
        home_score=3,
        away_score=5,
        game_status="COMPLETED",
        graph=graph,
        sources=[{"source_name": "kbo_official", "base_url": "https://koreabaseball.com"}],
        stored_tables={"game": 1, "game_batting_stats": 18},
        corrections=[
            CorrectionRecord(
                entity_type="game",
                entity_id="20240401LGNC0",
                field_name="away_score",
                original_value=None,
                corrected_value=5,
                reason="Test correction",
                remediation_id="REM-TEST",
            )
        ],
        certification_status={"H01": "PASS"},
    )

    tree_str = LineageReporter.render_game_tree(report)
    assert "Game Lineage: 20240401LGNC0" in tree_str
    assert "NC 3 vs LG 5" in tree_str
    assert "REM-TEST" in tree_str

    mermaid_str = LineageReporter.render_mermaid(graph)
    assert "graph TD" in mermaid_str
    assert "source_20240401LGNC0" in mermaid_str
    assert "game_20240401LGNC0" in mermaid_str

    json_str = LineageReporter.render_json(report)
    assert '"game_id": "20240401LGNC0"' in json_str


def test_render_player_and_audit_tree() -> None:
    """Test ASCII tree for player metric and audit reports."""
    graph = LineageGraph(root_node_id="metric:1")
    player_rep = PlayerMetricLineageReport(
        player_id=52622,
        player_name="김도영",
        season=2024,
        metric_name="hits",
        metric_value=143,
        formula="SUM(hits)",
        graph=graph,
        team_scheduled_games=144,
        player_appeared_games=141,
        expected_contributing_rows=141,
        observed_contributing_rows=141,
        lineage_coverage=1.0,
        participation_rate=0.979,
        transformation_chain=["Step 1", "Step 2"],
        contributing_rows_sample=[{"game_id": "20240401HTLG0", "hits": 2, "home_runs": 1, "at_bats": 4}],
    )

    p_tree = LineageReporter.render_player_tree(player_rep)
    assert "Player Metric Lineage: 김도영" in p_tree
    assert "HITS = 143" in p_tree
    assert "141 games" in p_tree
    assert "Participation Rate" in p_tree
    assert "Lineage Coverage" in p_tree

    census = TableLineageCensus(
        table_name="game",
        total_rows=50,
        eligible_rows=50,
        traceable_rows=50,
        broken_rows=0,
        na_rows=0,
        traceability_ratio=1.0,
    )

    audit_rep = LineageAuditReport(
        audit_mode="FULL",
        season=2024,
        total_population=50,
        eligible_entities=50,
        fully_traceable_count=50,
        broken_lineage_count=0,
        na_count=0,
        traceability_ratio=1.0,
        table_breakdowns={"game": census},
        cycles_detected=0,
        orphaned_nodes=[],
        duration_ms=25.0,
        is_compliant=True,
        compliance_status="FULLY TRACEABLE",
    )
    a_tree = LineageReporter.render_audit_tree(audit_rep)
    assert "KBO DATA LINEAGE & PROVENANCE AUDIT" in a_tree
    assert "FULLY TRACEABLE" in a_tree
    assert "game" in a_tree
