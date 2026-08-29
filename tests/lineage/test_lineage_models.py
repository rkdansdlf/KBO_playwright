"""Tests for Lineage Data Models and Graph Structures."""

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
    OriginType,
    PlayerMetricLineageReport,
    TableLineageCensus,
)


def test_lineage_node_and_edge_serialization() -> None:
    """Test node and edge creation and serialization."""
    node1 = LineageNode(
        node_id="source:20240401LGNC0",
        node_type=LineageNodeType.SOURCE_SNAPSHOT,
        origin_type=OriginType.EXTERNAL_SOURCE,
        label="Official BoxScore Webpage",
        entity_type="source",
        entity_id="20240401LGNC0",
        metadata={"url": "https://koreabaseball.com/..."},
    )
    node2 = LineageNode(
        node_id="game:20240401LGNC0",
        node_type=LineageNodeType.STORED_ROW,
        origin_type=OriginType.EXTERNAL_SOURCE,
        label="Game 20240401LGNC0 (LG 5 vs NC 3)",
        entity_type="game",
        entity_id="20240401LGNC0",
    )
    edge = LineageEdge(
        source_node_id=node1.node_id,
        target_node_id=node2.node_id,
        edge_type=LineageEdgeType.CRAWLED_FROM,
        description="Extracted raw game data",
    )

    d_node = node1.to_dict()
    assert d_node["node_id"] == "source:20240401LGNC0"
    assert d_node["node_type"] == "SOURCE_SNAPSHOT"
    assert d_node["origin_type"] == "EXTERNAL_SOURCE"

    d_edge = edge.to_dict()
    assert d_edge["edge_type"] == "CRAWLED_FROM"
    assert d_edge["description"] == "Extracted raw game data"


def test_lineage_graph_traversal() -> None:
    """Test graph construction, upstream, and downstream lookups."""
    graph = LineageGraph(root_node_id="metric:52622:2024:hits")

    n_src = LineageNode(
        node_id="src:1",
        node_type=LineageNodeType.SOURCE_SNAPSHOT,
        origin_type=OriginType.EXTERNAL_SOURCE,
        label="Source HTML",
        entity_type="source",
        entity_id="1",
    )
    n_parser = LineageNode(
        node_id="parser:1",
        node_type=LineageNodeType.PARSER,
        origin_type=OriginType.EXTERNAL_SOURCE,
        label="Parser v2.1",
        entity_type="parser",
        entity_id="1",
    )
    n_stored = LineageNode(
        node_id="game_row:1",
        node_type=LineageNodeType.STORED_ROW,
        origin_type=OriginType.EXTERNAL_SOURCE,
        label="GameBattingStats",
        entity_type="game_batting_stats",
        entity_id="1",
    )
    n_metric = LineageNode(
        node_id="metric:52622:2024:hits",
        node_type=LineageNodeType.DERIVED_METRIC,
        origin_type=OriginType.DERIVED_INPUTS,
        label="Kim Do-yeong Hits = 143",
        entity_type="metric",
        entity_id="52622_2024_hits",
    )

    graph.add_node(n_src)
    graph.add_node(n_parser)
    graph.add_node(n_stored)
    graph.add_node(n_metric)

    graph.add_edge(LineageEdge(n_src.node_id, n_parser.node_id, LineageEdgeType.EXTRACTED_BY))
    graph.add_edge(LineageEdge(n_parser.node_id, n_stored.node_id, LineageEdgeType.STORED_AS))
    graph.add_edge(LineageEdge(n_stored.node_id, n_metric.node_id, LineageEdgeType.AGGREGATED_INTO))

    assert len(graph.nodes) == 4
    assert len(graph.edges) == 3

    upstream = graph.get_upstream_nodes("metric:52622:2024:hits")
    assert len(upstream) == 1
    assert upstream[0].node_id == "game_row:1"

    downstream = graph.get_downstream_nodes("src:1")
    assert len(downstream) == 1
    assert downstream[0].node_id == "parser:1"


def test_reports_serialization() -> None:
    """Test GameLineageReport, PlayerMetricLineageReport, and LineageAuditReport."""
    graph = LineageGraph(root_node_id="game:1")
    game_rep = GameLineageReport(
        game_id="20240401LGNC0",
        game_date="2024-04-01",
        home_team="NC",
        away_team="LG",
        home_score=3,
        away_score=5,
        game_status="COMPLETED",
        graph=graph,
        stored_tables={"game": 1, "game_batting_stats": 18},
    )
    assert game_rep.to_dict()["game_id"] == "20240401LGNC0"

    player_rep = PlayerMetricLineageReport(
        player_id=52622,
        player_name="김도영",
        season=2024,
        metric_name="hits",
        metric_value=143,
        formula="SUM(game_batting_stats.hits)",
        graph=graph,
        team_scheduled_games=144,
        player_appeared_games=141,
        expected_contributing_rows=141,
        observed_contributing_rows=141,
        lineage_coverage=1.0,
        participation_rate=0.979,
    )
    p_dict = player_rep.to_dict()
    assert p_dict["metric_value"] == 143
    assert p_dict["player_appeared_games"] == 141
    assert p_dict["lineage_coverage"] == 1.0

    corr = CorrectionRecord(
        entity_type="game",
        entity_id="20210523LTOB0",
        affected_table="game",
        affected_count=1,
        field_name="away_score",
        original_value=None,
        corrected_value=0,
        reason="Shutout correction",
        remediation_id="REM-1",
    )
    assert corr.to_dict()["corrected_value"] == 0
    assert corr.to_dict()["affected_table"] == "game"

    census = TableLineageCensus(
        table_name="game",
        total_rows=100,
        eligible_rows=100,
        traceable_rows=100,
        broken_rows=0,
        na_rows=0,
        traceability_ratio=1.0,
    )

    audit_rep = LineageAuditReport(
        audit_mode="FULL",
        season=2024,
        total_population=100,
        eligible_entities=100,
        fully_traceable_count=100,
        broken_lineage_count=0,
        na_count=0,
        traceability_ratio=1.0,
        table_breakdowns={"game": census},
        cycles_detected=0,
        orphaned_nodes=[],
        duration_ms=45.2,
        is_compliant=True,
        compliance_status="FULLY TRACEABLE",
    )
    a_dict = audit_rep.to_dict()
    assert a_dict["is_compliant"] is True
    assert a_dict["audit_mode"] == "FULL"
    assert "game" in a_dict["table_breakdowns"]
