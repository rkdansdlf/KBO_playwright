"""Core Data Models and Graph Structures for Phase 103 Data Lineage & Provenance."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class OriginType(StrEnum):
    """Classification of the ultimate origin for a data value or entity."""

    EXTERNAL_SOURCE = "EXTERNAL_SOURCE"  # Direct external webpage scrape / API payload
    DERIVED_INPUTS = "DERIVED_INPUTS"  # Calculated mathematical metric aggregated from core rows
    DECLARED_REMEDIATION = "DECLARED_REMEDIATION"  # Certified patch / normalization record
    SYSTEM_GENERATED = "SYSTEM_GENERATED"  # Metadata, stubs, sequence IDs, synthetic descriptors


class LineageNodeType(StrEnum):
    """Classification of nodes in the data lineage DAG."""

    SOURCE_SNAPSHOT = "SOURCE_SNAPSHOT"  # Raw external web snapshot / URL
    CRAWL_RUN = "CRAWL_RUN"  # Crawler execution instance
    RAW_PAYLOAD = "RAW_PAYLOAD"  # Intermediate extracted dictionary / JSON
    PARSER = "PARSER"  # Parsing rule and schema engine
    STORED_ROW = "STORED_ROW"  # Persisted database record in core table
    DERIVED_METRIC = "DERIVED_METRIC"  # Calculated season stat or sabermetric
    CERTIFICATION_GATE = "CERTIFICATION_GATE"  # Verification gate result (G01~G10, H01~H07)
    REMEDIATION_ACTION = "REMEDIATION_ACTION"  # Historical correction / backfill patch


class LineageEdgeType(StrEnum):
    """Classification of directed relationships between lineage nodes."""

    CRAWLED_FROM = "CRAWLED_FROM"  # CrawlRun -> SourceSnapshot
    EXTRACTED_BY = "EXTRACTED_BY"  # RawPayload -> Parser
    STORED_AS = "STORED_AS"  # StoredRow -> RawPayload / Parser
    AGGREGATED_INTO = "AGGREGATED_INTO"  # StoredRow -> DerivedMetric
    DERIVED_FROM = "DERIVED_FROM"  # DerivedMetric -> StoredRow
    CERTIFIED_BY = "CERTIFIED_BY"  # StoredRow -> CertificationGate
    CORRECTED_BY = "CORRECTED_BY"  # StoredRow -> RemediationAction


@dataclass
class LineageNode:
    """Represents an atomic entity, artifact, or transformation in the lineage graph."""

    node_id: str
    node_type: LineageNodeType
    label: str
    entity_type: str  # "game", "player", "team", "metric", "source", "run"
    entity_id: str
    metadata: dict[str, Any] = field(default_factory=dict)
    origin_type: OriginType = OriginType.EXTERNAL_SOURCE

    def to_dict(self) -> dict[str, Any]:
        """Convert lineage node to dictionary."""
        return {
            "node_id": self.node_id,
            "node_type": self.node_type.value,
            "origin_type": self.origin_type.value,
            "label": self.label,
            "entity_type": self.entity_type,
            "entity_id": self.entity_id,
            "metadata": self.metadata,
        }


@dataclass
class LineageEdge:
    """Represents a directed provenance relationship between two lineage nodes."""

    source_node_id: str
    target_node_id: str
    edge_type: LineageEdgeType
    description: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert lineage edge to dictionary."""
        return {
            "source_node_id": self.source_node_id,
            "target_node_id": self.target_node_id,
            "edge_type": self.edge_type.value,
            "description": self.description,
        }


@dataclass
class LineageGraph:
    """Directed Acyclic Graph (DAG) representing end-to-end data provenance."""

    root_node_id: str
    nodes: dict[str, LineageNode] = field(default_factory=dict)
    edges: list[LineageEdge] = field(default_factory=list)

    def add_node(self, node: LineageNode) -> None:
        """Add a node to the lineage graph."""
        self.nodes[node.node_id] = node

    def add_edge(self, edge: LineageEdge) -> None:
        """Add a directed edge to the lineage graph."""
        self.edges.append(edge)

    def get_upstream_nodes(self, node_id: str) -> list[LineageNode]:
        """Find all nodes that feed directly into the specified node."""
        upstream_ids = [e.source_node_id for e in self.edges if e.target_node_id == node_id]
        return [self.nodes[uid] for uid in upstream_ids if uid in self.nodes]

    def get_downstream_nodes(self, node_id: str) -> list[LineageNode]:
        """Find all nodes that derive directly from the specified node."""
        downstream_ids = [e.target_node_id for e in self.edges if e.source_node_id == node_id]
        return [self.nodes[did] for did in downstream_ids if did in self.nodes]

    def has_cycle(self) -> bool:
        """Check if the lineage graph contains cycles using 3-color DFS traversal."""
        adj: dict[str, list[str]] = {nid: [] for nid in self.nodes}
        for e in self.edges:
            if e.source_node_id in adj and e.target_node_id in self.nodes:
                adj[e.source_node_id].append(e.target_node_id)

        # 0: WHITE (unvisited), 1: GRAY (visiting), 2: BLACK (visited)
        state: dict[str, int] = dict.fromkeys(self.nodes, 0)

        def _dfs(u: str) -> bool:
            state[u] = 1
            for v in adj.get(u, []):
                if state[v] == 1:
                    return True
                if state[v] == 0 and _dfs(v):
                    return True
            state[u] = 2
            return False

        return any(state[nid] == 0 and _dfs(nid) for nid in self.nodes)

    def get_orphaned_nodes(self) -> list[str]:
        """Find nodes completely disconnected from any edges (when total nodes > 1)."""
        if len(self.nodes) <= 1:
            return []
        connected = set()
        for e in self.edges:
            connected.add(e.source_node_id)
            connected.add(e.target_node_id)
        return [nid for nid in self.nodes if nid not in connected]

    def to_dict(self) -> dict[str, Any]:
        """Convert entire lineage graph to dictionary."""
        return {
            "root_node_id": self.root_node_id,
            "nodes": {nid: n.to_dict() for nid, n in self.nodes.items()},
            "edges": [e.to_dict() for e in self.edges],
            "total_nodes": len(self.nodes),
            "total_edges": len(self.edges),
            "has_cycle": self.has_cycle(),
            "orphaned_nodes": self.get_orphaned_nodes(),
        }


@dataclass
class CorrectionRecord:
    """Detailed record of a data remediation or normalization event."""

    entity_type: str
    entity_id: str
    field_name: str
    original_value: Any
    corrected_value: Any
    reason: str
    remediation_id: str
    affected_table: str = "game"
    affected_count: int = 1
    source_evidence: str = "KBO Official BoxScore & Play-by-Play Verification"
    timestamp: str = "2026-08-29"
    code_revision: str = "7b2f9a8c"
    certification_run_id: str = "CERT-45-SEASONS-LOCAL"
    reversible: bool = True

    def to_dict(self) -> dict[str, Any]:
        """Convert correction record to dictionary."""
        return {
            "remediation_id": self.remediation_id,
            "entity_type": self.entity_type,
            "entity_id": self.entity_id,
            "affected_table": self.affected_table,
            "affected_count": self.affected_count,
            "field_name": self.field_name,
            "original_value": self.original_value,
            "corrected_value": self.corrected_value,
            "reason": self.reason,
            "source_evidence": self.source_evidence,
            "timestamp": self.timestamp,
            "code_revision": self.code_revision,
            "certification_run_id": self.certification_run_id,
            "reversible": self.reversible,
        }


@dataclass
class GameLineageReport:
    """Complete provenance and lineage report for a specific game."""

    game_id: str
    game_date: str
    home_team: str
    away_team: str
    home_score: int | None
    away_score: int | None
    game_status: str
    graph: LineageGraph
    sources: list[dict[str, Any]] = field(default_factory=list)
    crawl_runs: list[dict[str, Any]] = field(default_factory=list)
    stored_tables: dict[str, int] = field(default_factory=dict)
    corrections: list[CorrectionRecord] = field(default_factory=list)
    certification_status: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert game lineage report to dictionary."""
        return {
            "game_id": self.game_id,
            "game_date": self.game_date,
            "home_team": self.home_team,
            "away_team": self.away_team,
            "home_score": self.home_score,
            "away_score": self.away_score,
            "game_status": self.game_status,
            "sources": self.sources,
            "crawl_runs": self.crawl_runs,
            "stored_tables": self.stored_tables,
            "corrections": [c.to_dict() for c in self.corrections],
            "certification_status": self.certification_status,
            "graph": self.graph.to_dict(),
        }


@dataclass
class PlayerMetricLineageReport:
    """Complete provenance and derivation report for an aggregated player season metric."""

    player_id: int
    player_name: str
    season: int
    metric_name: str
    metric_value: Any
    formula: str
    graph: LineageGraph
    team_scheduled_games: int = 0
    player_appeared_games: int = 0
    expected_contributing_rows: int = 0
    observed_contributing_rows: int = 0
    lineage_coverage: float = 1.0  # observed / expected contributing rows (1.0 = 100%)
    participation_rate: float = 1.0  # player appeared / team scheduled games
    contributing_rows_sample: list[dict[str, Any]] = field(default_factory=list)
    transformation_chain: list[str] = field(default_factory=list)
    certification_status: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert player metric lineage report to dictionary."""
        return {
            "player_id": self.player_id,
            "player_name": self.player_name,
            "season": self.season,
            "metric_name": self.metric_name,
            "metric_value": self.metric_value,
            "formula": self.formula,
            "team_scheduled_games": self.team_scheduled_games,
            "player_appeared_games": self.player_appeared_games,
            "expected_contributing_rows": self.expected_contributing_rows,
            "observed_contributing_rows": self.observed_contributing_rows,
            "lineage_coverage": self.lineage_coverage,
            "participation_rate": self.participation_rate,
            "contributing_rows_sample": self.contributing_rows_sample,
            "transformation_chain": self.transformation_chain,
            "certification_status": self.certification_status,
            "graph": self.graph.to_dict(),
        }


@dataclass
class TableLineageCensus:
    """Lineage completeness census metrics for a specific database table."""

    table_name: str
    total_rows: int
    eligible_rows: int
    traceable_rows: int
    broken_rows: int
    na_rows: int
    traceability_ratio: float

    def to_dict(self) -> dict[str, Any]:
        """Convert table lineage census to dictionary."""
        return {
            "table_name": self.table_name,
            "total_rows": self.total_rows,
            "eligible_rows": self.eligible_rows,
            "traceable_rows": self.traceable_rows,
            "broken_rows": self.broken_rows,
            "na_rows": self.na_rows,
            "traceability_ratio": self.traceability_ratio,
        }


@dataclass
class LineageAuditReport:
    """System-wide or season-wide lineage completeness audit report."""

    audit_mode: str  # "SAMPLE" or "FULL"
    season: int | None
    total_population: int
    eligible_entities: int
    fully_traceable_count: int
    broken_lineage_count: int
    na_count: int
    traceability_ratio: float
    table_breakdowns: dict[str, TableLineageCensus]
    cycles_detected: int
    orphaned_nodes: list[str]
    duration_ms: float
    is_compliant: bool
    compliance_status: str  # "FULLY TRACEABLE", "SAMPLE AUDIT PASS", "DEFECTS DETECTED"
    sample_size: int | None = None
    git_sha: str = ""
    generated_at_utc: str = ""
    sha256_checksum: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert lineage audit report to dictionary."""
        return {
            "audit_mode": self.audit_mode,
            "sample_size": self.sample_size,
            "season": self.season,
            "total_population": self.total_population,
            "eligible_entities": self.eligible_entities,
            "fully_traceable_count": self.fully_traceable_count,
            "broken_lineage_count": self.broken_lineage_count,
            "na_count": self.na_count,
            "traceability_ratio": self.traceability_ratio,
            "cycles_detected": self.cycles_detected,
            "orphaned_nodes": self.orphaned_nodes,
            "duration_ms": self.duration_ms,
            "is_compliant": self.is_compliant,
            "compliance_status": self.compliance_status,
            "git_sha": self.git_sha,
            "generated_at_utc": self.generated_at_utc,
            "sha256_checksum": self.sha256_checksum,
            "table_breakdowns": {t: c.to_dict() for t, c in self.table_breakdowns.items()},
        }


__all__ = [
    "CorrectionRecord",
    "GameLineageReport",
    "LineageAuditReport",
    "LineageEdge",
    "LineageEdgeType",
    "LineageGraph",
    "LineageNode",
    "LineageNodeType",
    "OriginType",
    "PlayerMetricLineageReport",
    "TableLineageCensus",
]
