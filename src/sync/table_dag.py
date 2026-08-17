"""Table dependency DAG for the SQLite to Oracle ADB initial load."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class SyncStrategy(StrEnum):
    """Synchronization strategy for database tables."""

    INCREMENTAL = "incremental"
    APPEND_ONLY = "append_only"
    SNAPSHOT = "snapshot"
    TRUNCATE_INSERT = "truncate_insert"


@dataclass
class TableMeta:
    """Metadata and dependency configuration for a table."""

    name: str
    level: int  # 0: base master, 1: entities/seasons, 2: game details/stats, 3: pbp/logs/cache
    strategy: SyncStrategy = SyncStrategy.INCREMENTAL
    timestamp_col: str | None = "updated_at"
    id_col: str | None = "id"
    natural_keys: list[str] | None = None
    omit_id: bool = False
    replace_by_default: bool = False


# Table metadata registry ordered by dependency level
TABLE_REGISTRY: list[TableMeta] = [
    # Level 0: Master tables & Static references
    TableMeta("kbo_seasons", level=0, strategy=SyncStrategy.SNAPSHOT, natural_keys=["season_id"]),
    TableMeta("team_code_map", level=0, strategy=SyncStrategy.SNAPSHOT, natural_keys=["id"]),
    TableMeta("team_franchises", level=0, strategy=SyncStrategy.SNAPSHOT, natural_keys=["id"]),
    TableMeta("teams", level=0, strategy=SyncStrategy.SNAPSHOT, natural_keys=["team_id"]),
    TableMeta("team_history", level=0, strategy=SyncStrategy.SNAPSHOT, natural_keys=["id"]),
    TableMeta("player_basic", level=0, strategy=SyncStrategy.INCREMENTAL, natural_keys=["player_id"]),
    TableMeta("players", level=0, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("stadium_info", level=0, strategy=SyncStrategy.SNAPSHOT, natural_keys=["stadium_code"]),
    TableMeta("data_sources", level=0, strategy=SyncStrategy.SNAPSHOT, natural_keys=["source_key"]),
    TableMeta("cheer_songs", level=0, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("ticket_open_rules", level=0, strategy=SyncStrategy.SNAPSHOT, natural_keys=["id"]),
    TableMeta("ticket_prices", level=0, strategy=SyncStrategy.SNAPSHOT, natural_keys=["id"]),
    TableMeta("parking_lots", level=0, strategy=SyncStrategy.SNAPSHOT, natural_keys=["id"]),
    TableMeta("stadium_foods", level=0, strategy=SyncStrategy.SNAPSHOT, natural_keys=["id"]),
    TableMeta("stadium_food_vendors", level=0, strategy=SyncStrategy.SNAPSHOT, natural_keys=["id"]),
    TableMeta("stadium_seat_sections", level=0, strategy=SyncStrategy.SNAPSHOT, natural_keys=["id"]),
    TableMeta("stadium_regulations", level=0, strategy=SyncStrategy.SNAPSHOT, natural_keys=["id"]),
    TableMeta("team_rivalries", level=0, strategy=SyncStrategy.SNAPSHOT, natural_keys=["id"]),
    TableMeta("cheer_chants", level=0, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    # Level 1: Core season & game entities
    TableMeta("game", level=1, strategy=SyncStrategy.INCREMENTAL, timestamp_col="updated_at", natural_keys=["game_id"]),
    TableMeta(
        "game_id_aliases",
        level=1,
        strategy=SyncStrategy.INCREMENTAL,
        natural_keys=["alias_game_id"],
    ),
    TableMeta(
        "game_metadata",
        level=1,
        strategy=SyncStrategy.INCREMENTAL,
        timestamp_col="updated_at",
        natural_keys=["game_id"],
    ),
    TableMeta("game_validation_metrics", level=1, strategy=SyncStrategy.INCREMENTAL, natural_keys=["game_id"]),
    TableMeta(
        "game_summary",
        level=1,
        strategy=SyncStrategy.INCREMENTAL,
        timestamp_col="updated_at",
        natural_keys=["id"],
    ),
    TableMeta(
        "player_season_batting",
        level=1,
        strategy=SyncStrategy.INCREMENTAL,
        natural_keys=["id"],
    ),
    TableMeta(
        "player_season_pitching",
        level=1,
        strategy=SyncStrategy.INCREMENTAL,
        natural_keys=["id"],
    ),
    TableMeta("player_season_fielding", level=1, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("player_season_baserunning", level=1, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("team_season_batting", level=1, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("team_season_pitching", level=1, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("team_season_fielding", level=1, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("team_season_baserunning", level=1, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("team_standings_daily", level=1, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("player_identities", level=1, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("awards", level=1, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("manager_changes", level=1, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("foreign_player_changes", level=1, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("fa_contracts", level=1, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("player_draft_histories", level=1, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("player_milestones", level=1, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("player_splits_stats", level=1, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("futures_game_schedules", level=1, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("futures_team_standings", level=1, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta(
        "quarantined_records",
        level=1,
        strategy=SyncStrategy.APPEND_ONLY,
        timestamp_col="created_at",
        natural_keys=["id"],
    ),
    TableMeta(
        "correction_audit_trail",
        level=1,
        strategy=SyncStrategy.APPEND_ONLY,
        timestamp_col="created_at",
        natural_keys=["id"],
    ),
    TableMeta(
        "player_projections",
        level=1,
        strategy=SyncStrategy.INCREMENTAL,
        timestamp_col="updated_at",
        natural_keys=["id"],
    ),
    # Level 2: Game-level detail stats & lineups
    TableMeta(
        "game_lineups",
        level=2,
        strategy=SyncStrategy.INCREMENTAL,
        natural_keys=["game_id", "team_side", "appearance_seq"],
    ),
    TableMeta(
        "game_batting_stats",
        level=2,
        strategy=SyncStrategy.INCREMENTAL,
        natural_keys=["game_id", "player_id", "appearance_seq"],
    ),
    TableMeta(
        "game_pitching_stats",
        level=2,
        strategy=SyncStrategy.INCREMENTAL,
        natural_keys=["game_id", "player_id", "appearance_seq"],
    ),
    TableMeta(
        "game_inning_scores",
        level=2,
        strategy=SyncStrategy.INCREMENTAL,
        natural_keys=["game_id", "team_side", "inning"],
    ),
    TableMeta("player_game_batting", level=2, strategy=SyncStrategy.INCREMENTAL, natural_keys=["game_id", "player_id"]),
    TableMeta(
        "player_game_pitching", level=2, strategy=SyncStrategy.INCREMENTAL, natural_keys=["game_id", "player_id"]
    ),
    TableMeta("game_highlights", level=2, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("game_mvps", level=2, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("game_broadcasts", level=2, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("team_daily_roster", level=2, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("roster_transactions", level=2, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("player_movements", level=2, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("team_events", level=2, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("injury_entries", level=2, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("stat_rankings", level=2, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("sla_metrics", level=2, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("stadium_operation_notices", level=2, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("parking_fee_rules", level=2, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("stadium_food_menu_items", level=2, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("stadium_congestion", level=2, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("stadium_transit_times", level=2, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("ticket_schedules", level=2, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("kbo_press_releases", level=2, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("crawl_runs", level=2, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("crawl_evidence", level=2, strategy=SyncStrategy.APPEND_ONLY, natural_keys=["id"]),
    TableMeta(
        "external_season_stats",
        level=2,
        strategy=SyncStrategy.INCREMENTAL,
        natural_keys=["source_record_key"],
    ),
    # Level 3: High-volume logs, play-by-play, snapshots, embeddings
    TableMeta(
        "game_play_by_play",
        level=3,
        strategy=SyncStrategy.INCREMENTAL,
        timestamp_col="created_at",
        natural_keys=["id"],
    ),
    TableMeta("game_events", level=3, strategy=SyncStrategy.INCREMENTAL, natural_keys=["game_id", "event_seq"]),
    TableMeta(
        "raw_source_snapshots",
        level=3,
        strategy=SyncStrategy.APPEND_ONLY,
        timestamp_col="fetched_at",
        natural_keys=["id"],
    ),
    TableMeta(
        "embedding_cache",
        level=3,
        strategy=SyncStrategy.INCREMENTAL,
        natural_keys=["text_hash", "model_name"],
    ),
    TableMeta("rag_chunks", level=3, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("matchup_batter_home_away", level=3, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("matchup_batter_splits", level=3, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("matchup_batter_stadium_split", level=3, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("matchup_batter_team_split", level=3, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("matchup_batter_vs_starter", level=3, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("matchup_bvp", level=3, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("matchup_pitcher_home_away", level=3, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("matchup_pitcher_splits", level=3, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
    TableMeta("matchup_pitcher_team_split", level=3, strategy=SyncStrategy.INCREMENTAL, natural_keys=["id"]),
]

TABLE_META_BY_NAME: dict[str, TableMeta] = {t.name: t for t in TABLE_REGISTRY}


def get_tables_by_level() -> dict[int, list[TableMeta]]:
    """Group table metadata by execution level."""
    levels: dict[int, list[TableMeta]] = {}
    for meta in TABLE_REGISTRY:
        levels.setdefault(meta.level, []).append(meta)
    return levels
