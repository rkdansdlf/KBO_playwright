"""cli 패키지."""

from __future__ import annotations

"""
KBO Playwright CLI Commands.

Available via: python -m src.cli.<module> [args]

Core Pipeline:
  run_daily_update       — Postgame finalize + OCI sync
  daily_preview_batch    — Pregame preview refresh
  live_crawler           — Live game data crawl
  crawl_futures          — Futures league stats
  crawl_retire           — Retired player stats

Data Crawlers:
  crawl_schedule         — Season schedule
  crawl_p0_data          — P0 data (events + roster + ticket)
  crawl_team_events      — Team news/events
  crawl_kbo_official_events — KBO official event/promotion links
  crawl_roster_transactions — Roster transactions
  crawl_ticket_info      — Ticket prices/open rules
  crawl_operation_notices — Stadium operation notices
  crawl_congestion       — Stadium congestion
  crawl_transit_time     — Transit time measurement
  crawl_parking          — Parking lot data
  crawl_seat_sections    — Seat sections
  crawl_stadium_food     — Food vendor/menu
  crawl_phase1_extra     — MVP/injury/foreign player data

Backfill & Maintenance:
  backfill_advanced_stats  — Advanced season stats
  backfill_pregame_previews — Pregame pitcher backfill
  gap_report               — Unified gap analysis (Tier 3)
  auto_healer              — PBP auto-healer
  monitor_data_freshness   — Stale source detection
  refresh_source_snapshots — Refresh DataSource snapshots and last-success timestamps
  freshness_gate           — OCI freshness check
  generate_quality_report  — Daily quality report
  recalc_season_stats      — Full stat recalculation
  recalc_player_stats      — Player season stats recalc
  recalc_team_stats        — Team stats recalc
  recalc_player_game_stats — Player game-level stats recalc
  sync_oci                 — SQLite → OCI sync
  audit_pa_formula         — PA formula audit and fix

Analysis:
  calculate_standings   — Daily standings with splits
  dashboard_report      — Full pipeline dashboard
  health_check          — DB health check
  check_data_status     — Data completeness status
"""
