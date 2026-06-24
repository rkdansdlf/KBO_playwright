"""PLR0913 Batch 1: Refactor 10 functions + create_run with dataclasses."""
import re

def read_file(path):
    with open(path) as f:
        return f.read()

def write_file(path, content):
    with open(path, 'w') as f:
        f.write(content)

# ============================================================
# 0. crawl_run_repository.py — RunStats
# ============================================================
content = read_file('src/repositories/crawl_run_repository.py')

# Add dataclass import
content = content.replace(
    'from typing import TYPE_CHECKING',
    'from dataclasses import dataclass\nfrom typing import TYPE_CHECKING'
)

# Add RunStats dataclass before class
content = content.replace(
    '\nclass CrawlRunRepository:\n',
    '''\n@dataclass\nclass RunStats:\n    label: str | None\n    started_at: datetime\n    finished_at: datetime\n    active_count: int\n    retired_count: int\n    staff_count: int\n    confirmed_profiles: int\n    heuristic_only: int\n\n\nclass CrawlRunRepository:\n'''
)

# Replace create_run signature and body
content = content.replace(
    '''    def create_run(  # noqa: PLR0913
        self,
        *,
        label: str | None,
        started_at: datetime,
        finished_at: datetime,
        active_count: int,
        retired_count: int,
        staff_count: int,
        confirmed_profiles: int,
        heuristic_only: int,
    ) -> CrawlRun:
        with SessionLocal() as session:
            run = CrawlRun(
                label=label,
                started_at=started_at,
                finished_at=finished_at,
                active_count=active_count,
                retired_count=retired_count,
                staff_count=staff_count,
                confirmed_profiles=confirmed_profiles,
                heuristic_only=heuristic_only,
            )
            session.add(run)
            session.commit()
            session.refresh(run)
            return run''',
    '''    def create_run(self, stats: RunStats) -> CrawlRun:
        with SessionLocal() as session:
            run = CrawlRun(
                label=stats.label,
                started_at=stats.started_at,
                finished_at=stats.finished_at,
                active_count=stats.active_count,
                retired_count=stats.retired_count,
                staff_count=stats.staff_count,
                confirmed_profiles=stats.confirmed_profiles,
                heuristic_only=stats.heuristic_only,
            )
            session.add(run)
            session.commit()
            session.refresh(run)
            return run'''
)

write_file('src/repositories/crawl_run_repository.py', content)
print("✅ crawl_run_repository.py: RunStats added, create_run refactored")

# ============================================================
# 1. game_deduplication_service.py — CandidateQuery
# ============================================================
content = read_file('src/services/game_deduplication_service.py')

# Add dataclass import
if 'from dataclasses import dataclass' not in content:
    content = content.replace(
        'from datetime import datetime',
        'from dataclasses import dataclass\nfrom datetime import datetime'
    )

# Add CandidateQuery dataclass
content = content.replace(
    '\ndef _load_candidates(  # noqa: PLR0913',
    '''\n@dataclass\nclass CandidateQuery:\n    game_date: str\n    home_fid: int\n    away_fid: int\n    suffix: str\n    start_date: str | None\n    end_date: str | None\n    suffixes: Sequence[str]\n\n\ndef _load_candidates('''
)

# Replace signature
content = content.replace(
    '''def _load_candidates(
    cursor: sqlite3.Cursor,
    *,
    game_date: str,
    home_fid: int,
    away_fid: int,
    suffix: str,
    start_date: str | None,
    end_date: str | None,
    suffixes: Sequence[str],
) -> list[tuple[str, int]]:''',
    '''def _load_candidates(
    cursor: sqlite3.Cursor,
    *,
    query: CandidateQuery,
) -> list[tuple[str, int]]:'''
)

# Update body
content = content.replace('params: list[object] = [game_date, home_fid, away_fid, f"%{suffix}"]', 'params: list[object] = [query.game_date, query.home_fid, query.away_fid, f"%{query.suffix}"]')
content = content.replace('if start_date and end_date:', 'if query.start_date and query.end_date:')
content = content.replace('params.extend([start_date, end_date])', 'params.extend([query.start_date, query.end_date])')
content = content.replace('if suffixes:', 'if query.suffixes:')
content = content.replace('suffix_placeholders = ",".join("?" for _ in suffixes)', 'suffix_placeholders = ",".join("?" for _ in query.suffixes)')
content = content.replace('params.extend(suffixes)', 'params.extend(query.suffixes)')

# Update call site
content = content.replace(
    '''        candidates = _load_candidates(
            cursor,
            game_date=game_date,
            home_fid=home_fid,
            away_fid=away_fid,
            suffix=suffix,
            start_date=start_date,
            end_date=end_date,
            suffixes=suffixes,
        )''',
    '''        candidates = _load_candidates(
            cursor,
            query=CandidateQuery(
                game_date=game_date,
                home_fid=home_fid,
                away_fid=away_fid,
                suffix=suffix,
                start_date=start_date,
                end_date=end_date,
                suffixes=suffixes,
            ),
        )'''
)

write_file('src/services/game_deduplication_service.py', content)
print("✅ game_deduplication_service.py: CandidateQuery added, _load_candidates refactored")

# ============================================================
# 2. map_api_client.py — TransitRequest
# ============================================================
content = read_file('src/utils/map_api_client.py')

# Add dataclass import
content = content.replace(
    'import httpx\nimport logging',
    'from dataclasses import dataclass\n\nimport httpx\nimport logging'
)

# Add TransitRequest dataclass after JAMSIL_LNG constant
content = content.replace(
    'JAMSIL_LNG = 127.0719\n\n\nasync def get_transit_time(  # noqa: PLR0913',
    '''JAMSIL_LNG = 127.0719


@dataclass
class TransitRequest:
    origin_label: str
    origin_lat: float
    origin_lng: float
    mode: TransportMode = "mixed"
    dest_lat: float = JAMSIL_LAT
    dest_lng: float = JAMSIL_LNG


async def get_transit_time('''
)

# Replace signature
content = content.replace(
    '''async def get_transit_time(
    origin_label: str,
    origin_lat: float,
    origin_lng: float,
    mode: TransportMode = "mixed",
    dest_lat: float = JAMSIL_LAT,
    dest_lng: float = JAMSIL_LNG,
) -> TransitResult | None:''',
    '''async def get_transit_time(req: TransitRequest) -> TransitResult | None:'''
)

# Update body
content = content.replace(
    'result = await caller(client, origin_lat, origin_lng, dest_lat, dest_lng, mode)',
    'result = await caller(client, req.origin_lat, req.origin_lng, req.dest_lat, req.dest_lng, req.mode)'
)
content = content.replace(
    '                    origin_label=origin_label,\n                    transport_mode=mode,',
    '                    origin_label=req.origin_label,\n                    transport_mode=req.mode,'
)
content = content.replace(
    '    logger.warning("[MapAPI] All APIs failed for origin=%s", origin_label)',
    '    logger.warning("[MapAPI] All APIs failed for origin=%s", req.origin_label)'
)

# Update call site in get_transit_times_batch
content = content.replace(
    'tasks = [get_transit_time(o["label"], o["lat"], o["lng"], mode, dest_lat, dest_lng) for o in origins]',
    'tasks = [get_transit_time(TransitRequest(o["label"], o["lat"], o["lng"], mode, dest_lat, dest_lng)) for o in origins]'
)

write_file('src/utils/map_api_client.py', content)
print("✅ map_api_client.py: TransitRequest added, get_transit_time refactored")

# ============================================================
# 3. at_bat_grouper.py — AtBatContext
# ============================================================
content = read_file('src/utils/at_bat_grouper.py')

# Add dataclass import
content = content.replace(
    'import logging\nfrom typing import Any',
    'import logging\nfrom dataclasses import dataclass\nfrom typing import Any'
)

# Add AtBatContext dataclass
content = content.replace(
    '\ndef _needs_new_at_bat(  # noqa: PLR0913',
    '''\n@dataclass\nclass AtBatContext:\n    current_batter_key: tuple[int | str | None, str | None, str] | None\n    inning: int | str | None\n    half: str | None\n    batter_name: str\n    current_batter: str | None\n    has_seen_result_this_at_bat: bool\n    event_type: str\n\n\ndef _needs_new_at_bat('''
)

# Replace signature
content = content.replace(
    '''def _needs_new_at_bat(
    *,
    current_batter_key: tuple[int | str | None, str | None, str] | None,
    inning: int | str | None,
    half: str | None,
    batter_name: str,
    current_batter: str | None,
    has_seen_result_this_at_bat: bool,
    event_type: str,
) -> bool:''',
    '''def _needs_new_at_bat(*, ctx: AtBatContext) -> bool:'''
)

# Update body
content = content.replace(
    'return (\n        (current_batter_key is not None and current_batter_key[:2] != (inning, half))\n        or (batter_name and current_batter is not None and batter_name != current_batter)\n        or (has_seen_result_this_at_bat and event_type in AT_BAT_TERMINAL_EVENTS)\n        or (current_batter is None and bool(batter_name))\n    )',
    'return (\n        (ctx.current_batter_key is not None and ctx.current_batter_key[:2] != (ctx.inning, ctx.half))\n        or (ctx.batter_name and ctx.current_batter is not None and ctx.batter_name != ctx.current_batter)\n        or (ctx.has_seen_result_this_at_bat and ctx.event_type in AT_BAT_TERMINAL_EVENTS)\n        or (ctx.current_batter is None and bool(ctx.batter_name))\n    )'
)

# Update call site
content = content.replace(
    '''        if _needs_new_at_bat(
            current_batter_key=current_batter_key,
            inning=inning,
            half=half,
            batter_name=batter_name,
            current_batter=current_batter,
            has_seen_result_this_at_bat=has_seen_result_this_at_bat,
            event_type=event_type,
        ):''',
    '''        if _needs_new_at_bat(
            ctx=AtBatContext(
                current_batter_key=current_batter_key,
                inning=inning,
                half=half,
                batter_name=batter_name,
                current_batter=current_batter,
                has_seen_result_this_at_bat=has_seen_result_this_at_bat,
                event_type=event_type,
            ),
        ):'''
)

write_file('src/utils/at_bat_grouper.py', content)
print("✅ at_bat_grouper.py: AtBatContext added, _needs_new_at_bat refactored")

# ============================================================
# 4. relay_validation.py — OutCountContext
# ============================================================
content = read_file('src/utils/relay_validation.py')

# Add dataclass import
content = content.replace(
    'import logging\nfrom typing import TYPE_CHECKING, Any',
    'import logging\nfrom dataclasses import dataclass\nfrom typing import TYPE_CHECKING, Any'
)

# Add OutCountContext dataclass
content = content.replace(
    '\ndef _out_count_warnings(  # noqa: PLR0913',
    '''\n@dataclass\nclass OutCountContext:\n    index: int\n    outs: int | None\n    inning: int | None\n    half: str | None\n    prev_outs: int | None\n    prev_inning: int | None\n    prev_half: str | None\n\n\ndef _out_count_warnings('''
)

# Replace signature
content = content.replace(
    '''def _out_count_warnings(
    index: int,
    outs: int | None,
    inning: int | None,
    half: str | None,
    prev_outs: int | None,
    prev_inning: int | None,
    prev_half: str | None,
) -> list[str]:''',
    '''def _out_count_warnings(ctx: OutCountContext) -> list[str]:'''
)

# Update body
content = content.replace(
    'if outs is None or prev_outs is None:',
    'if ctx.outs is None or ctx.prev_outs is None:'
)
content = content.replace(
    'return [f"event_{index}: out count out of range {outs}"]',
    'return [f"event_{ctx.index}: out count out of range {ctx.outs}"]'
)
content = content.replace(
    'if index <= 0 or inning != prev_inning or half != prev_half:',
    'if ctx.index <= 0 or ctx.inning != ctx.prev_inning or ctx.half != ctx.prev_half:'
)
content = content.replace(
    'out_diff = outs - prev_outs',
    'out_diff = ctx.outs - ctx.prev_outs'
)
content = content.replace(
    'return [f"event_{index}: outs decreased {prev_outs}->{outs} without inning change"]',
    'return [f"event_{ctx.index}: outs decreased {ctx.prev_outs}->{ctx.outs} without inning change"]'
)
content = content.replace(
    'return [f"event_{index}: outs jumped by {out_diff} in one event"]',
    'return [f"event_{ctx.index}: outs jumped by {out_diff} in one event"]'
)

# Update call site
content = content.replace(
    'warnings.extend(_out_count_warnings(i, outs, inning, half, prev_outs, prev_inning, prev_half))',
    'warnings.extend(_out_count_warnings(OutCountContext(i, outs, inning, half, prev_outs, prev_inning, prev_half)))'
)

write_file('src/utils/relay_validation.py', content)
print("✅ relay_validation.py: OutCountContext added, _out_count_warnings refactored")

# ============================================================
# 5. sync_oci.py — SyncRunConfig
# ============================================================
content = read_file('src/cli/sync_oci.py')

# Add dataclass import
content = content.replace(
    'import argparse\nimport logging',
    'import argparse\nimport logging\nfrom dataclasses import dataclass'
)

# Add SyncRunConfig dataclass
content = content.replace(
    '\ndef _run_sync(  # noqa: PLR0913',
    '''\n@dataclass\nclass SyncRunConfig:\n    parallel_support: bool = False\n    header: str | None = None\n    years_getter: Callable[[Session], list[int]] | None = None\n    completion_msg: str | None = None\n\n\ndef _run_sync('''
)

# Replace signature
content = content.replace(
    '''def _run_sync(
    args: argparse.Namespace,
    sync_fn: Callable[..., object],
    *,
    parallel_support: bool = False,
    header: str | None = None,
    years_getter: Callable[[Session], list[int]] | None = None,
    completion_msg: str | None = None,
) -> None:''',
    '''def _run_sync(
    args: argparse.Namespace,
    sync_fn: Callable[..., object],
    *,
    config: SyncRunConfig,
) -> None:'''
)

# Update body
content = content.replace('if header:', 'if config.header:')
content = content.replace('logger.info(header)', 'logger.info(config.header)')
content = content.replace(
    'if parallel_support and args.parallel and years_getter:',
    'if config.parallel_support and args.parallel and config.years_getter:'
)
content = content.replace('target_years = years_getter(session)', 'target_years = config.years_getter(session)')
content = content.replace('if completion_msg:', 'if config.completion_msg:')
content = content.replace('logger.info(completion_msg)', 'logger.info(config.completion_msg)')

# Update call site
content = content.replace(
    '''        _run_sync(
            args,
            sync_fn,
            parallel_support=parallel_ok,
            header=header_str,
            years_getter=year_getter,
            completion_msg=completion_msg,
        )''',
    '''        _run_sync(
            args,
            sync_fn,
            config=SyncRunConfig(
                parallel_support=parallel_ok,
                header=header_str,
                years_getter=year_getter,
                completion_msg=completion_msg,
            ),
        )'''
)

write_file('src/cli/sync_oci.py', content)
print("✅ sync_oci.py: SyncRunConfig added, _run_sync refactored")

# ============================================================
# 6. dashboard_report.py — ViolationContext
# ============================================================
content = read_file('src/cli/dashboard_report.py')

# Add dataclass import
content = content.replace(
    'from datetime import datetime\nfrom typing import TYPE_CHECKING, Any',
    'from dataclasses import dataclass\nfrom datetime import datetime\nfrom typing import TYPE_CHECKING, Any'
)

# Add ViolationContext dataclass
content = content.replace(
    '\ndef _append_quality_violation_lines(  # noqa: PLR0913',
    '''\n@dataclass\nclass ViolationContext:\n    pa_ok: bool\n    team_bat_ok: bool\n    team_pit_ok: bool\n\n\ndef _append_quality_violation_lines('''
)

# Replace signature
content = content.replace(
    '''def _append_quality_violation_lines(
    msg_lines: list[str],
    quality: dict[str, Any],
    gate: dict[str, Any],
    *,
    pa_ok: bool,
    team_bat_ok: bool,
    team_pit_ok: bool,
) -> None:''',
    '''def _append_quality_violation_lines(
    msg_lines: list[str],
    quality: dict[str, Any],
    gate: dict[str, Any],
    *,
    ctx: ViolationContext,
) -> None:'''
)

# Update body
content = content.replace('if not pa_ok:', 'if not ctx.pa_ok:')
content = content.replace('if not team_bat_ok:', 'if not ctx.team_bat_ok:')
content = content.replace('if not team_pit_ok:', 'if not ctx.team_pit_ok:')
content = content.replace(
    '_append_first_mismatch_line(msg_lines, gate, "team_batting", "팀타격", is_ok=team_bat_ok)',
    '_append_first_mismatch_line(msg_lines, gate, "team_batting", "팀타격", is_ok=ctx.team_bat_ok)'
)
content = content.replace(
    '_append_first_mismatch_line(msg_lines, gate, "team_pitching", "팀투수", is_ok=team_pit_ok)',
    '_append_first_mismatch_line(msg_lines, gate, "team_pitching", "팀투수", is_ok=ctx.team_pit_ok)'
)

# Update call site
content = content.replace(
    '''    _append_quality_violation_lines(
        msg_lines,
        quality,
        gate,
        pa_ok=pa_ok,
        team_bat_ok=team_bat_ok,
        team_pit_ok=team_pit_ok,
    )''',
    '''    _append_quality_violation_lines(
        msg_lines,
        quality,
        gate,
        ctx=ViolationContext(pa_ok=pa_ok, team_bat_ok=team_bat_ok, team_pit_ok=team_pit_ok),
    )'''
)

write_file('src/cli/dashboard_report.py', content)
print("✅ dashboard_report.py: ViolationContext added, _append_quality_violation_lines refactored")

# ============================================================
# 7. run_daily_update.py — RecoveryConfig
# ============================================================
content = read_file('src/cli/run_daily_update.py')

# Add dataclass import
content = content.replace(
    'import argparse\nimport asyncio',
    'import argparse\nimport asyncio\nfrom dataclasses import dataclass'
)

# Add RecoveryConfig dataclass
content = content.replace(
    '\nasync def _run_detail_recovery_passes(  # noqa: PLR0913',
    '''\n@dataclass\nclass RecoveryConfig:\n    ctx: _RunContext\n    g_crawler: GameDetailCrawler\n    detail_results_by_game: dict\n    unrecovered_game_ids: set[str]\n    recoverable_failure_ids: set[str]\n    max_recovery_rounds: int\n\n\nasync def _run_detail_recovery_passes('''
)

# Replace signature
content = content.replace(
    '''async def _run_detail_recovery_passes(
    ctx: _RunContext,
    g_crawler: GameDetailCrawler,
    detail_results_by_game: dict,
    unrecovered_game_ids: set[str],
    recoverable_failure_ids: set[str],
    max_recovery_rounds: int,
) -> None:''',
    '''async def _run_detail_recovery_passes(config: RecoveryConfig) -> None:'''
)

# Update body — need to replace all param references
content = content.replace('for _ in range(max_recovery_rounds - 1):', 'for _ in range(config.max_recovery_rounds - 1):')
content = content.replace(
    'if ctx.detail_recovery_attempts.get(game_id, 0) < max_recovery_rounds',
    'if config.ctx.detail_recovery_attempts.get(game_id, 0) < config.max_recovery_rounds'
)
content = content.replace(
    'ctx.detail_recovery_passes += 1',
    'config.ctx.detail_recovery_passes += 1'
)
content = content.replace(
    'ctx.detail_games_by_id[game_id] for game_id in retry_game_ids',
    'config.ctx.detail_games_by_id[game_id] for game_id in retry_game_ids'
)
content = content.replace(
    'ctx.detail_recovery_attempts[game_id] = ctx.detail_recovery_attempts.get(game_id, 0) + 1',
    'config.ctx.detail_recovery_attempts[game_id] = config.ctx.detail_recovery_attempts.get(game_id, 0) + 1'
)
content = content.replace(
    'logger.info(\n            "   \U0001f501 Detail recovery pass #%s (%s game(s))", ctx.detail_recovery_passes, len(retry_targets)\n        )',
    'logger.info(\n            "   \U0001f501 Detail recovery pass #%s (%s game(s))", config.ctx.detail_recovery_passes, len(retry_targets)\n        )'
)
content = content.replace(
    'retry_targets,\n            detail_crawler=g_crawler,',
    'retry_targets,\n            detail_crawler=config.g_crawler,'
)
content = content.replace(
    'write_contract=ctx.write_contract,',
    'write_contract=config.ctx.write_contract,'
)
content = content.replace(
    'source_reason=f"postgame_finalize:{ctx.target_date}:recovery",',
    'source_reason=f"postgame_finalize:{config.ctx.target_date}:recovery",'
)
content = content.replace(
    'if normalized_game_id in unrecovered_game_ids:',
    'if normalized_game_id in config.unrecovered_game_ids:'
)
content = content.replace(
    'unrecovered_game_ids.remove(normalized_game_id)',
    'config.unrecovered_game_ids.remove(normalized_game_id)'
)
content = content.replace(
    'ctx.detail_recovered_after_retry += 1',
    'config.ctx.detail_recovered_after_retry += 1'
)
content = content.replace(
    'recoverable_failure_ids.discard(normalized_game_id)',
    'config.recoverable_failure_ids.discard(normalized_game_id)'
)
content = content.replace(
    'elif not _is_recoverable_detail_reason(item.failure_reason):\n                unrecovered_game_ids.discard(normalized_game_id)\n                recoverable_failure_ids.discard(normalized_game_id)',
    'elif not _is_recoverable_detail_reason(item.failure_reason):\n                config.unrecovered_game_ids.discard(normalized_game_id)\n                config.recoverable_failure_ids.discard(normalized_game_id)'
)

# Update call site
content = content.replace(
    '''    await _run_detail_recovery_passes(
        ctx,
        g_crawler,
        detail_results_by_game,
        unrecovered_game_ids,
        recoverable_failure_ids,
        max(1, int(DETAIL_RECOVERY_MAX_ROUNDS)),
    )''',
    '''    await _run_detail_recovery_passes(
        RecoveryConfig(
            ctx=ctx,
            g_crawler=g_crawler,
            detail_results_by_game=detail_results_by_game,
            unrecovered_game_ids=unrecovered_game_ids,
            recoverable_failure_ids=recoverable_failure_ids,
            max_recovery_rounds=max(1, int(DETAIL_RECOVERY_MAX_ROUNDS)),
        )
    )'''
)

write_file('src/cli/run_daily_update.py', content)
print("✅ run_daily_update.py: RecoveryConfig added, _run_detail_recovery_passes refactored")

# ============================================================
# 8. regenerate_game_stories.py — StoryGameContext
# ============================================================
content = read_file('src/cli/regenerate_game_stories.py')

# Add dataclass import
content = content.replace(
    'import logging\nimport sys',
    'import logging\nimport sys\nfrom dataclasses import dataclass'
)

# Add StoryGameContext dataclass
content = content.replace(
    '\ndef _process_story_game(  # noqa: PLR0913',
    '''\n@dataclass\nclass StoryGameContext:\n    session: Session\n    game: Game\n    detail_crawler: Any\n    roster_crawler: Any\n    pbp_crawler: Any\n    write_contract: GameWriteContract | None\n\n\ndef _process_story_game('''
)

# Replace signature
content = content.replace(
    '''def _process_story_game(
    session: Session,
    game: Game,
    detail_crawler: Any,
    roster_crawler: Any,
    pbp_crawler: Any,
    write_contract: GameWriteContract | None,
) -> tuple[Any, bool]:''',
    '''def _process_story_game(ctx: StoryGameContext) -> tuple[Any, bool]:'''
)

# Update body — replace param references with ctx.xxx
# This is complex, let me do targeted replacements
content = content.replace('detail_games = _fetch_games(session, game.game_id)', 'detail_games = _fetch_games(ctx.session, ctx.game.game_id)')
content = content.replace('pbp_rows = _fetch_pbp(pbp_crawler, game.game_id)', 'pbp_rows = _fetch_pbp(ctx.pbp_crawler, ctx.game.game_id)')
content = content.replace('if not pbp_rows:', 'if not pbp_rows:')
content = content.replace('roster = _fetch_roster(roster_crawler, game.game_id)', 'roster = _fetch_roster(ctx.roster_crawler, ctx.game.game_id)')
content = content.replace('if write_contract:', 'if ctx.write_contract:')
content = content.replace(
    'save_game_stories(session, game.game_id, stories, write_contract=write_contract)',
    'save_game_stories(ctx.session, ctx.game.game_id, stories, write_contract=ctx.write_contract)'
)
content = content.replace(
    'row, should_sync = _process_story_game(session, game, detail_crawler, roster_crawler, pbp_crawler, write_contract)',
    'row, should_sync = _process_story_game(StoryGameContext(session, game, detail_crawler, roster_crawler, pbp_crawler, write_contract))'
)

write_file('src/cli/regenerate_game_stories.py', content)
print("✅ regenerate_game_stories.py: StoryGameContext added, _process_story_game refactored")

# ============================================================
# 9. wpa_calculator.py — WpaInput
# ============================================================
content = read_file('src/services/wpa_calculator.py')

# Add dataclass import
content = content.replace(
    'import logging\n\nfrom src.constants',
    'import logging\nfrom dataclasses import dataclass\n\nfrom src.constants'
)

# Add WpaInput dataclass before class
content = content.replace(
    '\nclass WPACalculator:\n',
    '''\n@dataclass\nclass WpaInput:\n    inning: int\n    is_bottom: bool\n    outs_before: int\n    runners_before: int\n    score_diff_before: int\n    outs_after: int\n    runners_after: int\n    score_diff_after: int\n\n\nclass WPACalculator:\n'''
)

# Replace calculate_wpa signature
content = content.replace(
    '''    def calculate_wpa(  # noqa: PLR0913
        self,
        inning: int,
        *,
        is_bottom: bool,
        outs_before: int,
        runners_before: int,
        score_diff_before: int,
        outs_after: int,
        runners_after: int,
        score_diff_after: int,
    ) -> float:''',
    '''    def calculate_wpa(self, *, data: WpaInput) -> float:'''
)

# Update body
content = content.replace(
    'we_before = self.get_win_probability(\n            inning,\n            is_bottom=is_bottom,\n            outs=outs_before,\n            runners=runners_before,\n            score_diff=score_diff_before,\n        )',
    'we_before = self.get_win_probability(\n            data.inning,\n            is_bottom=data.is_bottom,\n            outs=data.outs_before,\n            runners=data.runners_before,\n            score_diff=data.score_diff_before,\n        )'
)
content = content.replace(
    'we_after = self.get_win_probability(\n            inning,\n            is_bottom=is_bottom,\n            outs=outs_after,\n            runners=runners_after,\n            score_diff=score_diff_after,\n        )',
    'we_after = self.get_win_probability(\n            data.inning,\n            is_bottom=data.is_bottom,\n            outs=data.outs_after,\n            runners=data.runners_after,\n            score_diff=data.score_diff_after,\n        )'
)
content = content.replace(
    'wpa = we_after - we_before if is_bottom else we_before - we_after',
    'wpa = we_after - we_before if data.is_bottom else we_before - we_after'
)

write_file('src/services/wpa_calculator.py', content)
print("✅ wpa_calculator.py: WpaInput added, calculate_wpa refactored")

# ============================================================
# 10. game_relay.py — RelayRowReplaceContext
# ============================================================
content = read_file('src/repositories/game_relay.py')

# Add dataclass import
content = content.replace(
    'from dataclasses import dataclass\n',
    'from dataclasses import dataclass\n'
)
if 'from dataclasses import dataclass' not in content:
    content = content.replace(
        'from datetime import date, datetime',
        'from dataclasses import dataclass\nfrom datetime import date, datetime'
    )

# Add RelayRowReplaceContext dataclass
content = content.replace(
    '\ndef _replace_relay_rows(  # noqa: PLR0913',
    '''\n@dataclass\nclass RelayRowReplaceContext:\n    pbp_rows: list[GamePlayByPlay]\n    event_rows: list[GameEvent]\n    source: GameWriteSource\n    write_contract: GameWriteContract | None\n\n\ndef _replace_relay_rows('''
)

# Replace signature
content = content.replace(
    '''def _replace_relay_rows(
    session: Session,
    game_id: str,
    pbp_rows: list[GamePlayByPlay],
    event_rows: list[GameEvent],
    source: GameWriteSource,
    write_contract: GameWriteContract | None,
) -> bool:''',
    '''def _replace_relay_rows(
    session: Session,
    game_id: str,
    *,
    ctx: RelayRowReplaceContext,
) -> bool:'''
)

# Update body
content = content.replace('if pbp_rows:', 'if ctx.pbp_rows:')
content = content.replace(
    'GamePlayByPlay,\n            game_id,\n            pbp_rows,',
    'GamePlayByPlay,\n            game_id,\n            ctx.pbp_rows,'
)
content = content.replace(
    'RecordReplaceContext(source=source, write_contract=write_contract),\n        )\n    if event_rows:',
    'RecordReplaceContext(source=ctx.source, write_contract=ctx.write_contract),\n        )\n    if ctx.event_rows:'
)
content = content.replace(
    'GameEvent,\n            game_id,\n            event_rows,',
    'GameEvent,\n            game_id,\n            ctx.event_rows,'
)
content = content.replace(
    'RecordReplaceContext(source=source, write_contract=write_contract),\n    )\n    return changed',
    'RecordReplaceContext(source=ctx.source, write_contract=ctx.write_contract),\n    )\n    return changed'
)

# Update call site
content = content.replace(
    'changed = _replace_relay_rows(session, game_id, pbp_rows, event_rows, source, opts.write_contract)',
    'changed = _replace_relay_rows(\n                session,\n                game_id,\n                ctx=RelayRowReplaceContext(\n                    pbp_rows=pbp_rows,\n                    event_rows=event_rows,\n                    source=source,\n                    write_contract=opts.write_contract,\n                ),\n            )'
)

write_file('src/repositories/game_relay.py', content)
print("✅ game_relay.py: RelayRowReplaceContext added, _replace_relay_rows refactored")

print("\n" + "=" * 60)
print("Batch 1 complete! 10 functions + create_run refactored.")
