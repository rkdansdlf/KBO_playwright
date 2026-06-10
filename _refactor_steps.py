"""
Temporary script to refactor run_daily_update.py - generate step functions and new run_update.
"""
import re

with open('src/cli/run_daily_update.py', 'r') as f:
    content = f.read()

lines = content.split('\n')

# Find the end of _format_target_date
insert_after = -1
for i, line in enumerate(lines):
    if 'return fallback_game_id[:8]' in line:
        insert_after = i
        break

# Find the start of run_update
run_update_start = -1
for i, line in enumerate(lines):
    if 'async def run_update(' in line:
        run_update_start = i
        break

# Find the end of run_update (the function that ends right before build_arg_parser)
def build_arg_parser_line = -1
run_update_end = -1
for i, line in enumerate(lines):
    if 'def build_arg_parser() -> argparse.ArgumentParser:' in line:
        build_arg_parser_line = i
        run_update_end = i - 2  # blank lines before it
        break

print(f"Insert step functions after line {insert_after+1}")
print(f"run_update: lines {run_update_start+1} to {run_update_end+1}")
print(f"build_arg_parser at line {build_arg_parser_line+1}")

# Generate step functions using the ORIGINAL file's step blocks
# We'll extract each step's code from the original run_update body

# Read the original run_update body for reference
body_start = -1
for i, line in enumerate(lines):
    if '"""Main orchestration logic for postgame finalize and daily reconciliation."""' in line:
        body_start = i
        break

print(f"Body starts at line {body_start+1}")

# Now construct the modified content
# Part 1: Everything before insert_after+1 (up to and including return fallback_game_id[:8])
# Part 2: Step functions (new)
# Part 3: From insert_after+2 to body_start-1 (blank lines + async def run_update + ... + docstring)
# Part 4: New simplified run_update body (replace the old body)
# Part 5: From run_update_end+1 to end

# For Part 4, we need the FULL old body to replace it
old_body = '\n'.join(lines[body_start:run_update_end+1])
print(f"Old body length: {len(old_body)} chars, {run_update_end - body_start + 1} lines")

# New simplified run_update body (with ctx setup)
new_body = '''    """Main orchestration logic for postgame finalize and daily reconciliation."""
    ctx = _RunContext(
        target_date=target_date,
        sync=sync,
        year=int(target_date[:4]),
        month=int(target_date[4:6]),
        today_kst=_today_kst(),
        runner=step_runner or _run_python_step,
        write_contract=GameWriteContract(run_label=f"daily_update:{target_date}", log=logger.info),
        step_runner=step_runner,
        summary_dir=summary_dir,
        seed_tomorrow_preview=seed_tomorrow_preview,
        run_auto_healer=run_auto_healer,
        run_postgame_reconciliation=run_postgame_reconciliation,
        postgame_reconcile_lookback_days=postgame_reconcile_lookback_days,
        fix=fix,
        skip_season_stats=skip_season_stats,
        skip_oci_supporting_sync=skip_oci_supporting_sync,
        run_p0_non_game=run_p0_non_game,
        headless=headless,
        limit=limit,
        detail_recovery_queue=RecoveryManager(checkpoint_path=DETAIL_RECOVERY_QUEUE_PATH),
    )
    ctx.detail_recovery_queue.purge_detail_recovery_queue()
    ctx.queued_recovery_game_ids = set(
        ctx.detail_recovery_queue.get_due_detail_recovery_targets(
            target_date,
            cooldown_minutes=DETAIL_RECOVERY_COOLDOWN_MINUTES,
        ),
    )

    logger.info(f"\\n{'=' * 60}")
    logger.info("\\U0001f680 KBO Daily Finalize Started for Date: %s", target_date)
    logger.info(f"{'=' * 60}")

    await _step_0_auto_healer(ctx)
    await _step_1_schedule(ctx)
    await _step_2_detail_crawl(ctx)
    await _step_3_refresh_status(ctx)
    await _step_4_relay_recovery(ctx)
    await _step_4_5_proactive_relay(ctx)
    await _step_5_content_generation(ctx)
    await _step_6_player_stats(ctx)
    await _step_6_5_maintenance(ctx)
    await _step_7_rosters(ctx)
    await _step_7_5_p0_non_game(ctx)
    await _step_8_derived_stats(ctx)
    await _step_10_7_enrichment(ctx)
    await _step_11_sync_pipeline(ctx)
    await _step_14_tomorrow_preview(ctx)

    summary_path = _daily_summary_path(target_date, summary_dir)
    stability_summary = _build_stability_summary(
        detail_failure_counts=ctx.detail_failure_counts,
        detail_failure_game_ids=ctx.detail_failure_game_ids,
        relay_recovery_target_ids=sorted(ctx.relay_recovery_target_ids),
        oci_skip_counts=ctx.oci_skip_counts,
        oci_skip_game_ids=ctx.oci_skip_game_ids,
        non_p0_quality_gate_counts=ctx.non_p0_quality_gate_counts,
        non_p0_quality_gate_ids=ctx.non_p0_quality_gate_ids,
        p0_non_game_counts=ctx.p0_non_game_counts,
        p0_non_game_errors=ctx.p0_non_game_errors,
        detail_recovery_passes=ctx.detail_recovery_passes,
        detail_recovered_after_retry=ctx.detail_recovered_after_retry,
        detail_still_missing=ctx.detail_still_missing,
        detail_recovery_attempts=ctx.detail_recovery_attempts,
        detail_recovery_escalation_game_ids=ctx.detail_retry_escalation_game_ids,
        summary_path=summary_path,
    )

    try:
        with SessionLocal() as p0_session:
            p0_readiness = build_p0_readiness(
                p0_session,
                target_date=target_date,
                lookback_days=0,
                lookahead_days=0,
                oci_skip_counts=ctx.oci_skip_counts,
                oci_skip_game_ids=ctx.oci_skip_game_ids,
            )
    except Exception:
        logger.exception("   Error building P0 readiness summary")
        p0_readiness = {
            "target_date": target_date,
            "schedule": {},
            "pregame": {},
            "live": {},
            "postgame": {},
            "relay": {},
            "roster": {},
            "broadcast": {},
            "oci": {"skip_counts": dict(ctx.oci_skip_counts), "skip_game_ids": dict(ctx.oci_skip_game_ids)},
            "failures": [
                {
                    "dataset": "p0_readiness",
                    "game_id": None,
                    "game_date": target_date,
                    "reason": "readiness_build_failed",
                    "severity": "critical",
                },
            ],
            "summary": {"ok": False, "failure_count": 1, "critical_failure_count": 1, "warning_count": 0},
        }

    manifest_path = write_refresh_manifest(
        phase="postgame_finalize",
        target_date=target_date,
        game_ids=sorted(set(ctx.processed_game_ids) | set(ctx.reconciliation_changed_ids))
        or ctx.status_refresh_game_ids
        or [game["game_id"] for game in ctx.daily_games],
        datasets=[
            "game",
            "game_metadata",
            "game_inning_scores",
            "game_lineups",
            "game_events",
            "game_summary",
            "game_play_by_play",
            "team_events",
            "ticket_prices",
            "ticket_open_rules",
            "roster_transactions",
        ],
        derived_refresh=ctx.derived_refresh,
        stability=stability_summary,
    )
    _write_daily_update_summary(
        target_date=target_date,
        stability=stability_summary,
        p0_readiness=p0_readiness,
        manifest_path=manifest_path,
        summary_path=summary_path,
    )

    logger.info(ctx.write_contract.summary())
    logger.info(
        "Stability summary: "
        f"detail_failures={_format_counts(ctx.detail_failure_counts)} "
        f"detail_recovery_passes={ctx.detail_recovery_passes} "
        f"detail_recovered_after_retry={ctx.detail_recovered_after_retry} "
        f"detail_still_missing={len(ctx.detail_still_missing)} "
        f"relay_targets={len(ctx.relay_recovery_target_ids)} "
        f"oci_skips={_format_counts(ctx.oci_skip_counts)} "
        f"non_p0_quality_gates={_format_counts(ctx.non_p0_quality_gate_counts)} "
        f"p0_non_game={_format_counts(ctx.p0_non_game_counts)}",
    )
    logger.info("P0 readiness: %s", format_p0_readiness_summary(p0_readiness))

    logger.info("\\nStep 14: PBP Recovery Alerting...")
    if ctx.relay_recovery_target_ids:
        try:
            with SessionLocal() as session:
                recovered_pbp_ids = (
                    session.execute(
                        select(GamePlayByPlay.game_id)
                        .where(GamePlayByPlay.game_id.in_(list(ctx.relay_recovery_target_ids)))
                        .distinct(),
                    )
                    .scalars()
                    .all()
                )

            failed_ids = set(ctx.relay_recovery_target_ids) - set(recovered_pbp_ids)
            success_count = len(recovered_pbp_ids)
            failed_count = len(failed_ids)

            import csv
            import glob

            report_files = glob.glob(f"logs/daily_update_summary/pbp_report_*_{target_date}.csv")
            attempts_by_game: dict[str, list[dict[str, str]]] = {}
            for file_path in report_files:
                try:
                    with open(file_path, encoding="utf-8") as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            gid = row.get("game_id")
                            if gid:
                                attempts_by_game.setdefault(gid, []).append(row)
                except Exception:
                    logger.exception("Failed to read PBP report file: %s", file_path)

            failed_details = []
            for gid in sorted(failed_ids):
                game_attempts = attempts_by_game.get(gid) or []
                if not game_attempts:
                    failed_details.append(f"- `{gid}`: No logs found")
                    continue

                attempt_summaries = []
                for att in game_attempts:
                    source = att.get("source_name", "unknown")
                    status = att.get("status", "unknown")
                    notes = att.get("notes") or ""

                    if "final_score_mismatch" in notes:
                        notes = "score_mismatch"
                    elif "missing_middle_inning" in notes:
                        notes = "inning_gap"

                    summary = f"*{source}*:{status}"
                    if notes:
                        summary += f" ({notes})"
                    attempt_summaries.append(summary)

                failed_details.append(f"- `{gid}`: " + " -> ".join(attempt_summaries))

            msg = f"*Daily PBP Recovery Report ({target_date})*"
            blocks = [
                {"type": "header", "text": {"type": "plain_text", "text": "Daily PBP Recovery Report"}},
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Target Games:* {len(ctx.relay_recovery_target_ids)}"},
                        {"type": "mrkdwn", "text": f"*Recovered:* {success_count}"},
                        {"type": "mrkdwn", "text": f"*Failed:* {failed_count}"},
                    ],
                },
            ]
            if failed_details:
                failed_text = "\\n".join(failed_details)

                if len(failed_text) > 2900:
                    failed_text = failed_text[:2800] + "\\n... (truncated)"
                blocks.append(
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*Detailed Failures:*\\n{failed_text}",
                        },
                    },
                )
            SlackWebhookClient.send_alert(msg, blocks=blocks)
            logger.info(
                "   Sent PBP recovery summary to Slack (Success: %s, Failed: %s)",
                success_count,
                failed_count,
            )
        except Exception:
            logger.exception("   Error sending PBP recovery summary")
    else:
        logger.info("   No PBP recovery targets for today.")

    logger.info(f"\\n{'=' * 60}")
    logger.info("Daily Finalize Finished for %s", target_date)
    logger.info("Refresh Manifest: %s", manifest_path)
    logger.info("Daily Summary: %s", summary_path)
    logger.info(f"{'=' * 60}\\n")

    return {
        "phase": "postgame_finalize",
        "target_date": target_date,
        "manifest_path": str(manifest_path),
        "summary_path": str(summary_path),
        "stability": stability_summary,
        "p0_readiness": p0_readiness,
    }'''

# Write the new body to a temp file for inspection
with open('_new_body.txt', 'w') as f:
    f.write(new_body)
print(f"New body written to _new_body.txt ({len(new_body)} chars)")

# Now let's reconstruct the entire file
# Part 1: First part up to and including the last line before step functions
part1 = '\n'.join(lines[:insert_after+1])

# Part 2: Read step functions from another temporary file
step_funcs_path = '_step_functions.py'
# We'll create this file separately

# Part 3: After step functions, up to and including the run_update signature
part3 = '\n'.join(lines[insert_after+1:body_start])

# Part 4: The old body (to be replaced)
# Part 5: The rest after run_update
part5 = '\n'.join(lines[run_update_end+1:])

print(f"Part1: {len(part1)} chars, ends with: ...{part1[-40:]}")
print(f"Part3: {len(part3)} chars, starts with: {part3[:40]}...")
print(f"Part5: {len(part5)} chars, starts with: {part5[:40]}...")
