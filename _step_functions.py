
async def _step_0_auto_healer(ctx: _RunContext) -> None:
    if ctx.run_auto_healer:
        logger.info("\n\U0001fa7a Step 0: Running Auto-Healer...")
        ctx.healer_recovery_targets = _collect_past_scheduled_recovery_targets(ctx.today_kst)
        try:
            await run_healer_async(dry_run=False)
        except Exception:
            logger.exception("   \u26a0\ufe0f Auto-Healer encountered an error (continuing anyway)")
            ctx.healer_recovery_targets = []
        if ctx.healer_recovery_targets:
            logger.info("   \u2705 Auto-Healer recovery candidates tracked: %s", len(ctx.healer_recovery_targets))
    else:
        logger.info("\n\U0001fa7a Step 0: Auto-Healer skipped for scoped backfill run.")


async def _step_1_schedule(ctx: _RunContext) -> None:
    logger.info("\n\U0001f4c5 Step 1: Crawling + saving monthly schedule...")
    s_crawler = ScheduleCrawler()
    schedule_games = await s_crawler.crawl_schedule(ctx.year, ctx.month)
    schedule_result = save_schedule_games(
        schedule_games,
        log=logger.info,
        write_contract=ctx.write_contract,
        source_reason=f"monthly_schedule_refresh:{ctx.year}-{ctx.month:02d}",
    )
    logger.info(
        f"   \u2705 Schedule discovered={schedule_result.discovered} "
        f"saved={schedule_result.saved} failed={schedule_result.failed}",
    )

    daily_games = [g for g in schedule_games if str(g.get("game_date", "")).replace("-", "") == ctx.target_date]
    detail_games = [g for g in daily_games if is_detail_candidate_game(g, today=ctx.today_kst)]
    skipped_detail_games = len(daily_games) - len(detail_games)
    if skipped_detail_games:
        logger.info("   \u2139\ufe0f Skipping %s non-detail schedule games", skipped_detail_games)
    if ctx.limit and len(detail_games) > ctx.limit:
        detail_games = detail_games[:ctx.limit]
        logger.info("   [LIMIT] Restricted to first %s games", ctx.limit)
    logger.info("   \u2705 Found %s games for %s", len(daily_games), ctx.target_date)
    ctx.daily_games = daily_games
    ctx.detail_games = detail_games

async def _step_2_detail_crawl(ctx: _RunContext) -> None:
    logger.info("\n\U0001f3ae Step 2: Crawling full postgame details...")
    resolver_session = SessionLocal()
    processed_game_ids_set: set[str] = set()
    for game in ctx.detail_games:
        game_id = normalize_kbo_game_id(game.get("game_id"))
        if not game_id:
            continue
        ctx.detail_games_by_id[game_id] = {
            "game_id": game_id,
            "game_date": str(game.get("game_date") or ctx.target_date),
        }
        ctx.detail_recovery_attempts[game_id] = 0

    queued_recovery_game_count = 0
    for queued_game_id in sorted(ctx.queued_recovery_game_ids):
        if queued_game_id in ctx.detail_games_by_id:
            continue
        if ctx.limit is not None and len(ctx.detail_games_by_id) >= ctx.limit:
            continue
        ctx.detail_games_by_id[queued_game_id] = {
            "game_id": queued_game_id,
            "game_date": ctx.target_date,
        }
        ctx.detail_recovery_attempts[queued_game_id] = 0
        queued_recovery_game_count += 1
    if queued_recovery_game_count > 0:
        logger.info(
            f"   \u267b\ufe0f Re-prioritizing {queued_recovery_game_count} queued detail-recovery game(s)",
        )

    try:
        resolver = PlayerIdResolver(
            resolver_session,
            strict_game_resolution=True,
            allow_auto_register=False,
        )
        resolver.preload_season_index(ctx.year)
        g_crawler = GameDetailCrawler(resolver=resolver)

        collection_result = await crawl_and_save_game_details(
            list(ctx.detail_games_by_id.values()),
            detail_crawler=g_crawler,
            force=True,
            concurrency=1,
            log=logger.info,
            write_contract=ctx.write_contract,
            source_reason=f"postgame_finalize:{ctx.target_date}",
        )
        detail_results_by_game = dict(collection_result.items)
        for game_id in ctx.detail_games_by_id:
            ctx.detail_recovery_attempts[game_id] = ctx.detail_recovery_attempts.get(game_id, 0) + 1

        max_recovery_rounds = max(1, int(DETAIL_RECOVERY_MAX_ROUNDS))
        unrecovered_game_ids = {
            normalize_kbo_game_id(game_id)
            for game_id, item in detail_results_by_game.items()
            if item and not item.detail_saved
        }
        recoverable_failure_ids = {
            game_id
            for game_id in unrecovered_game_ids
            if _is_recoverable_detail_reason(detail_results_by_game[game_id].failure_reason)
        }

        for _ in range(max_recovery_rounds - 1):
            retry_game_ids = sorted(
                game_id
                for game_id in recoverable_failure_ids
                if ctx.detail_recovery_attempts.get(game_id, 0) < max_recovery_rounds
            )
            if not retry_game_ids:
                break

            ctx.detail_recovery_passes += 1
            retry_targets = [ctx.detail_games_by_id[game_id] for game_id in retry_game_ids if game_id in ctx.detail_games_by_id]
            for game_id in retry_game_ids:
                ctx.detail_recovery_attempts[game_id] = ctx.detail_recovery_attempts.get(game_id, 0) + 1

            logger.info("   \U0001f501 Detail recovery pass #%s (%s game(s))", ctx.detail_recovery_passes, len(retry_targets))
            retry_result = await crawl_and_save_game_details(
                retry_targets,
                detail_crawler=g_crawler,
                force=True,
                concurrency=1,
                log=logger.info,
                write_contract=ctx.write_contract,
                source_reason=f"postgame_finalize:{ctx.target_date}:recovery",
            )

            for game_id, item in retry_result.items.items():
                normalized_game_id = normalize_kbo_game_id(game_id)
                detail_results_by_game[normalized_game_id] = item

                if item.detail_saved:
                    if normalized_game_id in unrecovered_game_ids:
                        unrecovered_game_ids.remove(normalized_game_id)
                        ctx.detail_recovered_after_retry += 1
                    recoverable_failure_ids.discard(normalized_game_id)
                elif not _is_recoverable_detail_reason(item.failure_reason):
                    unrecovered_game_ids.discard(normalized_game_id)
                    recoverable_failure_ids.discard(normalized_game_id)

        for game_id, item in detail_results_by_game.items():
            reason = item.failure_reason if item else None
            if item.detail_saved:
                processed_game_ids_set.add(game_id)
                ctx.detail_recovery_queue.mark_detail_recovery_success(ctx.target_date, game_id)
            elif _is_recoverable_detail_reason(reason):
                ctx.detail_recovery_queue.mark_detail_recovery_failure(
                    ctx.target_date,
                    game_id,
                    failure_reason=reason,
                )
            else:
                ctx.detail_recovery_queue.mark_detail_recovery_success(ctx.target_date, game_id)

        ctx.processed_game_ids = sorted(processed_game_ids_set)
        ctx.detail_failure_counts, ctx.detail_failure_game_ids = _failure_reason_summary(detail_results_by_game)

        for game_id in sorted(ctx.detail_games_by_id):
            item = detail_results_by_game.get(game_id)
            if item and item.detail_saved:
                logger.info("   \u2705 Successfully saved %s", game_id)
                continue

            reason = item.failure_reason if item else "exception"
            if item and item.detail_status == "save_failed":
                logger.error("   \u274c Failed to save details for %s to local DB", game_id)
            else:
                logger.warning(f"   \u26a0\ufe0f Could not fetch details for {game_id} (reason={reason or 'unknown'})")

            ctx.detail_still_missing.add(game_id)
            if ctx.detail_recovery_attempts.get(game_id, 0) >= DETAIL_RECOVERY_RETRY_ALERT_THRESHOLD + 1:
                ctx.detail_retry_escalation_game_ids.append(game_id)

            fallback = _failure_status(ctx.target_date, reason, ctx.today_kst)
            if fallback:
                with SessionLocal() as status_check_session:
                    current_game = (
                        status_check_session.query(Game)
                        .filter(Game.game_id == normalize_kbo_game_id(game_id))
                        .one_or_none()
                    )
                    if (
                        current_game
                        and current_game.game_status in {GAME_STATUS_CANCELLED, "POSTPONED"}
                        and fallback != GAME_STATUS_CANCELLED
                    ):
                        logger.info(
                            f"   \u2139\ufe0f Preservation: Keeping terminal status '{current_game.game_status}' for {game_id}",
                        )
                    else:
                        update_game_status(game_id, fallback)

        logger.info(
            f"   \u2705 Detail result success={len(ctx.processed_game_ids)} failed={len(ctx.detail_still_missing)} "
            f"recovery_passes={ctx.detail_recovery_passes}",
        )
        if ctx.detail_failure_counts:
            logger.info("   \u2139\ufe0f Detail failure reasons: %s", _format_counts(ctx.detail_failure_counts))
        if ctx.detail_recovery_passes:
            logger.info(
                f"   \u2139\ufe0f Detail recovery recovered_after_retry={ctx.detail_recovered_after_retry}, "
                f"still_missing={len(ctx.detail_still_missing)}, escalated={len(ctx.detail_retry_escalation_game_ids)}",
            )

        if ctx.detail_retry_escalation_game_ids:
            try:
                SlackWebhookClient.send_alert(
                    "\u26a0\ufe0f Detail recovery repeated failures: "
                    f"target_date={ctx.target_date} threshold={DETAIL_RECOVERY_RETRY_ALERT_THRESHOLD} "
                    f"game_ids={','.join(sorted(set(ctx.detail_retry_escalation_game_ids)))}",
                )
            except Exception:
                logger.exception("   \u274c Failed to send detail recovery escalation alert")

        if ctx.run_postgame_reconciliation:
            reconcile_start = (
                datetime.strptime(ctx.target_date, "%Y%m%d") - timedelta(days=max(0, ctx.postgame_reconcile_lookback_days))
            ).strftime("%Y%m%d")
            logger.info("\n\U0001f9e9 Step 2.5: Reconciling recently started games (%s~%s)...", reconcile_start, ctx.target_date)
            reconciliation_result = await reconcile_postgame_range(
                reconcile_start,
                ctx.target_date,
                detail_crawler=g_crawler,
                concurrency=1,
                log=logger.info,
                write_contract=ctx.write_contract,
                source_reason=f"postgame_reconciliation:{reconcile_start}-{ctx.target_date}",
            )
            ctx.reconciliation_changed_ids = reconciliation_result.changed_game_ids
            ctx.reconciliation_dates = sorted({change.game_date for change in reconciliation_result.changes})
            logger.info(
                f"   \u2705 candidates={reconciliation_result.candidates} changed={len(reconciliation_result.changes)}",
            )
            if reconciliation_result.changes:
                for line in format_reconciliation_report(reconciliation_result.changes).splitlines():
                    logger.info("   %s", line)
        else:
            logger.info("\n\U0001f9e9 Step 2.5: Postgame reconciliation skipped.")
    except Exception:
        logger.exception("   \u274c Error processing daily details")
        if ctx.detail_games:
            ctx.detail_failure_counts["exception"] = ctx.detail_failure_counts.get("exception", 0) + len(ctx.detail_games)
            ctx.detail_failure_game_ids.setdefault("exception", []).extend(
                str(game["game_id"]) for game in ctx.detail_games if game.get("game_id")
            )
            ctx.detail_still_missing.update(str(game["game_id"]) for game in ctx.detail_games if game.get("game_id"))
        for game in ctx.detail_games:
            game_id = game["game_id"]
            fallback = _failure_status(ctx.target_date, "exception", ctx.today_kst)
            if fallback:
                update_game_status(game_id, fallback)
    finally:
        resolver_session.close()

async def _step_3_refresh_status(ctx: _RunContext) -> None:
    logger.info("\n\U0001f9ed Step 3: Refreshing game status for target date...")
    status_result = refresh_game_status_for_date(ctx.target_date, today=ctx.today_kst)
    ctx.status_refresh_game_ids = [
        normalized for game_id in status_result.get("game_ids", []) if (normalized := normalize_kbo_game_id(game_id))
    ]
    logger.info(
        "   \u2705 "
        f"total={status_result.get('total', 0)} "
        f"updated={status_result.get('updated', 0)} "
        f"counts={status_result.get('status_counts', {})}",
    )


async def _step_4_relay_recovery(ctx: _RunContext) -> None:
    logger.info("\n\U0001f4dd Step 4: Relay recovery (events / PBP)...")
    try:
        relay_game_ids = sorted(set(ctx.processed_game_ids) | set(ctx.reconciliation_changed_ids))
        if relay_game_ids:
            ctx.relay_recovery_target_ids.update(relay_game_ids)
            logger.info("   \u2139\ufe0f Relay candidates=%s", len(relay_game_ids))
            ctx.runner(
                [
                    "scripts/fetch_kbo_pbp.py",
                    "--game-ids",
                    ",".join(relay_game_ids),
                    "--include-incomplete",
                    "--report-out",
                    f"logs/daily_update_summary/pbp_report_daily_{ctx.target_date}.csv",
                ],
            )
        else:
            logger.info("   \u2139\ufe0f No detail-success relay candidates for target date")

        healer_ids_by_date: dict[str, set[str]] = {}
        for item in ctx.healer_recovery_targets:
            game_id = item["game_id"]
            if game_id in relay_game_ids:
                continue
            healer_ids_by_date.setdefault(item["game_date"], set()).add(game_id)
        for recovery_date in sorted(healer_ids_by_date):
            healer_ids = sorted(healer_ids_by_date[recovery_date])
            ctx.relay_recovery_target_ids.update(healer_ids)
            ctx.runner(
                [
                    "scripts/fetch_kbo_pbp.py",
                    "--game-ids",
                    ",".join(healer_ids),
                    "--include-incomplete",
                    "--report-out",
                    f"logs/daily_update_summary/pbp_report_healer_{ctx.target_date}.csv",
                ],
            )
        logger.info("   \u2705 Relay recovery complete")
    except Exception:
        logger.exception("   \u274c Error generating relay events")


async def _step_4_5_proactive_relay(ctx: _RunContext) -> None:
    logger.info("\n\U0001f50d Step 4.5: Proactive Relay Recovery (Last 30 days)...")
    try:
        with SessionLocal() as session:
            thirty_days_ago = datetime.now(KST).date() - timedelta(days=30)

            valid_wpa_event_ids = (
                select(GameEvent.game_id)
                .where(
                    GameEvent.wpa.isnot(None),
                    GameEvent.win_expectancy_before.isnot(None),
                    GameEvent.win_expectancy_after.isnot(None),
                    GameEvent.home_score.isnot(None),
                    GameEvent.away_score.isnot(None),
                    GameEvent.outs.isnot(None),
                    or_(GameEvent.base_state.isnot(None), GameEvent.bases_after.isnot(None)),
                )
                .distinct()
            )

            stmt = (
                select(Game.game_id)
                .where(
                    Game.game_date >= thirty_days_ago,
                    Game.game_date <= datetime.now(KST).date(),
                    Game.game_status.in_(["COMPLETED", "DRAW", GAME_STATUS_UNRESOLVED, GAME_STATUS_SCHEDULED]),
                )
                .where(
                    or_(
                        ~Game.game_id.in_(select(GamePlayByPlay.game_id).distinct()),
                        ~Game.game_id.in_(select(GameEvent.game_id).distinct()),
                        ~Game.game_id.in_(valid_wpa_event_ids),
                    ),
                )
            )
            missing_relay_game_ids = session.execute(stmt).scalars().all()

            if missing_relay_game_ids:
                logger.info(
                    f"   \u26a0\ufe0f Found {len(missing_relay_game_ids)} games missing PBP/event/WPA data. Attempting recovery...",
                )
                to_recover = [gid for gid in missing_relay_game_ids if gid not in ctx.relay_recovery_target_ids]
                if to_recover:
                    ctx.runner(
                        [
                            "scripts/fetch_kbo_pbp.py",
                            "--game-ids",
                            ",".join(to_recover),
                            "--include-incomplete",
                            "--report-out",
                            f"logs/daily_update_summary/pbp_report_proactive_{ctx.target_date}.csv",
                        ],
                    )
                    ctx.relay_recovery_target_ids.update(to_recover)
                    logger.info("   \u2705 Proactive recovery initiated for %s games", len(to_recover))
                else:
                    logger.info("   \u2139\ufe0f Missing games already covered in Step 4")
            else:
                logger.info("   \u2705 No missing PBP/event/WPA data detected in recent games")
    except Exception:
        logger.exception("   \u274c Error in proactive relay recovery")


async def _step_5_content_generation(ctx: _RunContext) -> None:
    ctx.freshness_dates = sorted(
        {ctx.target_date} | set(ctx.reconciliation_dates) | {item["game_date"] for item in ctx.healer_recovery_targets},
    )

    logger.info("\n\U0001f4dd Step 5: Post-game review/WPA generation...")
    try:
        for f_date in ctx.freshness_dates:
            review_args = ["-m", "src.cli.daily_review_batch", "--date", f_date, "--no-sync"]
            ctx.runner(review_args)
        logger.info("   \u2705 Review context generation complete")
    except Exception:
        logger.exception("   \u274c Error generating review context")

    logger.info("\n\U0001f3ac Step 5.2: Daily highlight generation...")
    try:
        for f_date in ctx.freshness_dates:
            highlight_args = ["-m", "src.cli.daily_highlight_batch", "--date", f_date, "--no-sync"]
            ctx.runner(highlight_args)
        logger.info("   \u2705 Daily highlight generation complete")
    except Exception:
        logger.exception("   \u274c Error generating daily highlights")

    logger.info("\n\U0001f4da Step 5.5: LLM-ready game story generation...")
    try:
        for f_date in ctx.freshness_dates:
            story_args = ["-m", "src.cli.daily_story_batch", "--date", f_date, "--no-sync"]
            ctx.runner(story_args)
        logger.info("   \u2705 Game story generation complete")
    except Exception:
        logger.exception("   \u274c Error generating game stories")


async def _step_6_player_stats(ctx: _RunContext) -> None:
    logger.info("\n\U0001f4c8 Step 6: Updating cumulative player stats...")
    if ctx.skip_season_stats:
        logger.info("   \u23ed\ufe0f Season stats update skipped by operator flag")
        return

    active_series = sorted({g.get("season_type", "regular") for g in ctx.daily_games if g.get("season_type")})
    if not active_series:
        active_series = ["regular"]

    logger.info("   \U0001f50d Active series detected: %s", active_series)

    try:
        for series_key in active_series:
            logger.info("   [%s] Updating Batting Stats...", series_key)
            await asyncio.to_thread(
                crawl_series_batting_stats,
                year=ctx.year,
                series_key=series_key,
                save_to_db=True,
                headless=ctx.headless,
                limit=ctx.limit,
            )
            logger.info("   [%s] Updating Pitching Stats...", series_key)
            await asyncio.to_thread(
                crawl_pitcher_series,
                year=ctx.year,
                series_key=series_key,
                save_to_db=True,
                headless=ctx.headless,
                limit=ctx.limit,
            )
        logger.info("   \u2705 Local cumulative stats for %s %s series updated", ctx.year, active_series)
    except Exception:
        logger.exception("   \u274c Error during stats update")


async def _step_6_5_maintenance(ctx: _RunContext) -> None:
    logger.info("\n\U0001fa79 Step 6.5: Backfilling starting pitchers from stats...")
    try:
        backfill_args = [
            "-m",
            "src.cli.backfill_starting_pitchers_from_stats",
            "--start-date",
            ctx.target_date,
            "--end-date",
            ctx.target_date,
        ]
        if ctx.sync:
            backfill_args.append("--sync")
        ctx.runner(backfill_args)
        logger.info("   \u2705 Starting pitcher backfill complete")
    except Exception:
        logger.exception("   \u274c Error during pitcher backfill")

    logger.info("\n\U0001f575\ufe0f  Step 6.6: Auditing season stats vs transactional details (Auto-remediation)...")
    try:
        audit_cmd = ["scripts/verification/audit_fallback_stats.py", "--year", str(ctx.year), "--type", "all"]
        if ctx.fix:
            audit_cmd.append("--fix")
        ctx.runner(audit_cmd)
        logger.info("   \u2705 Statistical audit and auto-remediation complete")
    except Exception:
        logger.exception("   \u26a0\ufe0f Statistical audit/fix found issues (see logs)")


async def _step_7_rosters(ctx: _RunContext) -> None:
    ctx.r_target_date = datetime.strptime(ctx.target_date, "%Y%m%d").strftime("%Y-%m-%d")

    logger.info("\n\U0001f504 Step 7: Updating player movements and daily rosters...")
    try:
        m_crawler = PlayerMovementCrawler()
        movements = await m_crawler.crawl_years(ctx.year, ctx.year)
        if movements:
            m_repo = PlayerRepository()
            m_count = m_repo.save_player_movements(movements)
            logger.info("   \u2705 Saved %s player movements for %s", m_count, ctx.year)

        r_crawler = DailyRosterCrawler()
        rosters = await r_crawler.crawl_date_range(ctx.r_target_date, ctx.r_target_date)
        if rosters:
            with SessionLocal() as session:
                r_repo = TeamRepository(session)
                r_count = r_repo.save_daily_rosters(rosters)
                logger.info("   \u2705 Saved %s daily roster records for %s", r_count, ctx.r_target_date)

        rt_crawler = RosterTransactionCrawler()
        roster_transactions = await rt_crawler.run(save=True, target_date=ctx.r_target_date)
        ctx.p0_non_game_counts["roster_transactions"] = len(roster_transactions)
        logger.info("   \u2705 Roster transactions checked for %s: %s rows", ctx.r_target_date, len(roster_transactions))
    except Exception:
        logger.exception("   \u274c Error updating player movements/rosters")
        ctx.p0_non_game_errors["roster_transactions"] = "roster_pipeline_failed"


async def _step_7_5_p0_non_game(ctx: _RunContext) -> None:
    logger.info("\n\U0001f39f\ufe0f Step 7.5: Updating P0 non-game events and tickets...")
    if ctx.run_p0_non_game:
        try:
            event_crawler = TeamEventCrawler(days_back=3)
            team_events = await event_crawler.run(save=True)
            ctx.p0_non_game_counts["team_events"] = len(team_events)
            logger.info("   \u2705 Team events checked: %s rows", len(team_events))
        except Exception as exc:
            logger.exception("   \u26a0\ufe0f Team event crawler failed")
            ctx.p0_non_game_errors["team_events"] = str(exc) or exc.__class__.__name__

        try:
            ticket_crawler = TicketCrawler()
            ticket_prices = await ticket_crawler.run(save=True, season=ctx.year)
            ctx.p0_non_game_counts["ticket_prices"] = len(ticket_prices)
            logger.info("   \u2705 Ticket prices checked for %s: %s rows", ctx.year, len(ticket_prices))
        except Exception as exc:
            logger.exception("   \u26a0\ufe0f Ticket crawler failed")
            ctx.p0_non_game_errors["ticket_prices"] = str(exc) or exc.__class__.__name__
    else:
        logger.info("   \u23ed\ufe0f P0 non-game event/ticket crawlers skipped by operator flag")
        ctx.p0_non_game_counts["skipped"] = 1


async def _step_8_derived_stats(ctx: _RunContext) -> None:
    logger.info("\n\U0001f4ca Step 8: Rebuilding derived standings...")
    try:
        ctx.runner(["-m", "src.cli.calculate_standings", "--year", str(ctx.year)])
        ctx.derived_refresh.append("standings")
    except Exception:
        logger.exception("   \u274c Error calculating standings")

    logger.info("\n\U0001f9ee Step 9: Recalculating matchup splits...")
    try:
        ctx.runner(["-m", "src.cli.calculate_matchups", "--year", str(ctx.year)])
        ctx.derived_refresh.append("matchups")
        logger.info("   \u2705 Matchup splits recalculated successfully")
    except Exception:
        logger.exception("   \u274c Error recalculating matchups")

    logger.info("\n\U0001f3f7\ufe0f Step 10: Recalculating stat rankings...")
    try:
        ctx.runner(["-m", "src.cli.calculate_rankings", "--year", str(ctx.year)])
        ctx.derived_refresh.append("stat_rankings")
        logger.info("   \u2705 Stat rankings recalculated successfully")
    except Exception:
        logger.exception("   \u274c Error recalculating stat rankings")

    logger.info("\n\U0001f4c8 Step 10.6: Calculating advanced Sabermetrics (wOBA, wRC+, WAR)...")
    try:
        ctx.runner(["-m", "src.cli.calculate_sabermetrics", "--years", str(ctx.year)])
        logger.info("   \u2705 Sabermetrics engine completed successfully")
    except Exception:
        logger.exception("   \u274c Error calculating Sabermetrics")


async def _step_10_7_enrichment(ctx: _RunContext) -> None:
    logger.info("\n\U0001f3ad Step 10.7: Enriching new player profiles (fetching missing photos/details)...")
    try:
        ctx.runner(["scripts/backfill_player_profiles.py", "--limit", "0", "--delay", "1.0"])
        logger.info("   \u2705 Player profile enrichment complete")
    except Exception:
        logger.exception("   \u26a0\ufe0f Profile enrichment found issues (continning)")

    logger.info("\n\U0001f575\ufe0f  Step 10.8: Deep statistical logic audit (cross-table invariants)...")
    try:
        from scripts.verification.audit_game_logic import audit_game_logic

        violations = audit_game_logic(year=ctx.year)

        if violations:
            inconsistent_ids = sorted({v["game_id"] for v in violations})
            logger.warning("   \u26a0\ufe0f  Audit found %s inconsistencies in %s games.", len(violations), len(inconsistent_ids))
            logger.info(f"   \U0001f680 Triggering targeted self-healing for: {', '.join(inconsistent_ids[:5])}...")

            await run_healer_async(target_game_ids=inconsistent_ids)

            logger.info("   \U0001f50d Re-auditing after repair...")
            violations_after = audit_game_logic(year=ctx.year)
            if not violations_after:
                logger.info("   \u2705 All inconsistencies resolved automatically.")
            else:
                remaining_ids = sorted({v["game_id"] for v in violations_after})
                logger.error(
                    f"   \u274c {len(violations_after)} inconsistencies still remain in {len(remaining_ids)} games.",
                )
        else:
            logger.info("   \u2705 Deep statistical logic audit complete (No issues found)")
    except Exception:
        logger.exception("   \u26a0\ufe0f  Deep statistical audit/heal process failed")


async def _step_11_sync_pipeline(ctx: _RunContext) -> None:
    ctx.candidate_sync_game_ids = sorted(
        {game["game_id"] for game in ctx.daily_games}
        | set(ctx.status_refresh_game_ids)
        | set(ctx.processed_game_ids)
        | set(ctx.reconciliation_changed_ids)
        | {item["game_id"] for item in ctx.healer_recovery_targets}
        | ctx.relay_recovery_target_ids,
    )

    if not ctx.sync:
        return

    logger.info("\n\U0001f9ea Step 11: Freshness gate before OCI publish...")
    freshness_ok = True
    for freshness_date in ctx.freshness_dates:
        try:
            ctx.runner(["-m", "src.cli.freshness_gate", "--date", freshness_date])
        except subprocess.CalledProcessError:
            freshness_ok = False
            logger.exception("   \u26a0\ufe0f Freshness gate found issues for %s (continuing)", freshness_date)
    if freshness_ok:
        logger.info("   \u2705 Freshness gate passed")

    logger.info("\n\U0001f575\ufe0f  Step 11.5: Local game status integrity audit...")
    try:
        _run_game_status_integrity_audit()
        logger.info("   \u2705 Local integrity audit passed")
    except Exception as exc:
        logger.exception("   \u274c Local integrity audit FAILED: %s", exc)
        raise RuntimeError("Aborting OCI sync due to local data integrity violations.") from exc

    logger.info("\n\u2696\ufe0f Step 12: Statistical quality gate check...")
    try:
        ctx.runner(["-m", "src.cli.quality_gate_check", "--year", str(ctx.year)])
        logger.info("   \u2705 Statistical quality gate passed")
    except subprocess.CalledProcessError as exc:
        reason = "non_p0_statistical_quality_gate_failed"
        ctx.non_p0_quality_gate_counts[reason] = ctx.non_p0_quality_gate_counts.get(reason, 0) + 1
        ctx.non_p0_quality_gate_ids.setdefault(reason, []).append(f"season:{ctx.year}")
        logger.exception("   \u26a0\ufe0f Non-P0 statistical quality gate failed (continuing OCI game publish): %s", exc)

    logger.info("\n\u2601\ufe0f Step 13: Synchronizing to OCI...")
    oci_url = os.getenv("OCI_DB_URL")
    if not oci_url:
        raise RuntimeError("OCI_DB_URL is required when --sync is enabled")

    with SessionLocal() as sync_session:
        syncer = OCISync(oci_url, sync_session)
        try:
            logger.info("   \U0001f6e1\ufe0f Syncing players/basic first to satisfy FK constraints...")
            syncer.sync_player_basic()
            syncer.sync_players()

            for game_id in ctx.candidate_sync_game_ids:
                sync_result = syncer.sync_specific_game(game_id)
                _merge_oci_skip_summary(ctx.oci_skip_counts, ctx.oci_skip_game_ids, sync_result, game_id)

            if ctx.skip_oci_supporting_sync:
                ctx.oci_skip_counts["oci_supporting_sync_skipped"] = (
                    ctx.oci_skip_counts.get("oci_supporting_sync_skipped", 0) + 1
                )
                ctx.oci_skip_game_ids.setdefault("oci_supporting_sync_skipped", []).append(f"season:{ctx.year}")
                logger.info("   \u23ed\ufe0f Non-P0 OCI supporting dataset sync skipped by operator flag")
            else:
                try:
                    syncer.sync_games(filters=[Game.game_id.like(f"{ctx.year}%")])
                    syncer.sync_standings(year=ctx.year)
                    syncer.sync_matchups(year=ctx.year)
                    syncer.sync_stat_rankings(year=ctx.year)
                    syncer.sync_player_season_batting(year=ctx.year)
                    syncer.sync_player_season_pitching(year=ctx.year)
                    syncer.sync_player_movements()
                    syncer.sync_daily_rosters(start_date=ctx.r_target_date, end_date=ctx.r_target_date)
                except Exception:
                    logger.exception("   \u26a0\ufe0f Non-P0 OCI supporting dataset sync failed")
                    ctx.oci_skip_counts["non_p0_supporting_sync_failed"] = (
                        ctx.oci_skip_counts.get("non_p0_supporting_sync_failed", 0) + 1
                    )
                    ctx.oci_skip_game_ids.setdefault("non_p0_supporting_sync_failed", []).append(f"season:{ctx.year}")
            if ctx.oci_skip_counts:
                logger.info("   \u2139\ufe0f OCI skip summary: %s", _format_counts(ctx.oci_skip_counts))
        finally:
            syncer.close()

    logger.info("\n\U0001f9ea Step 13.5: Freshness gate after OCI publish...")
    for freshness_date in ctx.freshness_dates:
        try:
            ctx.runner(["-m", "src.cli.freshness_gate", "--date", freshness_date, "--source-url-env", "OCI_DB_URL"])
        except subprocess.CalledProcessError:
            logger.exception("   \u26a0\ufe0f OCI freshness gate found issues for %s (continuing)", freshness_date)

    logger.info("\n\u2696\ufe0f Step 13.6: OCI parity quality gate check...")
    try:
        _run_oci_parity_quality_gate()
        logger.info("   \u2705 OCI parity check complete")
    except Exception:
        logger.exception("OCI parity quality gate failed")
        reason = "non_p0_oci_parity_quality_gate_failed"
        ctx.non_p0_quality_gate_counts[reason] = ctx.non_p0_quality_gate_counts.get(reason, 0) + 1
        ctx.non_p0_quality_gate_ids.setdefault(reason, []).append("oci")


async def _step_14_tomorrow_preview(ctx: _RunContext) -> None:
    if ctx.seed_tomorrow_preview:
        tomorrow_date = (datetime.strptime(ctx.target_date, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")
        logger.info("\n\U0001f52e Step 14: Seeding tomorrow preview contexts (%s)...", tomorrow_date)
        try:
            preview_args = ["-m", "src.cli.daily_preview_batch", "--date", tomorrow_date]
            if not ctx.sync:
                preview_args.append("--no-sync")
            ctx.runner(preview_args)
            logger.info("   \u2705 Tomorrow preview seed complete")
        except Exception:
            logger.exception("   \u274c Error generating tomorrow preview seed")
