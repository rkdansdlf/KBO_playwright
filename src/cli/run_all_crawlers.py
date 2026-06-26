"""CLI entrypoint and scheduler daemon to execute KBO crawlers, transform text, generate embeddings, and load to database."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from apscheduler.schedulers.blocking import BlockingScheduler
from dotenv import load_dotenv
from sqlalchemy.exc import SQLAlchemyError

from src.cli.verify_sync_consistency import run_consistency_audit

# Load environment variables
from src.constants import KST
from src.crawlers.dynamic_data_crawler import DynamicDataCrawler
from src.crawlers.realtime_issue_crawler import RealtimeIssueCrawler
from src.crawlers.static_text_crawler import StaticTextCrawler
from src.db.engine import get_db_session, get_oci_url
from src.parsers.text_transformer import TextTransformer
from src.repositories.rag_chunk_repository import RagChunkRepository
from src.services.embedding_service import EmbeddingService
from src.utils.alerting import SlackWebhookClient

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

load_dotenv()

logger = logging.getLogger(__name__)
PIPELINE_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, OSError)
FILE_READ_EXCEPTIONS = (OSError, UnicodeError)

# Directory-to-category mapping for Docs/baseball subdirectories
_CATEGORY_MAP: dict[str, str] = {
    "baseball_rules": "game_rules",
    "glossary": "glossary",
    "bylaws": "bylaws",
    "league_regulations": "league_regulations",
    "player_regulations": "player_regulations",
    "scoring_rules": "scoring_rules",
    "disciplinary_regulations": "disciplinary_regulations",
    "supplementary_regulations": "supplementary_regulations",
    "kbo_knowledge": "kbo_knowledge",
    "kbo_rulebook": "rulebook",
}


def enrich_and_prepare_contents(all_chunks: list[dict[str, Any]]) -> list[str]:
    """Enriches chunk metadata using LLM and prepares content strings for vector embedding."""
    from src.services.metadata_enrichment_service import MetadataEnrichmentService

    enrich_svc = MetadataEnrichmentService()

    if enrich_svc.enabled:
        logger.info("✨ LLM Metadata Enrichment enabled. Processing %s chunks...", len(all_chunks))
        for idx, chunk in enumerate(all_chunks):
            logger.info("   [%s/%s] Analyzing chunk...", idx + 1, len(all_chunks))
            enrichment = enrich_svc.enrich_chunk(chunk["content"])
            chunk["meta"].update(enrichment)

    # Form content strings for embedding
    contents_to_embed = []
    for c in all_chunks:
        meta = c["meta"]
        if meta.get("keywords") or meta.get("questions"):
            kw_str = ", ".join(meta.get("keywords", []))
            q_str = " ".join(meta.get("questions", []))
            enriched_text = (
                f"Summary: {meta.get('summary', '')}\nKeywords: {kw_str}\nQuestions: {q_str}\n\nContent: {c['content']}"
            )
            contents_to_embed.append(enriched_text)
        else:
            contents_to_embed.append(c["content"])

    return contents_to_embed


def _markdown_category(path_parts: list[str]) -> tuple[str, str | None]:
    category = "rulebook"
    subcategory = None
    for part in path_parts[:-1]:
        mapped = _CATEGORY_MAP.get(part)
        if mapped:
            if category == "rulebook":
                category = mapped
            else:
                subcategory = mapped
    return category, subcategory


def _markdown_title(content: str, file: str) -> str:
    first_line = content.lstrip().split("\n")[0]
    if first_line.startswith("#"):
        return first_line.lstrip("#").strip()
    return Path(file).stem.replace("_", " ").title()


def _load_local_markdown_docs(rules_dir: str = "Docs/baseball") -> list[dict[str, Any]]:
    if not Path(rules_dir).exists():
        return []
    logger.info("📁 Scanning directory '%s' for static markdown files...", rules_dir)
    raw_docs = []
    for root, _, files in os.walk(rules_dir):
        for file in files:
            if not file.endswith(".md"):
                continue
            full_path = Path(root, file)
            try:
                with full_path.open(encoding="utf-8") as f:
                    content = f.read()
                rel_path = os.path.relpath(full_path, rules_dir)
                category, subcategory = _markdown_category(rel_path.replace("\\", "/").split("/"))
                raw_docs.append(
                    {
                        "title": _markdown_title(content, file),
                        "content": content,
                        "meta": {
                            "source": full_path,
                            "source_file": file,
                            "crawled_at": datetime.now(KST).isoformat(),
                            "category": category,
                            **({"subcategory": subcategory} if subcategory else {}),
                        },
                    },
                )
            except FILE_READ_EXCEPTIONS:
                logger.exception("⚠️ Error reading local markdown %s", file)
    return raw_docs


async def _crawl_static_docs(crawler: StaticTextCrawler, pdf_path: str | None) -> list[dict[str, Any]]:
    raw_docs = []
    if pdf_path:
        if Path(pdf_path).exists():
            raw_docs.extend(crawler.parse_local_pdf(pdf_path))
        else:
            logger.warning("⚠️ Specified PDF path does not exist: %s", pdf_path)
    raw_docs.extend(_load_local_markdown_docs())

    for url in ["https://namu.wiki/w/KBO%20%EB%A6%AC%EA%B7%B8"]:
        try:
            raw_docs.append(await crawler.crawl_namuwiki(url))
        except PIPELINE_EXCEPTIONS:
            logger.exception("⚠️ Namuwiki crawl skipped/failed for %s", url)
    return raw_docs


def _chunk_static_docs(transformer: TextTransformer, raw_docs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    logger.info("🔄 Cleansing and chunking %s documents...", len(raw_docs))
    all_chunks = []
    for doc in raw_docs:
        all_chunks.extend(transformer.chunk_document(doc))
    logger.info("   Generated %s semantic chunks.", len(all_chunks))
    return all_chunks


def _embed_and_save_static_chunks(
    embedding_svc: EmbeddingService,
    repo: RagChunkRepository,
    all_chunks: list[dict[str, Any]],
) -> None:
    if not all_chunks:
        return
    logger.info("⚡ Fetching vector embeddings for %s chunks...", len(all_chunks))
    embeddings = embedding_svc.get_embeddings_batch(enrich_and_prepare_contents(all_chunks))
    for idx, emb in enumerate(embeddings):
        all_chunks[idx]["embedding"] = emb

    logger.info("💾 Saving chunks to local database...")
    with get_db_session() as session:
        upserted = repo.upsert_chunks(session, all_chunks)
        logger.info("✅ Upserted %s RAG chunks to local DB.", upserted)
        if os.getenv("RUN_SYNC_SUPABASE") == "1" or os.getenv("RUN_SYNC_OCI") == "1":
            _sync_static_chunks_to_oci(session)


def _sync_static_chunks_to_oci(session: Session) -> None:
    logger.info("🚚 Syncing new static RAG chunks to OCI...")
    from src.sync.oci_sync import OCISync

    oci_url = get_oci_url()
    if not oci_url:
        return
    syncer = OCISync(oci_url, session)
    try:
        logger.info("✅ Synced %s static RAG chunks to OCI.", syncer.sync_rag_chunks())
    finally:
        syncer.close()


async def run_static_pipeline(pdf_path: str | None = None) -> None:
    """Runs extraction, chunking, and embedding for static rulebooks and wikis."""
    logger.info("\n🏁 Starting Static Text Pipeline...")

    crawler = StaticTextCrawler()
    transformer = TextTransformer()
    embedding_svc = EmbeddingService()
    repo = RagChunkRepository()

    raw_docs = await _crawl_static_docs(crawler, pdf_path)
    if not raw_docs:
        logger.info("ℹ️ No static documents found to process.")
        return
    _embed_and_save_static_chunks(embedding_svc, repo, _chunk_static_docs(transformer, raw_docs))


async def run_dynamic_pipeline() -> None:
    """Runs extraction and DB updates for schedules, rosters, and ticket times."""
    logger.info("\n🏁 Starting Dynamic Data Pipeline...")

    with get_db_session() as session:
        crawler = DynamicDataCrawler(session)

        # 1. Update/Calculate ticket open schedules (next 14 days)
        crawler.crawl_and_update_ticket_times(lookahead_days=14)

        # 2. Crawl player rosters changes for today and yesterday
        today_str = datetime.now(KST).strftime("%Y-%m-%d")
        yesterday_str = (datetime.now(KST) - timedelta(days=1)).strftime("%Y-%m-%d")
        try:
            roster_records = await crawler.crawl_roster_changes(start_date=yesterday_str, end_date=today_str)
            # Save roster records to team_daily_roster table using repository to resolve player_basic_id/person_type
            from src.repositories.team_repository import TeamRepository

            r_repo = TeamRepository(session)
            inserted_count = r_repo.save_daily_rosters(roster_records)
            logger.info("✅ Dynamic rosters updated successfully (%s records processed).", inserted_count)
        except PIPELINE_EXCEPTIONS:
            logger.exception("⚠️ Roster crawler execution failure")

        # 3. Automatically sync to OCI if config allows
        if os.getenv("RUN_SYNC_SUPABASE") == "1" or os.getenv("RUN_SYNC_OCI") == "1":
            logger.info("🚚 Syncing ticket schedules and daily rosters to OCI...")
            from src.sync.oci_sync import OCISync

            oci_url = get_oci_url()
            if oci_url:
                syncer = OCISync(oci_url, session)
                try:
                    synced_tickets = syncer.sync_ticket_schedules()
                    synced_rosters = syncer.sync_daily_rosters()
                    logger.info("✅ Synced %s ticket schedules and %s rosters to OCI.", synced_tickets, synced_rosters)
                finally:
                    syncer.close()


async def run_realtime_pipeline() -> None:
    """Runs news and community thread crawler, transforms text, embeds and loads."""
    logger.info("\n🏁 Starting Realtime Issue Pipeline...")

    crawler = RealtimeIssueCrawler()
    transformer = TextTransformer()
    embedding_svc = EmbeddingService()
    repo = RagChunkRepository()

    raw_docs = []

    # 1. Fetch Naver news headlines
    try:
        news_docs = crawler.fetch_naver_news_headlines()
        raw_docs.extend(news_docs)
    except PIPELINE_EXCEPTIONS:
        logger.exception("⚠️ Naver news crawler error")

    # 2. Fetch MLBPark bullpen threads
    try:
        forum_docs = crawler.fetch_mlbpark_bullpen_posts()
        raw_docs.extend(forum_docs)
    except PIPELINE_EXCEPTIONS:
        logger.exception("⚠️ MLBPark crawler error")

    if not raw_docs:
        logger.info("ℹ_ No realtime news or forum documents found to process.")
        return

    # 3. Transform & Chunk
    logger.info("🔄 Cleansing and chunking %s articles...", len(raw_docs))
    all_chunks = []
    for doc in raw_docs:
        chunks = transformer.chunk_document(doc)
        all_chunks.extend(chunks)
    logger.info("   Generated %s news chunks.", len(all_chunks))

    # 4. Generate Embeddings & Load to Database
    if all_chunks:
        logger.info("⚡ Fetching vector embeddings for %s chunks...", len(all_chunks))
        contents_to_embed = enrich_and_prepare_contents(all_chunks)
        embeddings = embedding_svc.get_embeddings_batch(contents_to_embed)

        for idx, emb in enumerate(embeddings):
            all_chunks[idx]["embedding"] = emb

        logger.info("💾 Saving chunks to local database...")
        with get_db_session() as session:
            upserted = repo.upsert_chunks(session, all_chunks)
            logger.info("✅ Upserted %s realtime RAG chunks to local DB.", upserted)

            # Automatically sync to OCI if config allows
            if os.getenv("RUN_SYNC_SUPABASE") == "1" or os.getenv("RUN_SYNC_OCI") == "1":
                logger.info("🚚 Syncing realtime RAG chunks to OCI...")
                from src.sync.oci_sync import OCISync

                oci_url = get_oci_url()
                if oci_url:
                    syncer = OCISync(oci_url, session)
                    try:
                        synced = syncer.sync_rag_chunks()
                        logger.info("✅ Synced %s realtime RAG chunks to OCI.", synced)
                    finally:
                        syncer.close()


def run_consistency_check(*, deep: bool = False) -> None:
    """
    Runs a post-sync consistency audit between local SQLite and OCI.

    Sends an alert if mismatches are found. Skips silently if OCI is not configured.
    """
    oci_url = get_oci_url()
    if not oci_url:
        logger.info("ℹ️  OCI URL not configured — skipping consistency check.")
        return

    logger.info("\n🔍 Running post-sync consistency audit...")
    try:
        success = run_consistency_audit(deep=deep, trigger_alert=True)
        if success:
            logger.info("✅ Consistency audit passed — databases are in sync.")
        else:
            logger.info("🚨 Consistency audit found mismatches — alert sent.")
    except (SQLAlchemyError, RuntimeError, OSError):
        err_msg = traceback.format_exc()
        logger.exception("Consistency audit raised an unexpected error")
        SlackWebhookClient.send_error_alert(f"Consistency audit error:\n{err_msg}")


def run_pipeline_sync(pipeline_type: str, pdf_path: str | None = None) -> None:
    """
    Helper to run async pipeline synchronously and catch errors for Telegram alerts.

    After OCI sync completes, automatically runs a count-level consistency audit.
    """
    run_sync = os.getenv("RUN_SYNC_SUPABASE") == "1" or os.getenv("RUN_SYNC_OCI") == "1"
    try:
        if pipeline_type == "static":
            asyncio.run(run_static_pipeline(pdf_path))
        elif pipeline_type == "dynamic":
            asyncio.run(run_dynamic_pipeline())
        elif pipeline_type == "realtime":
            asyncio.run(run_realtime_pipeline())
        else:
            logger.error("❌ Unknown pipeline type: %s", pipeline_type)
            return
    except PIPELINE_EXCEPTIONS:
        logger.exception("Critical Pipeline Failure")
        err_msg = traceback.format_exc()
        # Send Telegram Bot Warning Webhook alert
        SlackWebhookClient.send_error_alert(err_msg)
        return

    # Run a lightweight (count-level) consistency audit after each successful sync
    if run_sync:
        run_consistency_check(deep=False)


def start_scheduler() -> None:
    """Starts APScheduler daemon in the background to execute pipelines periodically."""
    logger.info("\n⏰ Starting background scheduler daemon...")
    scheduler = BlockingScheduler()

    # 1. Realtime Pipeline: Runs every 2 hours
    scheduler.add_job(
        run_pipeline_sync,
        "interval",
        hours=2,
        args=["realtime"],
        id="realtime_pipeline",
        name="Realtime News and Forum Crawler",
    )

    # 2. Dynamic Pipeline: Runs daily at 3:00 AM
    scheduler.add_job(
        run_pipeline_sync,
        "cron",
        hour=3,
        minute=0,
        args=["dynamic"],
        id="dynamic_pipeline",
        name="Daily Schedule & Roster Crawler",
    )

    # 3. Static Pipeline: Runs quarterly (e.g. Day 1 of Jan, Apr, Jul, Oct at 4:00 AM)
    scheduler.add_job(
        run_pipeline_sync,
        "cron",
        month="1,4,7,10",
        day=1,
        hour=4,
        minute=0,
        args=["static"],
        id="static_pipeline",
        name="Quarterly Rules & Wiki Crawler",
    )

    # 4. Deep Consistency Audit: Runs daily at 05:00 AM (after dynamic pipeline at 03:00 AM)
    #    Only active when OCI sync is enabled via environment variable.
    scheduler.add_job(
        run_consistency_check,
        "cron",
        hour=5,
        minute=0,
        kwargs={"deep": True},
        id="consistency_audit",
        name="Daily Deep Consistency Audit (SQLite ↔ OCI)",
    )

    logger.info("   Jobs scheduled:")
    for job in scheduler.get_jobs():
        logger.info("   - [%s] %s (Next run: %s)", job.id, job.name, job.next_run_time)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.exception("⏰ Scheduler stopped.")


def main() -> int:
    """Main entry point for this CLI command."""
    parser = argparse.ArgumentParser(description="KBO Knowledge & Issue Crawler Pipeline Orchestrator")
    parser.add_argument(
        "--type",
        choices=["static", "dynamic", "realtime"],
        help="Execute specific crawler pipeline type immediately.",
    )
    parser.add_argument("--pdf", type=str, help="Local KBO rules PDF file path. (Used only with --type static)")
    parser.add_argument(
        "--daemon",
        action="store_true",
        help="Run scheduler daemon in background to execute jobs periodically.",
    )

    args = parser.parse_args()

    if not args.type and not args.daemon:
        parser.print_help()
        sys.exit(1)

    if args.type:
        run_pipeline_sync(args.type, args.pdf)
    elif args.daemon:
        start_scheduler()


if __name__ == "__main__":
    main()
