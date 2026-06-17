import asyncio
import logging
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

# Ensure project root is in path
sys.path.append(str(Path.cwd()))

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from src.crawlers.team_history_crawler import TeamHistoryCrawler
from src.crawlers.team_info_crawler import TeamInfoCrawler
from src.db.engine import SessionLocal
from src.models.franchise import Franchise


async def main():
    logger.info("🚀 Starting Team Data Quality Improvement Crawl...")

    # 1. Crawl Admin Info (TeamInfoCrawler)
    info_crawler = TeamInfoCrawler()
    try:
        info_data = await info_crawler.crawl()
        await info_crawler.close()

        # Save Info Data
        if info_data:
            logger.info(f"💾 Updating Franchise Metadata for {len(info_data)} teams...")
            with SessionLocal() as session:
                for item in info_data:
                    stmt = select(Franchise).where(Franchise.name == item["name"])
                    result = session.execute(stmt).scalars().first()

                    if not result:
                        stmt = select(Franchise).where(Franchise.name.like(f"%{item['name']}%"))
                        result = session.execute(stmt).scalars().first()

                    if result:
                        meta = result.metadata_json or {}
                        meta.update(
                            {
                                "found_year": item["found_year"],
                                "owner": item["owner"],
                                "ceo": item["ceo"],
                                "address": item["address"],
                                "phone": item["phone"],
                            }
                        )
                        result.metadata_json = meta
                        result.web_url = item["homepage"]
                        session.add(result)
                        logger.info(f"   ✅ Updated {result.name}")
                    else:
                        logger.info(f"   ⚠️ Franchise not found for {item['name']}")
                session.commit()
    except Exception as e:  # noqa: BLE001
        logger.info(f"❌ TeamInfoCrawler Failed: {e}")
        if info_crawler:
            await info_crawler.close()

    # 2. Crawl History (TeamHistoryCrawler)
    hist_crawler = TeamHistoryCrawler()
    try:
        history_data = await hist_crawler.crawl()
        await hist_crawler.save(history_data)
    except (TimeoutError, ConnectionError, OSError, ValueError, KeyError, SQLAlchemyError) as e:
        logger.info(f"❌ TeamHistoryCrawler Failed: {e}")
        import traceback

        traceback.print_exc()
    finally:
        await hist_crawler.close()


if __name__ == "__main__":
    asyncio.run(main())
