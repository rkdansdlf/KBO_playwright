import asyncio
import os
import sys

# Ensure project root is in path
sys.path.append(os.getcwd())

from sqlalchemy import select

from src.crawlers.team_history_crawler import TeamHistoryCrawler
from src.crawlers.team_info_crawler import TeamInfoCrawler
from src.db.engine import SessionLocal
from src.models.franchise import Franchise


async def main():
    print("🚀 Starting Team Data Quality Improvement Crawl...")

    # 1. Crawl Admin Info (TeamInfoCrawler)
    info_crawler = TeamInfoCrawler()
    try:
        info_data = await info_crawler.crawl()
        await info_crawler.close()

        # Save Info Data
        if info_data:
            print(f"💾 Updating Franchise Metadata for {len(info_data)} teams...")
            with SessionLocal() as session:
                for item in info_data:
                    # Find Franchise by name (Assuming 'name' column in Franchise matches or contains crawled name)
                    # "Samsung Lions" -> "Samsung"
                    # Current Franchise names in DB are Korean? e.g. "삼성 라이온즈"
                    # Crawled name is "삼성 라이온즈"
                    # Perfect match likely.

                    stmt = select(Franchise).where(Franchise.name == item["name"])
                    result = session.execute(stmt).scalars().first()

                    if not result:
                        # Try partial match
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
                        print(f"   ✅ Updated {result.name}")
                    else:
                        print(f"   ⚠️ Franchise not found for {item['name']}")
                session.commit()
    except Exception as e:
        print(f"❌ TeamInfoCrawler Failed: {e}")
        if info_crawler:
            await info_crawler.close()

    # 2. Crawl History (TeamHistoryCrawler)
    hist_crawler = TeamHistoryCrawler()
    try:
        history_data = await hist_crawler.crawl()
        await hist_crawler.save(history_data)
    except Exception as e:
        print(f"❌ TeamHistoryCrawler Failed: {e}")
        import traceback

        traceback.print_exc()
    finally:
        await hist_crawler.close()


if __name__ == "__main__":
    asyncio.run(main())
