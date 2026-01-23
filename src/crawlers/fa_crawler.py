import asyncio
import argparse
import re
import json
from typing import List, Dict, Any, Optional
from datetime import date, datetime

from playwright.async_api import async_playwright, Page, TimeoutError, BrowserContext
from sqlalchemy.orm import Session

from src.db.engine import SessionLocal
from src.models.player import PlayerMovement
from src.utils.team_codes import resolve_team_code
from src.utils.safe_print import safe_print as print
from src.utils.playwright_blocking import install_async_resource_blocking

from playwright_stealth import Stealth


class FACrawler:
    def __init__(self, headless: bool = False):
        self.url = "https://namu.wiki/w/KBO%20%EB%A6%AC%EA%B7%B8/%EC%97%AD%EB%8C%80%20FA"
        self.headless = headless

    async def crawl(self, target_years: List[int]) -> List[Dict[str, Any]]:
        """
        Crawl FA data for specific years.
        If target_years is empty, crawls all available years (from 2017).
        """
        results = []
        async with async_playwright() as p:
            # Use stealth plugin with headless mode (or non-headless as recommended)
            browser = await p.chromium.launch(headless=self.headless)
            
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080},
                locale="ko-KR",
                timezone_id="Asia/Seoul"
            )
            await install_async_resource_blocking(context)
            
            page = await context.new_page()
            await Stealth().apply_stealth_async(page)

            try:
                print(f"🌍 Navigating to {self.url}...")
                await page.goto(self.url, wait_until="domcontentloaded", timeout=60000)
                
                # Wait for table or specific content wrapper as per user tip
                try:
                    await page.wait_for_selector(".wiki-table-wrap table, #content .table, table", timeout=30000)
                except TimeoutError:
                    print("⚠️ Timeout waiting for table selector. Exploring content...")

                # Extract data from 4 main sections
                # 2.1 Retained Pitchers
                # 2.2 Retained Fielders
                # 3.1 Transferred Pitchers
                # 3.2 Transferred Fielders
                
                # Selectors identified in planning
                sections = [
                    {"id": "s-2.1", "type": "RETAINED", "pos": "PITCHER"},
                    {"id": "s-2.2", "type": "RETAINED", "pos": "FIELDER"},
                    {"id": "s-3.1", "type": "TRANSFERRED", "pos": "PITCHER"},
                    {"id": "s-3.2", "type": "TRANSFERRED", "pos": "FIELDER"},
                ]
                
                for section in sections:
                    print(f"🔍 Processing Section {section['id']} ({section['type']} - {section['pos']})...")
                    section_data = await self._extract_section_table(page, section['id'], section['type'])
                    
                    # Filter by year
                    if target_years:
                        filtered = [d for d in section_data if d['year'] in target_years]
                        print(f"   => Found {len(filtered)} records for target years.")
                        results.extend(filtered)
                    else:
                        print(f"   => Found {len(section_data)} records total.")
                        results.extend(section_data)
                        
            except Exception as e:
                print(f"❌ Error during crawling: {e}")
                import traceback
                traceback.print_exc()
            finally:
                await browser.close()
                
        return results

    async def _extract_section_table(self, page: Page, section_anchor_id: str, section_type: str) -> List[Dict[str, Any]]:
        """
        Extracts table data following a specific header anchor ID or Text.
        """
        # Search for Header by ID or Text
        # The section_anchor_id is like 's-2.1', corresponding text '2.1. 투수' (approx)
        
        target_text = ""
        if "2.1" in section_anchor_id: target_text = "2.1"
        elif "2.2" in section_anchor_id: target_text = "2.2"
        elif "3.1" in section_anchor_id: target_text = "3.1"
        elif "3.2" in section_anchor_id: target_text = "3.2"
        
        # User tip: Table selector .wiki-table-wrap table or #content .table
        
        full_script = f"""
        () => {{
            const scanForTable = (startElem) => {{
                let next = startElem.nextElementSibling;
                while (next && next.tagName !== 'H2' && next.tagName !== 'H3' && next.tagName !== 'H1') {{
                    if (next.tagName === 'TABLE') return next;
                    const nestedTable = next.querySelector('div.wiki-table-wrap table');
                    if (nestedTable) return nestedTable;
                    const anyTable = next.querySelector('table');
                    if (anyTable) return anyTable;
                    
                    next = next.nextElementSibling;
                }}
                return null;
            }};
            
            let table = null;
            
            // 1. Try by ID
            const anchor = document.getElementById("{section_anchor_id}");
            if (anchor) {{
                table = scanForTable(anchor.parentElement);
            }}
            
            // 2. Try by Text (Backup)
            if (!table) {{
                const headers = Array.from(document.querySelectorAll('h1, h2, h3, h4, h5, h6'));
                
                // target_text like "2.1"
                const specificHeader = headers.find(h => h.innerText.includes("{target_text}") && (h.innerText.includes("투수") || h.innerText.includes("야수") || h.innerText.includes("타자")));
                if (specificHeader) {{
                    table = scanForTable(specificHeader);
                }}
            }}
            
            if (!table) return null;
            
            // PARSE TABLE
            const rows = Array.from(table.rows);
            const data = [];
            
            rows.forEach(tr => {{
                const cells = Array.from(tr.cells).map(c => {{
                    return {{
                        text: c.innerText.trim(),
                        rowSpan: c.rowSpan || 1,
                        colSpan: c.colSpan || 1
                    }};
                }});
                data.push(cells);
            }});
            
            return data;
        }}
        """
        
        raw_rows = await page.evaluate(full_script)
        
        if not raw_rows:
            print(f"   ⚠️ No table found for section {section_anchor_id}")
            return []
            
        parsed_data = []
        current_year = None
        
        if not raw_rows: return []
        
        for row_idx, row in enumerate(raw_rows):
            # Check for header
            texts = [c['text'] for c in row]
            if "이름" in texts and "계약기간" in texts: continue
            if "팀" in texts and "총액" in texts: continue
            
            row_texts = [cell['text'] for cell in row]
            if not row_texts: continue
            
            year_candidate = None
            is_new_year_group = False
            
            # Try to find year in first few columns
            for i in range(min(2, len(row_texts))):
                val = row_texts[i]
                if re.match(r'^\d{4}$', val) or re.match(r'^\d{4}년$', val):
                    try:
                        year_candidate = int(re.sub(r'\D', '', val))
                        is_new_year_group = True
                        break
                    except: pass
            
            if year_candidate:
                current_year = year_candidate
                
            if not current_year: continue
            
            # Determine Name Index
            name_idx = -1
            if is_new_year_group:
                for i in range(min(2, len(row_texts))):
                    val = row_texts[i]
                    if re.match(r'^\d{4}(\D|$)', val):
                         name_idx = i + 1
                         break
            else:
                 if re.match(r'^\d+$', row_texts[0]):
                     name_idx = 1
                 else:
                     name_idx = 0
            
            if name_idx == -1 or name_idx >= len(row_texts): continue
            
            item = {}
            item['year'] = current_year
            
            try:
                if section_type == "RETAINED":
                    item['player_name'] = row_texts[name_idx]
                    item['team'] = row_texts[name_idx + 1]
                    item['duration'] = row_texts[name_idx + 2]
                    item['amount'] = row_texts[name_idx + 3]
                    item['remarks'] = row_texts[name_idx + 4] if len(row_texts) > name_idx + 4 else ""
                    
                elif section_type == "TRANSFERRED":
                    item['player_name'] = row_texts[name_idx]
                    item['old_team'] = row_texts[name_idx + 1]
                    item['new_team'] = row_texts[name_idx + 2]
                    item['team'] = row_texts[name_idx + 2] 
                    item['duration'] = row_texts[name_idx + 3]
                    item['amount'] = row_texts[name_idx + 4]
                    item['remarks'] = row_texts[name_idx + 5] if len(row_texts) > name_idx + 5 else ""

                # Cleanup
                for k, v in item.items():
                    if isinstance(v, str):
                        item[k] = re.sub(r'\[[^\]]+\]', '', v).strip()
            
                parsed_data.append(item)
            
            except IndexError:
                continue

        return parsed_data

    def load_from_json(self, filepath: str) -> List[Dict[str, Any]]:
        """Loads FA data from a JSON file."""
        print(f"📂 Loading data from {filepath}...")
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            print(f"   => Loaded {len(data)} records.")
            return data
        except Exception as e:
            print(f"❌ Error loading JSON: {e}")
            return []

    def save_to_db(self, data: List[Dict[str, Any]], session: Session, dry_run: bool = False):
        """
        Saves or updates FA data in the database.
        """
        new_records = 0
        updates = 0
        
        print(f"💾 processing {len(data)} records for Database...")
        
        for item in data:
            if not item.get('player_name'): continue
            
            name = item['player_name']
            year = item['year']
            team_raw = item.get('team')
            team_code = resolve_team_code(team_raw)
            
            if not team_code:
                # Special case for split contracts or unknown teams
                if item.get('type') == 'transfer' and not item.get('new_team'):
                     # Likely split contract details like Hwang Jae-gyun 2017
                     print(f"   ⚠️ Skipping record for {name} ({year}): No valid team (likely overseas split).")
                     continue

                print(f"   ⚠️ Could not resolve team code for '{team_raw}' ({name}, {year}). Skipping.")
                continue
                
            # Construct Remarks string
            contract_info = f"FA계약: {item.get('contract_duration', item.get('duration', '?'))}, {item.get('total_amount', item.get('amount', '?'))}"
            remarks = item.get('remarks')
            if remarks and remarks != "-" and remarks != "비공개":
                 contract_info += f", {remarks}"
            
            # Matching Logic
            # Find existing record in player_movements
            # Range: Nov of (Year-1) to Mar of (Year)
            start_date = date(year - 1, 11, 1)
            end_date = date(year, 3, 31)
            
            existing = session.query(PlayerMovement).filter(
                PlayerMovement.player_name == name,
                PlayerMovement.team_code == team_code,
                PlayerMovement.date >= start_date,
                PlayerMovement.date <= end_date
            ).first()
            
            if dry_run:
                print(f"   [DRY RUN] {name} ({year}): {'MATCH FOUND' if existing else 'NEW RECORD'} -> {contract_info}")
                continue
                
            if existing:
                if existing.remarks:
                    if "FA계약" not in existing.remarks:
                        existing.remarks = f"{existing.remarks} | {contract_info}"
                        updates += 1
                else:
                    existing.remarks = contract_info
                    updates += 1
            else:
                # Insert new record
                default_date = date(year, 1, 15)
                new_move = PlayerMovement(
                    date=default_date,
                    section="FA",
                    team_code=team_code,
                    player_name=name,
                    remarks=contract_info
                )
                session.add(new_move)
                new_records += 1
        
        if not dry_run:
            try:
                session.commit()
                print(f"✅ DB Update Complete: {new_records} Inserted, {updates} Updated.")
            except Exception as e:
                session.rollback()
                print(f"❌ DB Error: {e}")

async def main():
    parser = argparse.ArgumentParser(description="Crawl KBO FA Contracts from Namu Wiki")
    parser.add_argument("--year", type=int, help="Specific year to crawl (e.g. 2024)")
    parser.add_argument("--save", action="store_true", help="Save changes to database")
    parser.add_argument("--dry-run", action="store_true", help="Print actions without saving")
    parser.add_argument("--file", type=str, help="Load data from JSON file instead of crawling")
    
    args = parser.parse_args()
    
    crawler = FACrawler(headless=False) # Configured to False by default per tip
    
    data = []
    if args.file:
        data = crawler.load_from_json(args.file)
        if args.year:
            data = [d for d in data if d.get('year') == args.year]
    else:
        target_years = [args.year] if args.year else []
        data = await crawler.crawl(target_years)
    
    if args.save or args.dry_run:
        session = SessionLocal()
        try:
            crawler.save_to_db(data, session, dry_run=args.dry_run or (not args.save))
        finally:
            session.close()
    else:
        # Just print raw data
        print(f"Fetched {len(data)} records.")
        for d in data[:5]:
            print(d)

if __name__ == "__main__":
    asyncio.run(main())
