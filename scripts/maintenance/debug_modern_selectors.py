"""Debug: inspect setGameDetail function and AJAX loading."""
import asyncio, sys, os
sys.path.insert(0, os.path.abspath('.'))
from playwright.async_api import async_playwright

async def main():
    game_id = "20240323HHLG0"
    game_date = "20240323"
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        # Monitor network requests
        requests_log = []
        page.on("request", lambda req: requests_log.append(f">> {req.method} {req.url}") if 'koreabaseball' in req.url else None)
        page.on("response", lambda res: requests_log.append(f"<< {res.status} {res.url}") if 'koreabaseball' in res.url else None)
        
        url = f"https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameDate={game_date}"
        await page.goto(url, wait_until="networkidle", timeout=30000)
        await asyncio.sleep(2)
        
        # 1. Get the source of setGameDetail
        fn_source = await page.evaluate("window.setGameDetail.toString()")
        print(f"=== setGameDetail source ===")
        print(fn_source)
        print("===========================\n")
        
        # 2. Check the page structure around game cards
        game_structure = await page.evaluate("""() => {
            const container = document.querySelector('.game-cont, .schedule-game, #gameSchedule, .game-list-wrap, .game_list_wrap');
            if (!container) {
                // Try to find game-related containers
                const allDivs = document.querySelectorAll('div[id], div[class]');
                const gameRelated = [];
                allDivs.forEach(d => {
                    const id = d.id || '';
                    const cls = d.className || '';
                    if ((id+cls).toLowerCase().match(/game|schedule|center/)) {
                        gameRelated.push({id, class: cls, childCount: d.children.length, html: d.outerHTML.substring(0, 200)});
                    }
                });
                return {found: false, gameRelated};
            }
            return {found: true, html: container.outerHTML.substring(0, 500)};
        }""")
        print(f"=== Game Structure ===")
        if game_structure.get('found'):
            print(game_structure['html'])
        else:
            for item in game_structure.get('gameRelated', [])[:10]:
                print(f"  id={item['id']}, class={item['class']}, children={item['childCount']}")
                print(f"    {item['html'][:150]}")
        print()
        
        # 3. Clear request log and call setGameDetail
        requests_log.clear()
        print(f"Calling setGameDetail('{game_id}')...")
        await page.evaluate(f"window.setGameDetail('{game_id}')")
        await asyncio.sleep(5)  # Wait for AJAX
        
        print(f"\n=== Network requests after setGameDetail ===")
        for r in requests_log:
            print(f"  {r}")
        print()
        
        # 4. Check DOM changes
        contents = await page.evaluate("""() => {
            const gc = document.querySelector('#gameCenterContents');
            return {
                gameCenterContents: gc ? gc.innerHTML.substring(0, 500) : 'NOT FOUND',
                gameCenterLen: gc ? gc.innerHTML.length : 0,
                bodyLen: document.body.innerHTML.length
            };
        }""")
        print(f"=== After setGameDetail ===")
        print(f"gameCenterContents length: {contents['gameCenterLen']}")
        print(f"gameCenterContents: {contents['gameCenterContents']}")
        print()
        
        # 5. Check ALL loaded scripts for game-related functions
        scripts_info = await page.evaluate("""() => {
            const fns = [];
            for (const key of Object.keys(window)) {
                if (typeof window[key] === 'function') {
                    const name = key.toLowerCase();
                    if (name.includes('game') || name.includes('detail') || name.includes('center') || name.includes('schedule')) {
                        fns.push(key);
                    }
                }
            }
            return fns;
        }""")
        print(f"=== Game-related window functions ===")
        for fn in scripts_info:
            print(f"  {fn}")
        
        # 6. Try navigating with gameId in URL directly
        print(f"\n=== Trying direct URL with gameId ===")
        direct_url = f"https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameDate={game_date}&gameId={game_id}&section=REVIEW"
        requests_log.clear()
        await page.goto(direct_url, wait_until="networkidle", timeout=30000)
        await asyncio.sleep(3)
        
        contents2 = await page.evaluate("""() => {
            const gc = document.querySelector('#gameCenterContents');
            const tables = ['#tblAwayHitter1','#tblHomeHitter1','#tblAwayPitcher','#tblHomePitcher'];
            const found = {};
            tables.forEach(s => {
                const el = document.querySelector(s);
                found[s] = el ? el.querySelectorAll('tbody tr').length + ' rows' : 'NOT FOUND';
            });
            return {
                gameCenterLen: gc ? gc.innerHTML.length : 0,
                tables: found,
                tabs: Array.from(document.querySelectorAll('li[section]')).map(t => t.getAttribute('section'))
            };
        }""")
        print(f"gameCenterContents length: {contents2['gameCenterLen']}")
        print(f"Tables: {contents2['tables']}")
        print(f"Tabs: {contents2['tabs']}")
        
        await page.screenshot(path="data/debug_modern_direct.png")
        print("\nScreenshot: data/debug_modern_direct.png")
        
        await browser.close()

asyncio.run(main())
