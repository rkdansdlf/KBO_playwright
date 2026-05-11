import asyncio
from playwright.async_api import async_playwright

async def find_boxscore_selector():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        url = "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameDate=20240323&gameId=20240323HHLG0&section=REVIEW"
        await page.goto(url, wait_until="networkidle")
        await page.wait_for_timeout(5000)
        
        # Find all elements that contain "박스스코어"
        matches = await page.evaluate("""
            () => {
                const results = [];
                const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_ELEMENT, null, false);
                let node;
                while(node = walker.nextNode()) {
                    if (node.innerText && node.innerText.includes("박스스코어")) {
                        results.push({
                            tagName: node.tagName,
                            className: node.className,
                            id: node.id,
                            innerText: node.innerText,
                            outerHTML: node.outerHTML.substring(0, 200)
                        });
                    }
                }
                return results;
            }
        """)
        
        for m in matches:
            print(f"Match: {m['tagName']} id='{m['id']}' class='{m['className']}' text='{m['innerText'][:50]}'")
            
        await browser.close()

if __name__ == "__main__":
    asyncio.run(find_boxscore_selector())
