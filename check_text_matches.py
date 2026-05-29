import asyncio

from playwright.async_api import async_playwright


async def run():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        url = "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx?gameDate=20240323&gameId=20240323HHLG0&section=REVIEW"
        await page.goto(url, wait_until="networkidle", timeout=60000)
        await page.wait_for_timeout(5000)

        content = await page.content()
        print(f"홈런 count: {content.count('홈런')}")
        print(f"볼넷 count: {content.count('볼넷')}")
        print(f"4사구 count: {content.count('4사구')}")

        # Also find all text nodes that contain these words and their parent tags
        matches = await page.evaluate("""() => {
            const terms = ['홈런', '볼넷', '4사구', '기록'];
            const results = [];
            const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, null, false);
            let node;
            while (node = walker.nextNode()) {
                const text = node.textContent.trim();
                if (terms.some(term => text.includes(term))) {
                    results.push({
                        text,
                        parentTag: node.parentElement.tagName,
                        parentId: node.parentElement.id,
                        parentClass: node.parentElement.className
                    });
                }
            }
            return results;
        }""")

        print("\nMatches found:")
        for m in matches:
            print(f"Text: '{m['text']}', Tag: {m['parentTag']}, ID: {m['parentId']}, Class: {m['parentClass']}")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(run())
