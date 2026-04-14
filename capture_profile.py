import asyncio
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        print("Navigating to profile...")
        await page.goto("https://www.koreabaseball.com/Record/Player/PitcherDetail/Basic.aspx?playerId=79171", wait_until="networkidle")
        await page.wait_for_timeout(2000)
        
        try:
            img_src = await page.locator(".photo img").get_attribute("src")
            salary = await page.locator("#cphContents_cphContents_cphContents_playerProfile_lblSalary").text_content()
            draft = await page.locator("#cphContents_cphContents_cphContents_playerProfile_lblDraft").text_content()
            payment = await page.locator("#cphContents_cphContents_cphContents_playerProfile_lblPayment").text_content()
            
            print(f"IMG: {img_src}")
            print(f"Salary: {salary.strip() if salary else 'None'}")
            print(f"Draft: {draft.strip() if draft else 'None'}")
            print(f"Payment: {payment.strip() if payment else 'None'}")
            
        except Exception as e:
            print(f"Error extracting DOM elements: {e}")
            
        await browser.close()

asyncio.run(main())
