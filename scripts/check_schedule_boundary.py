import time

from playwright.sync_api import sync_playwright


def check_schedule_years():
    url = "https://www.koreabaseball.com/Schedule/Schedule.aspx"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, wait_until="networkidle")

        for year in ["2010", "2008"]:
            print(f"\n📡 Checking Year {year}...")
            try:
                page.select_option("#ddlYear", year)
                time.sleep(1)
                page.select_option("#ddlMonth", "04")
                time.sleep(2)

                tbl = page.query_selector(".tbl-type06")
                if tbl:
                    text_sample = tbl.inner_text()[:100].replace(chr(10), " ")
                    print(f"   Table Content: {text_sample}")
                    if "데이터가 없습니다" in text_sample:
                        print(f"   ❌ Year {year}: No Data")
                    else:
                        print(f"   ✅ Year {year}: Data Found!")
                        links = tbl.query_selector_all("a")
                        print(f"   Found {len(links)} links.")
                else:
                    print(f"   ❌ Year {year}: Table not found")

            except Exception as e:
                print(f"   ❌ Error checking {year}: {e}")

        browser.close()


if __name__ == "__main__":
    check_schedule_years()
