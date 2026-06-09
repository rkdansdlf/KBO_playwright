import argparse
import subprocess
import time


def run_crawler(year, crawler_type):
    """실행 중인 크롤러 프로세스 호출"""
    module = f"src.crawlers.legacy_{crawler_type}_crawler"
    cmd = ["venv/bin/python3", "-m", module, "--year", str(year), "--save", "--headless"]
    print(f"🚀 Running {crawler_type} crawler for {year}...")
    try:
        result = subprocess.run(cmd, capture_output=False, text=True)
        return result.returncode == 0
    except Exception as e:  # noqa: BLE001
        print(f"❌ Error running {crawler_type} for {year}: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="KBO 1982-2000 Full Stats Recovery")
    parser.add_argument("--start-year", type=int, default=1982)
    parser.add_argument("--end-year", type=int, default=2000)
    args = parser.parse_args()

    start = args.start_year
    end = args.end_year

    print(f"🌟 Starting Full Recovery for {start}-{end}...")

    for year in range(start, end + 1):
        print(f"\n📅 --- {year} Season ---")

        # Batting
        success_bat = run_crawler(year, "batting")
        if not success_bat:
            print(f"⚠️ Batting crawler failed for {year}")

        # Pitching
        success_pit = run_crawler(year, "pitching")
        if not success_pit:
            print(f"⚠️ Pitching crawler failed for {year}")

        print(f"✅ Finished {year} season.")
        time.sleep(2)


if __name__ == "__main__":
    main()
