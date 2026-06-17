#!/usr/bin/env python3
"""
KBO 전체 연도 크롤링 - 자동 전략 선택
2001년까지: 레거시 단순 컬럼 구조
2002년부터: 기존 복합 구조
"""

import argparse
import logging
import subprocess
import sys
from datetime import datetime
from typing import Any

from src.utils.series_validation import filter_series_for_year

logger = logging.getLogger(__name__)
SUBPROCESS_EXCEPTIONS = (subprocess.SubprocessError, OSError, RuntimeError, ValueError)


def get_year_range_validation(start_year: int, end_year: int) -> tuple:
    """연도 범위 유효성 검증"""
    current_year = datetime.now().year

    if start_year < 1982:
        raise ValueError("KBO는 1982년에 창설되었습니다.")

    if end_year > current_year:
        raise ValueError(f"미래 연도는 크롤링할 수 없습니다. (현재: {current_year}년)")

    if start_year > end_year:
        raise ValueError("시작 년도가 끝 년도보다 클 수 없습니다.")

    return start_year, end_year


def determine_crawling_strategy(year: int) -> str:
    """년도에 따른 크롤링 전략 결정"""
    if year <= 2001:
        return "legacy"
    else:
        return "modern"


def run_legacy_crawling(year: int, series: str, data_type: str, headless: bool = True) -> tuple:
    """
    레거시 크롤링 실행 (2001년 이전)

    Returns:
        (success: bool, output: str)
    """
    if data_type == "batting":
        cmd = [
            sys.executable,
            "-m",
            "src.crawlers.legacy_batting_crawler",
            "--year",
            str(year),
            "--series",
            series,
            "--save",
        ]
    elif data_type == "pitching":
        cmd = [
            sys.executable,
            "-m",
            "src.crawlers.legacy_pitching_crawler",
            "--year",
            str(year),
            "--series",
            series,
            "--save",
        ]
    else:
        return False, f"Unknown data type: {data_type}"

    if headless:
        cmd.append("--headless")

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        success = result.returncode == 0 and "크롤링 완료" in result.stdout
        return success, result.stdout + result.stderr
    except subprocess.TimeoutExpired:
        return False, "크롤링 타임아웃"
    except SUBPROCESS_EXCEPTIONS as e:
        return False, f"크롤링 실행 오류: {e}"


def run_modern_crawling(year: int, series: str, data_type: str, headless: bool = True) -> tuple:
    """
    현대 크롤링 실행 (2002년 이후)

    Returns:
        (success: bool, output: str)
    """
    if data_type == "batting":
        cmd = [
            sys.executable,
            "-m",
            "src.crawlers.player_batting_all_series_crawler",
            "--year",
            str(year),
            "--series",
            series,
            "--save",
        ]
    elif data_type == "pitching":
        cmd = [
            sys.executable,
            "-m",
            "src.crawlers.player_pitching_all_series_crawler",
            "--year",
            str(year),
            "--series",
            series,
            "--save",
        ]
    else:
        return False, f"Unknown data type: {data_type}"

    if headless:
        cmd.append("--headless")

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        success = result.returncode == 0 and "크롤링 완료" in result.stdout
        return success, result.stdout + result.stderr
    except subprocess.TimeoutExpired:
        return False, "크롤링 타임아웃"
    except SUBPROCESS_EXCEPTIONS as e:
        return False, f"크롤링 실행 오류: {e}"


def crawl_historical_data(
    start_year: int = 1982,
    end_year: int = None,
    series_list: list[str] = None,
    headless: bool = True,
    reset_db: bool = True,
) -> dict[str, Any]:
    """
    전체 KBO 역사 데이터 크롤링
    년도별 자동 전략 선택
    """
    if end_year is None:
        end_year = datetime.now().year

    if series_list is None:
        series_list = ["regular", "korean_series", "playoff"]

    # 유효성 검증
    start_year, end_year = get_year_range_validation(start_year, end_year)

    logger.info("🚀 KBO 전체 역사 데이터 크롤링 시작")
    logger.info("=" * 50)
    logger.info(f"📅 대상 기간: {start_year}년 ~ {end_year}년")
    logger.info(f"📊 시리즈: {', '.join(series_list)}")
    logger.info(f"🤖 헤드리스 모드: {'ON' if headless else 'OFF'}")
    logger.info(f"🗑️ DB 초기화: {'ON' if reset_db else 'OFF'}")

    # 전략별 년도 분류
    legacy_years = [y for y in range(start_year, end_year + 1) if y <= 2001]
    modern_years = [y for y in range(start_year, end_year + 1) if y >= 2002]

    logger.info("\n📋 크롤링 전략:")
    if legacy_years:
        logger.info(f"  🕰️ 레거시 모드: {len(legacy_years)}년 ({min(legacy_years)}-{max(legacy_years)})")
    if modern_years:
        logger.info(f"  🚀 현대 모드: {len(modern_years)}년 ({min(modern_years)}-{max(modern_years)})")

    # 데이터베이스 초기화
    if reset_db:
        logger.info("\n🗑️ SQLite 데이터베이스 초기화 중...")
        reset_cmd = [sys.executable, "reset_sqlite.py", "--range", str(start_year), str(end_year), "--force"]
        try:
            subprocess.run(reset_cmd, check=True)
            logger.info("✅ 데이터베이스 초기화 완료")
        except subprocess.CalledProcessError:
            logger.info("⚠️ 데이터베이스 초기화 실패, 계속 진행")

    # 결과 추적
    results = {"total_tasks": 0, "success_count": 0, "failed_tasks": [], "legacy_count": 0, "modern_count": 0}

    len(range(start_year, end_year + 1))

    # 실제 크롤링 가능한 작업 수 계산 (연도별 시리즈 필터링 고려)
    actual_total_tasks = 0
    for year in range(start_year, end_year + 1):
        available_series = filter_series_for_year(year, series_list)
        actual_total_tasks += len(available_series) * 2  # 타자 + 투수

    results["total_tasks"] = actual_total_tasks

    logger.info(f"\n🎯 총 작업 수: {actual_total_tasks}개 (연도별 가능한 시리즈 × 타자/투수)")
    logger.info("\n%s", "=" * 50)

    # 년도별 크롤링
    for year in range(start_year, end_year + 1):
        strategy = determine_crawling_strategy(year)
        year_start = datetime.now()

        logger.info(f"\n📅 {year}년 크롤링 시작 ({strategy} 모드)")
        logger.info("-" * 30)

        year_success = 0
        year_total = len(series_list) * 2

        # 해당 연도에 존재하는 시리즈만 필터링
        available_series = filter_series_for_year(year, series_list)
        year_total = len(available_series) * 2

        # 시리즈별 크롤링
        for series in available_series:
            logger.info(f"  📊 {series} 시리즈:")

            # 타자 데이터 크롤링
            logger.info("    🏏 타자 크롤링...")
            if strategy == "legacy":
                success, output = run_legacy_crawling(year, series, "batting", headless)
                results["legacy_count"] += 1
            else:
                success, output = run_modern_crawling(year, series, "batting", headless)
                results["modern_count"] += 1

            if success:
                logger.info("✅")
                results["success_count"] += 1
                year_success += 1
            else:
                logger.info("❌")
                results["failed_tasks"].append(f"{year}-{series}-batting")
                if "타임아웃" in output or "실행 오류" in output:
                    logger.info(f"      💥 {output}")

            # 투수 데이터 크롤링
            logger.info("    ⚾ 투수 크롤링...")
            if strategy == "legacy":
                success, output = run_legacy_crawling(year, series, "pitching", headless)
                results["legacy_count"] += 1
            else:
                success, output = run_modern_crawling(year, series, "pitching", headless)
                results["modern_count"] += 1

            if success:
                logger.info("✅")
                results["success_count"] += 1
                year_success += 1
            else:
                logger.info("❌")
                results["failed_tasks"].append(f"{year}-{series}-pitching")
                if "타임아웃" in output or "실행 오류" in output:
                    logger.info(f"      💥 {output}")

        # 년도별 결과
        year_duration = (datetime.now() - year_start).total_seconds()
        success_rate = (year_success / year_total) * 100
        logger.info(
            f"  📊 {year}년 결과: {year_success}/{year_total} 성공 ({success_rate:.1f}%) - {year_duration:.0f}초"
        )

    # 최종 결과
    logger.info("\n%s", "=" * 50)
    logger.info("🎉 전체 크롤링 완료!")

    overall_success_rate = (results["success_count"] / results["total_tasks"]) * 100
    logger.info("\n📊 최종 결과:")
    logger.info(f"  ✅ 성공: {results['success_count']}/{results['total_tasks']} ({overall_success_rate:.1f}%)")
    logger.info(f"  🕰️ 레거시 모드: {results['legacy_count']}개 작업")
    logger.info(f"  🚀 현대 모드: {results['modern_count']}개 작업")

    if results["failed_tasks"]:
        logger.info("\n❌ 실패한 작업들:")
        for task in results["failed_tasks"][:10]:  # 처음 10개만 표시
            logger.info(f"    - {task}")
        if len(results["failed_tasks"]) > 10:
            logger.info(f"    ... 및 {len(results['failed_tasks']) - 10}개 더")

    # 데이터베이스 상태 확인
    logger.info("\n🔍 최종 데이터베이스 확인:")
    check_cmd = [
        sys.executable,
        "-c",
        f"""
from src.db.engine import SessionLocal
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching
from sqlalchemy import and_
import logging


logger = logging.getLogger(__name__)
with SessionLocal() as session:
    batting_count = session.query(PlayerSeasonBatting).filter(
        and_(
            PlayerSeasonBatting.season >= {start_year},
            PlayerSeasonBatting.season <= {end_year}
        )
    ).count()

    pitching_count = session.query(PlayerSeasonPitching).filter(
        and_(
            PlayerSeasonPitching.season >= {start_year},
            PlayerSeasonPitching.season <= {end_year}
        )
    ).count()

    logger.info(f"  📊 타자 데이터: {{batting_count:,}}건")
    logger.info(f"  📊 투수 데이터: {{pitching_count:,}}건")
    logger.info(f"  📊 총 데이터: {{batting_count + pitching_count:,}}건")
""",
    ]

    try:
        subprocess.run(check_cmd)
    except SUBPROCESS_EXCEPTIONS:
        logger.info("  ⚠️ 데이터베이스 확인 실패")

    return results


def main():
    parser = argparse.ArgumentParser(description="KBO 전체 역사 데이터 크롤링 (자동 전략 선택)")

    parser.add_argument("--start", type=int, default=1982, help="시작 년도 (기본값: 1982)")
    parser.add_argument("--end", type=int, help="끝 년도 (기본값: 현재년도)")
    parser.add_argument(
        "--series",
        nargs="+",
        choices=["regular", "exhibition", "korean_series", "playoff", "wildcard", "semi_playoff"],
        default=["regular", "korean_series", "playoff"],
        help="크롤링할 시리즈 목록",
    )
    parser.add_argument("--no-headless", action="store_true", help="브라우저 UI 표시")
    parser.add_argument("--no-reset", action="store_true", help="DB 초기화 생략")
    parser.add_argument("--recent", action="store_true", help="최근 3년만 크롤링")
    parser.add_argument("--full-history", action="store_true", help="전체 역사 크롤링 (1982-현재)")

    args = parser.parse_args()

    # 특수 모드 처리
    current_year = datetime.now().year
    if args.recent:
        start_year = current_year - 2
        end_year = current_year
    elif args.full_history:
        start_year = 1982
        end_year = current_year
    else:
        start_year = args.start
        end_year = args.end if args.end else current_year

    try:
        results = crawl_historical_data(
            start_year=start_year,
            end_year=end_year,
            series_list=args.series,
            headless=not args.no_headless,
            reset_db=not args.no_reset,
        )

        success_rate = (results["success_count"] / results["total_tasks"]) * 100

        if success_rate >= 90:
            logger.info(f"\n🎉 크롤링 성공! 성공률 {success_rate:.1f}%")
            sys.exit(0)
        elif success_rate >= 70:
            logger.info(f"\n⚠️ 크롤링 부분 성공. 성공률 {success_rate:.1f}%")
            sys.exit(1)
        else:
            logger.info(f"\n❌ 크롤링 실패. 성공률 {success_rate:.1f}%")
            sys.exit(2)

    except KeyboardInterrupt:
        logger.info("\n❌ 사용자가 중단했습니다.")
        sys.exit(130)
    except Exception as e:  # noqa: BLE001
        logger.info(f"\n❌ 예상치 못한 오류: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
