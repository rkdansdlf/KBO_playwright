"""
로컬에 저장된 경기 일정 HTML 파일을 데이터베이스로 가져오는 CLI 스크립트.

이 스크립트는 `ingest_mock_game_html.py`와 유사하지만, 경기 상세 정보가 아닌
월별 경기 '일정' 페이지만을 처리하여 `game_schedules` 테이블에 저장합니다.
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable, List, Dict, Any

from src.parsers.schedule_parser import parse_schedule_html
from src.services.schedule_collection_service import save_schedule_games


def ingest_schedule_html(args: argparse.Namespace) -> None:
    """저장된 경기 일정 HTML 파일들을 파싱하여 데이터베이스에 저장합니다."""
    fixtures_dir = Path(args.fixtures_dir)
    if not fixtures_dir.exists():
        raise SystemExit(f"Fixture directory not found: {fixtures_dir}")

    all_games: List[Dict[str, Any]] = []

    files = sorted(fixtures_dir.glob("*.html"))
    if not files:
        print("ℹ️  No HTML files found. Save schedule pages as *.html first.")
        return

    for html_file in files:
        html = html_file.read_text(encoding="utf-8")
        # HTML에서 경기 일정 정보를 파싱합니다.
        games = parse_schedule_html(
            html,
            default_year=args.default_year,
            season_type=args.season_type,
        )
        all_games.extend(games)
        print(f"📄 Parsed {len(games)} games from {html_file.name}")

    if not all_games:
        print("ℹ️  No games parsed from fixtures.")
        return

    # 파싱된 모든 경기 일정을 데이터베이스에 저장합니다.
    result = save_schedule_games(all_games)
    print(f"✅ Ingested {result.saved} games from fixtures. Failed: {result.failed}")


def build_arg_parser() -> argparse.ArgumentParser:
    """CLI 인자 파서를 생성합니다."""
    parser = argparse.ArgumentParser(description="Ingest saved schedule HTML files.")
    parser.add_argument(
        "--fixtures-dir",
        type=str,
        default="tests/fixtures/schedules",
        help="저장된 경기 일정 HTML 파일이 있는 디렉터리",
    )
    parser.add_argument(
        "--default-year",
        type=int,
        default=None,
        help="game_id에서 연도를 추론할 수 없을 때 사용할 기본 연도",
    )
    parser.add_argument(
        "--season-type",
        type=str,
        default="regular",
        choices=["preseason", "regular", "postseason"],
        help="가져온 경기에 적용할 시즌 유형",
    )
    return parser


def main(argv: Iterable[str] | None = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    ingest_schedule_html(args)


if __name__ == "__main__":  # pragma: no cover
    main()

