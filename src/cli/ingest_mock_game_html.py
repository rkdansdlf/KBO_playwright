"""로컬에 저장된 경기 상세 HTML 파일(fixture)을 데이터베이스로 가져오는 스크립트.

주로 오프라인 테스트나 디버깅 목적으로 사용됩니다. 지정된 디렉터리에서 HTML 파일을
읽어와 파싱한 후, 그 결과를 데이터베이스에 저장합니다.
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

from src.parsers.game_detail_parser import parse_game_detail_html
from src.repositories.game_repository import GameRepository


def ingest_mock_html(args: argparse.Namespace) -> None:
    """저장된 HTML fixture를 파싱하여 데이터베이스에 저장하는 로직을 수행합니다."""
    repo = GameRepository()
    fixtures_dir = Path(args.fixtures_dir)
    if not fixtures_dir.exists():
        raise SystemExit(f"Fixture directory not found: {fixtures_dir}")

    files = sorted(fixtures_dir.glob("*.html"))
    if args.limit:
        files = files[: args.limit]

    if not files:
        print("ℹ️  No HTML fixtures found. Place files like 20251001NCLG0.html in the directory.")
        return

    for html_file in files:
        game_id = html_file.stem
        with html_file.open("r", encoding="utf-8") as f:
            html = f.read()
        game_date = game_id[:8]
        
        # HTML 내용을 파싱하여 구조화된 데이터로 변환합니다.
        payload = parse_game_detail_html(html, game_id, game_date)
        
        # 변환된 데이터를 데이터베이스에 저장합니다.
        success = repo.save_game_detail(payload)
        if success:
            print(f"✅ Ingested mock game {game_id}")
        else:
            print(f"❌ Failed to ingest mock game {game_id}")


def build_arg_parser() -> argparse.ArgumentParser:
    """CLI 인자 파서를 생성합니다."""
    parser = argparse.ArgumentParser(description="Ingest saved GameCenter HTML fixtures")
    parser.add_argument("--fixtures-dir", type=str, default="tests/fixtures/game_details", help="HTML fixture 파일이 있는 디렉터리")
    parser.add_argument("--limit", type=int, default=None, help="처리할 최대 파일 수")
    return parser


def main(argv: Iterable[str] | None = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    ingest_mock_html(args)


if __name__ == "__main__":  # pragma: no cover
    main()

