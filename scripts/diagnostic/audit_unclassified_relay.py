"""
진단 스크립트: 미분류(unnclassified) 문자 중계 텍스트 분석.

특정 기간의 game_play_by_play 테이블에서 event_type='unclassified' 또는
detect_relay_event_type()이 'unknown'을 반환한 텍스트를 수집/분석하여
_RELAY_NOISE_TOKENS 업데이트 필요성을 판단합니다.

사용법:
    python3 scripts/diagnostic/audit_unclassified_relay.py --days 7
    python3 scripts/diagnostic/audit_unclassified_relay.py --game-id 20260412SKLG0
    python3 scripts/diagnostic/audit_unclassified_relay.py --days 30 --top-words 20
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from collections import Counter
from datetime import datetime, timedelta

sys.path.insert(0, ".")

from src.db.engine import SessionLocal
from src.models.game import Game, GamePlayByPlay

logger = logging.getLogger(__name__)


def collect_unclassified_text(
    *,
    days: int | None = None,
    game_id: str | None = None,
    limit: int = 1000,
) -> list[dict]:
    """Collect unclassified / unknown event_type rows from game_play_by_play."""
    with SessionLocal() as session:
        query = session.query(GamePlayByPlay).filter(GamePlayByPlay.event_type.in_(["unknown", "unclassified"]))

        if game_id:
            query = query.filter(GamePlayByPlay.game_id == game_id)
        elif days:
            cutoff = datetime.now() - timedelta(days=days)
            query = query.join(Game, GamePlayByPlay.game_id == Game.game_id).filter(Game.game_date >= cutoff.date())

        rows = query.order_by(GamePlayByPlay.id.desc()).limit(limit).all()
        return [
            {
                "id": r.id,
                "game_id": r.game_id,
                "inning": r.inning,
                "inning_half": r.inning_half,
                "text": r.play_description or "",
                "event_type": r.event_type,
            }
            for r in rows
        ]


def analyze_texts(rows: list[dict], top_words: int = 15) -> None:
    """Analyze unclassified texts for patterns."""
    if not rows:
        logger.info("No unclassified relay texts found. Good!")
        return

    texts = [r["text"] for r in rows if r["text"]]

    games = Counter(r["game_id"] for r in rows)
    logger.info(f"\nTotal unclassified entries: {len(rows)}")
    logger.info(f"Across {len(games)} game(s)")
    logger.info("\nTop games by unclassified count:")
    for game_id, count in games.most_common(10):
        logger.info(f"  {game_id}: {count}")

    prefixes = Counter()
    for text in texts:
        if ":" in text:
            prefix = text.split(":", 1)[0].strip()
            prefixes[prefix] += 1
        else:
            prefixes[f"[NO_COLON] {text[:50]}"] += 1

    logger.info(f"\nTop {top_words} most common prefixes:")
    for prefix, count in prefixes.most_common(top_words):
        logger.info(f"  {count:4d}x  {prefix}")

    result_keywords = Counter()
    for text in texts:
        if ":" in text:
            result = text.split(":", 1)[1].strip()
            for token in re.findall(r"[\w]+", result):
                result_keywords[token] += 1

    logger.info(f"\nTop {top_words} most common keywords in result text:")
    for word, count in result_keywords.most_common(top_words):
        logger.info(f"  {count:4d}x  {word}")

    from src.utils.relay_text import _RELAY_NOISE_PATTERNS, _RELAY_NOISE_TOKENS

    {t.lower() for t in _RELAY_NOISE_TOKENS}
    candidate_noise = Counter()
    for text in texts:
        text_lower = text.lower()
        is_known_noise = any(pattern.search(text) for pattern in _RELAY_NOISE_PATTERNS) or any(
            token.lower() in text_lower for token in _RELAY_NOISE_TOKENS
        )
        if not is_known_noise:
            segment = text.split(":")[0].strip() if ":" in text else text.strip()
            if len(segment) > 3:
                candidate_noise[segment] += 1

    logger.info(f"\nTop {top_words} potential new noise patterns (not in current filters):")
    for segment, count in candidate_noise.most_common(top_words):
        logger.info(f"  {count:4d}x  '{segment}'")

    logger.info(f"\nRecommendation: {'UPDATE _RELAY_NOISE_TOKENS' if candidate_noise else 'No update needed'}")


def main():
    parser = argparse.ArgumentParser(description="Audit unclassified relay text for noise pattern updates")
    parser.add_argument("--days", type=int, default=7, help="Look back N days (default: 7)")
    parser.add_argument("--game-id", type=str, help="Specific game ID to audit")
    parser.add_argument("--top-words", type=int, default=15, help="Number of top patterns to show")
    parser.add_argument("--limit", type=int, default=1000, help="Max rows to fetch")
    args = parser.parse_args()

    rows = collect_unclassified_text(
        days=args.days,
        game_id=args.game_id,
        limit=args.limit,
    )
    analyze_texts(rows, top_words=args.top_words)


if __name__ == "__main__":
    main()
