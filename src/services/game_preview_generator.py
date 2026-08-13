"""Game Preview Generator Service blending matchups, milestones, and notices."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from src.services.context_aggregator import ContextAggregator
from src.services.rag_search_engine import RagSearchEngine

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class GamePreviewGenerator:
    """Service to generate comprehensive pregame preview cards."""

    def __init__(self, session: Session) -> None:
        """Initialize generator.

        Args:
            session: DB Session.

        """
        self.session = session
        self.aggregator = ContextAggregator(session)
        self.rag_engine = RagSearchEngine(session)

    def generate_preview(
        self,
        away_team: str,
        home_team: str,
        season: int = 2026,
    ) -> dict[str, Any]:
        """Generate structured game preview card.

        Args:
            away_team: Away team code (e.g. 'LG').
            home_team: Home team code (e.g. 'KIA').
            season: Season year.

        Returns:
            Preview report dictionary with markdown summary.

        """
        # 1. Fetch milestone alerts for competing teams
        milestones = self.aggregator.get_milestone_alerts(
            away_team=away_team,
            home_team=home_team,
            season=season,
        )

        # 2. Fetch recent KBO notices
        notices = self.aggregator.get_recent_notices(limit=3)

        # 3. Search RAG context chunks for team keywords
        rag_chunks = self.rag_engine.search(query=f"{away_team} {home_team}", top_k=3)

        # 4. Formulate markdown text
        md_lines = [
            f"# ⚾ [매치업 프리뷰] {away_team} vs {home_team} ({season} 시즌)",
            "",
            "## 🚨 [대기록 임박 선수 경고]",
        ]

        if milestones:
            md_lines.extend(f"- **{m['player_name']}** ({m['team_code']}): {m['alert_message']}" for m in milestones)
        else:
            md_lines.append("- 현재 양 팀에 임박한 대기록 달성 선수가 없습니다.")

        md_lines.extend(["", "## 📢 [최신 KBO 행정 공시 & 소식]"])
        if notices:
            md_lines.extend(
                f"- [{n['published_date']}] [{n['category']}] [{n['title']}]({n['source_url']})" for n in notices
            )
        else:
            md_lines.append("- 등록된 최신 공시가 없습니다.")

        if rag_chunks:
            md_lines.extend(["", "## 🔍 [관련 RAG 지식 검색]"])
            md_lines.extend(f"- **{r['title']}**: {r['content'][:100]}..." for r in rag_chunks)

        markdown_report = "\n".join(md_lines)

        return {
            "season": season,
            "matchup": f"{away_team} vs {home_team}",
            "away_team": away_team,
            "home_team": home_team,
            "milestone_alerts": milestones,
            "recent_notices": notices,
            "rag_chunks": rag_chunks,
            "markdown_report": markdown_report,
        }
