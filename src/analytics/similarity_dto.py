"""Data Transfer Objects for KBO Player Similarity Search and Head-to-Head Comparison."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

MAX_BAR_WIDTH = 20
PERCENT_DIVISOR = 5.0


class PlayerRole(StrEnum):
    """Player position role category."""

    BATTER = "BATTER"
    PITCHER = "PITCHER"


@dataclass
class PlayerVector:
    """Quantitative 5-axis player performance vector for similarity analysis."""

    player_id: int
    player_name: str
    team_code: str
    season: int
    role: PlayerRole
    dimensions: dict[str, float] = field(default_factory=dict)
    key_stats: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert player vector to dictionary."""
        return {
            "player_id": self.player_id,
            "player_name": self.player_name,
            "team_code": self.team_code,
            "season": self.season,
            "role": self.role.value,
            "dimensions": {k: round(v, 1) for k, v in self.dimensions.items()},
            "key_stats": self.key_stats,
        }


@dataclass
class SimilarPlayerMatch:
    """A matched player result ranked by cosine similarity."""

    player: PlayerVector
    similarity_score: float  # 0.000 to 1.000
    style_tag: str
    common_strengths: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert match result to dictionary."""
        return {
            "player": self.player.to_dict(),
            "similarity_score": round(self.similarity_score, 4),
            "style_tag": self.style_tag,
            "common_strengths": self.common_strengths,
        }


@dataclass
class PlayerSimilarityResult:
    """Result of searching for historical and contemporary similar players."""

    target_player: PlayerVector
    matches: list[SimilarPlayerMatch] = field(default_factory=list)

    def to_ascii_card(self) -> str:
        """Render terminal ASCII similarity ranking card."""
        t = self.target_player
        lines = [
            "╔════════════════════════════════════════════════════════════════════╗",
            f"║ 🔍 KBO PLAYER SIMILARITY SEARCH: {t.player_name} ({t.team_code}, {t.season})".ljust(68) + "║",
            f"║ Position: {t.role.value:<8} | Matches Found: {len(self.matches)}".ljust(68) + "║",
            "╠════════════════════════════════════════════════════════════════════╣",
            "║ Rank | Player (Team, Season)   | Similarity | Style Profile        ║",
            "╟──────┼─────────────────────────┼────────────┼──────────────────────╢",
        ]
        for idx, m in enumerate(self.matches, start=1):
            p = m.player
            p_label = f"{p.player_name} ({p.team_code}, {p.season})"
            sim_pct = f"{m.similarity_score * 100.0:5.1f}%"
            line = f"║  #{idx:<2} | {p_label:<23} |   {sim_pct:<7}  | {m.style_tag:<20} ║"
            lines.append(line)

        lines.append("╚════════════════════════════════════════════════════════════════════╝")
        return "\n".join(lines)

    def to_markdown(self) -> str:
        """Render similarity search results as a Markdown report."""
        t = self.target_player
        md = [
            f"# 🔍 KBO 선수 유사도 분석 리포트: {t.player_name} ({t.team_code}, {t.season})",
            "",
            f"- **선수 ID**: `{t.player_id}`",
            f"- **포지션/역할**: `{t.role.value}`",
            "",
            "## 🏆 역대 가장 유사한 KBO 선수 랭킹 (Top Similar Players)",
            "",
            "| 순위 | 선수명 | 소속팀 | 시즌 | 유사도 (Similarity) | 플레이스타일 유형 | 공통 강점 |",
            "|:---:|:---|:---:|:---:|:---:|:---|:---|",
        ]
        for idx, m in enumerate(self.matches, start=1):
            p = m.player
            strengths_str = ", ".join(m.common_strengths) if m.common_strengths else "전반적 균형"
            md.append(
                f"| {idx} | **{p.player_name}** | `{p.team_code}` | {p.season} | "
                f"**`{m.similarity_score * 100.0:.1f}%`** | {m.style_tag} | {strengths_str} |"
            )
        md.append("")
        return "\n".join(md)

    def to_dict(self) -> dict[str, Any]:
        """Convert similarity search result to dictionary."""
        return {
            "target_player": self.target_player.to_dict(),
            "matches": [m.to_dict() for m in self.matches],
        }


@dataclass
class HeadToHeadComparisonResult:
    """Result of 1:1 head-to-head sabermetric comparison between two players."""

    player1: PlayerVector
    player2: PlayerVector
    similarity_score: float
    dimension_diffs: dict[str, float] = field(default_factory=dict)
    advantage_player1: list[str] = field(default_factory=list)
    advantage_player2: list[str] = field(default_factory=list)
    verdict_summary: str = ""

    def to_ascii_radar(self) -> str:
        """Render terminal ASCII 2-player 5-axis comparison radar bar card."""
        p1 = self.player1
        p2 = self.player2
        sim_pct = f"{self.similarity_score * 100.0:.1f}%"

        lines = [
            "╔════════════════════════════════════════════════════════════════════╗",
            f"║ ⚔️ KBO 1:1 COMPARISON: {p1.player_name} ({p1.season}) vs {p2.player_name} ({p2.season})".ljust(68) + "║",
            f"║ Style Similarity: {sim_pct:<6} | Verdict: [{self.verdict_summary}]".ljust(68) + "║",
            "╠════════════════════════════════════════════════════════════════════╣",
            f"║ 5-Axis Dimension       {p1.player_name:<10} (P1)  vs  {p2.player_name:<10} (P2)          ║",
            "╟──────────────────────┬──────────────────────┬──────────────────────╢",
        ]

        all_dims = list(p1.dimensions.keys())
        for dim in all_dims:
            s1 = p1.dimensions.get(dim, 50.0)
            s2 = p2.dimensions.get(dim, 50.0)
            bar1_len = round(s1 / PERCENT_DIVISOR)
            bar2_len = round(s2 / PERCENT_DIVISOR)
            bar1 = f"{'█' * bar1_len}{'░' * (MAX_BAR_WIDTH - bar1_len)}"
            bar2 = f"{'█' * bar2_len}{'░' * (MAX_BAR_WIDTH - bar2_len)}"
            diff = s1 - s2
            diff_icon = f"+{diff:.1f}" if diff > 0 else f"{diff:.1f}"

            lines.append(f"║ {dim:<20} │ P1: [{bar1}] {s1:4.1f} ║".ljust(68) + "║")
            lines.append(f"║ (Diff: {diff_icon:>5})         │ P2: [{bar2}] {s2:4.1f} ║".ljust(68) + "║")
            lines.append("╟──────────────────────┴─────────────────────────────────────────────╢")

        if lines[-1].startswith("╟"):
            lines[-1] = "╠════════════════════════════════════════════════════════════════════╣"

        lines.append(f"║ [{p1.player_name} 우세 항목] {', '.join(self.advantage_player1) or '없음'}".ljust(68) + "║")
        lines.append(f"║ [{p2.player_name} 우세 항목] {', '.join(self.advantage_player2) or '없음'}".ljust(68) + "║")
        lines.append("╚════════════════════════════════════════════════════════════════════╝")
        return "\n".join(lines)

    def to_markdown(self) -> str:
        """Render 1:1 comparison results as a rich Markdown report."""
        p1 = self.player1
        p2 = self.player2
        h_line = f"| 역량 평가 항목 | {p1.player_name} ({p1.season}) | {p2.player_name} ({p2.season}) | 격차 | 우세 |"

        md = [
            f"# ⚔️ KBO 1:1 세이버메트릭스 비교: {p1.player_name} vs {p2.player_name}",
            "",
            f"- **선수 1**: `{p1.player_name}` ({p1.team_code}, {p1.season}시즌, {p1.role.value})",
            f"- **선수 2**: `{p2.player_name}` ({p2.team_code}, {p2.season}시즌, {p2.role.value})",
            f"- **플레이스타일 유사도**: **`{self.similarity_score * 100.0:.1f}%`**",
            f"- **종합 비교 판정**: **`{self.verdict_summary}`**",
            "",
            "## 📊 5대 역량 세이버메트릭스 비교 (5-Axis Percentile Comparison)",
            "",
            h_line,
            "|:---|:---:|:---:|:---:|:---:|",
        ]
        for dim in p1.dimensions:
            s1 = p1.dimensions.get(dim, 50.0)
            s2 = p2.dimensions.get(dim, 50.0)
            diff = s1 - s2
            diff_str = f"+{diff:.1f}" if diff > 0 else f"{diff:.1f}"
            leader = p1.player_name if diff > 0 else p2.player_name if diff < 0 else "동률"
            md.append(f"| **{dim}** | `{s1:.1f}` | `{s2:.1f}` | `{diff_str}` | **{leader}** |")

        md.extend(
            [
                "",
                "## 🎯 선수별 비교 강점",
                "",
                f"- 🟢 **{p1.player_name} 우위**: {', '.join(self.advantage_player1) or '없음'}",
                f"- 🔵 **{p2.player_name} 우위**: {', '.join(self.advantage_player2) or '없음'}",
                "",
            ]
        )
        return "\n".join(md)

    def to_dict(self) -> dict[str, Any]:
        """Convert head-to-head comparison result to dictionary."""
        return {
            "player1": self.player1.to_dict(),
            "player2": self.player2.to_dict(),
            "similarity_score": round(self.similarity_score, 4),
            "dimension_diffs": {k: round(v, 1) for k, v in self.dimension_diffs.items()},
            "advantage_player1": self.advantage_player1,
            "advantage_player2": self.advantage_player2,
            "verdict_summary": self.verdict_summary,
        }


__all__ = [
    "MAX_BAR_WIDTH",
    "PERCENT_DIVISOR",
    "HeadToHeadComparisonResult",
    "PlayerRole",
    "PlayerSimilarityResult",
    "PlayerVector",
    "SimilarPlayerMatch",
]
