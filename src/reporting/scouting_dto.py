"""DTOs and data models for KBO Sabermetric Scouting Reports."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class PlayerRole(StrEnum):
    """Player position category."""

    BATTER = "BATTER"
    PITCHER = "PITCHER"


@dataclass
class ScoutingDimension:
    """A discrete 5-axis scouting dimension evaluated on a 0-100 percentile scale."""

    name: str
    score: float
    primary_metric: str
    grade: str
    description: str

    def to_dict(self) -> dict[str, Any]:
        """Convert dimension to dictionary."""
        return {
            "name": self.name,
            "score": round(self.score, 1),
            "primary_metric": self.primary_metric,
            "grade": self.grade,
            "description": self.description,
        }


@dataclass
class ScoutingReport:
    """Comprehensive multi-dimensional sabermetric scouting report for a player."""

    player_id: int
    player_name: str
    team_code: str
    season: int
    role: PlayerRole
    overall_grade: str
    scouting_tier: str
    dimensions: list[ScoutingDimension] = field(default_factory=list)
    strengths: list[str] = field(default_factory=list)
    weaknesses: list[str] = field(default_factory=list)
    classic_stats: dict[str, Any] = field(default_factory=dict)
    advanced_stats: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert scouting report to dictionary."""
        return {
            "player_id": self.player_id,
            "player_name": self.player_name,
            "team_code": self.team_code,
            "season": self.season,
            "role": self.role.value,
            "overall_grade": self.overall_grade,
            "scouting_tier": self.scouting_tier,
            "dimensions": [d.to_dict() for d in self.dimensions],
            "strengths": self.strengths,
            "weaknesses": self.weaknesses,
            "classic_stats": self.classic_stats,
            "advanced_stats": self.advanced_stats,
        }

    def to_ascii_card(self) -> str:
        """Render a terminal-friendly ASCII 5-axis visual card."""
        bar_width = 20
        lines = [
            "╔" + "═" * 68 + "╗",
            f"║ ⚾ KBO SABERMETRIC SCOUTING REPORT: {self.player_name} ({self.team_code}, {self.season})".ljust(69)
            + "║",
            f"║ Overall Grade: [{self.overall_grade}] | Tier: {self.scouting_tier}".ljust(69) + "║",
            "╠" + "═" * 68 + "╣",
            "║ [5-AXIS PERFORMANCE RADAR / PERCENTILE METERS]".ljust(69) + "║",
        ]

        for d in self.dimensions:
            filled = round((d.score / 100.0) * bar_width)
            filled = max(0, min(bar_width, filled))
            bar = "█" * filled + "░" * (bar_width - filled)
            metric_str = f"[{bar}] {d.score:5.1f}% ({d.grade}) - {d.primary_metric}"
            dim_label = f"• {d.name}:".ljust(22)
            lines.append(f"║ {dim_label} {metric_str}".ljust(69) + "║")

        lines.extend(
            [
                "╠" + "═" * 68 + "╣",
                "║ [KEY SCOUTING SUMMARY]".ljust(69) + "║",
            ]
        )

        if self.strengths:
            lines.append(f"║ 🌟 Strengths:  {', '.join(self.strengths)}".ljust(69) + "║")
        if self.weaknesses:
            lines.append(f"║ ⚠️  Weaknesses: {', '.join(self.weaknesses)}".ljust(69) + "║")

        lines.append("╚" + "═" * 68 + "╝")
        return "\n".join(lines)

    def to_markdown(self) -> str:
        """Render comprehensive Markdown scouting report."""
        lines = [
            f"# ⚾ {self.player_name} ({self.team_code}) {self.season} 시즌 세이버메트릭스 스카우팅 리포트",
            "",
            f"- **선수 ID**: `{self.player_id}`",
            f"- **소속 구단**: `{self.team_code}`",
            f"- **대상 시즌**: `{self.season}`",
            f"- **포지션 분류**: `{self.role.value}`",
            f"- **종합 평가 등급**: **`{self.overall_grade}`** (`{self.scouting_tier}`)",
            "",
            "## 📊 5대 핵심 차원 역량 평가 (5-Axis Percentile)",
            "",
            "| 역량 차원 | 백분위수 (상위 %) | 등급 | 주요 대표 지표 | 분석 및 평가 요약 |",
            "|:---|:---:|:---:|:---|:---|",
        ]

        for d in self.dimensions:
            top_pct = max(0.1, round(100.0 - d.score, 1))
            lines.append(
                f"| **{d.name}** | `{d.score:.1f}%` (상위 {top_pct}%) | **{d.grade}** | "
                f"`{d.primary_metric}` | {d.description} |"
            )

        lines.extend(
            [
                "",
                "## 🎯 스카우팅 총평 (Scouting Summary)",
                "",
                f"- 🌟 **핵심 강점**: {', '.join(self.strengths) if self.strengths else '리그 평균 수준'}",
                f"- ⚠️ **보완 및 약점**: {', '.join(self.weaknesses) if self.weaknesses else '특이 약점 없음'}",
                "",
                "## 📈 세부 기록 및 세이버메트릭스",
                "",
                "### 1. 클래식 스탯",
                "```json",
                str(self.classic_stats),
                "```",
                "",
                "### 2. 세이버메트릭스 스탯",
                "```json",
                str(self.advanced_stats),
                "```",
            ]
        )

        return "\n".join(lines)


__all__ = ["PlayerRole", "ScoutingDimension", "ScoutingReport"]
