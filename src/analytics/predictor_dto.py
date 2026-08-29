"""Data Transfer Objects for KBO Game Matchup Prediction and Feature Engineering."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class MatchupFeatureVector:
    """Quantitative feature vector representing a game matchup."""

    game_id: str
    game_date: str
    home_team: str
    away_team: str
    stadium: str = "Jamsil"
    home_starter_name: str = "선발 미정"
    home_starter_fip: float = 4.20
    home_starter_whip: float = 1.35
    away_starter_name: str = "선발 미정"
    away_starter_fip: float = 4.20
    away_starter_whip: float = 1.35
    home_team_wrc_plus: float = 100.0
    home_team_ops: float = 0.750
    away_team_wrc_plus: float = 100.0
    away_team_ops: float = 0.750
    home_bullpen_era: float = 4.30
    away_bullpen_era: float = 4.30
    h2h_home_wins: int = 0
    h2h_away_wins: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert feature vector to dictionary."""
        return asdict(self)


@dataclass
class MatchupPredictionResult:
    """Result of a game win probability and score prediction."""

    game_id: str
    game_date: str
    home_team: str
    away_team: str
    stadium: str
    home_win_prob: float
    away_win_prob: float
    predicted_home_score: float
    predicted_away_score: float
    predicted_total_runs: float
    predicted_winner: str
    confidence_tier: str  # "HIGH", "MEDIUM", "TOSS_UP"
    key_factors: list[str] = field(default_factory=list)
    features: MatchupFeatureVector = field(
        default_factory=lambda: MatchupFeatureVector(
            game_id="",
            game_date="",
            home_team="",
            away_team="",
        )
    )

    def to_ascii_card(self) -> str:
        """Render a terminal ASCII matchup preview card."""
        home_bar_len = round(self.home_win_prob * 20)
        away_bar_len = 20 - home_bar_len
        bar_visual = f"[{'█' * home_bar_len}{'░' * away_bar_len}]"

        p_line = (
            f"║ [WIN PROB]     {self.home_team} {self.home_win_prob * 100:4.1f}%  {bar_visual}  "
            f"{self.away_team} {self.away_win_prob * 100:4.1f}%"
        )
        s_line = (
            f"║ [SCORE EXP]    {self.home_team} {self.predicted_home_score:4.1f}점  vs  "
            f"{self.away_team} {self.predicted_away_score:4.1f}점 (합계: {self.predicted_total_runs:4.1f})"
        )
        st_line = (
            f"║ [STARTERS]     {self.home_team} {self.features.home_starter_name} "
            f"({self.features.home_starter_fip:.2f}) vs {self.away_team} "
            f"{self.features.away_starter_name} ({self.features.away_starter_fip:.2f})"
        )
        off_line = (
            f"║ [OFFENSE]      {self.home_team} wRC+ {self.features.home_team_wrc_plus:5.1f}  vs  "
            f"{self.away_team} wRC+ {self.features.away_team_wrc_plus:5.1f}"
        )

        lines = [
            "╔════════════════════════════════════════════════════════════════════╗",
            f"║ ⚾ KBO MATCHUP WIN PREDICTION: {self.away_team} vs {self.home_team} ({self.game_date})".ljust(68) + "║",
            f"║ Stadium: {self.stadium} | Winner: [{self.predicted_winner}] (Tier: {self.confidence_tier})".ljust(68)
            + "║",
            "╠════════════════════════════════════════════════════════════════════╣",
            p_line.ljust(68) + "║",
            s_line.ljust(68) + "║",
            "╠════════════════════════════════════════════════════════════════════╣",
            st_line.ljust(68) + "║",
            off_line.ljust(68) + "║",
            "╠════════════════════════════════════════════════════════════════════╣",
            "║ [KEY MATCHUP FACTORS]                                              ║",
        ]
        lines.extend(f"║ • {factor}".ljust(68) + "║" for factor in self.key_factors[:3])
        lines.append("╚════════════════════════════════════════════════════════════════════╝")
        return "\n".join(lines)

    def to_markdown(self) -> str:
        """Render prediction results as a rich Markdown report."""
        f = self.features
        h_starter_label = f"{self.home_team} ({f.home_starter_name})"
        a_starter_label = f"{self.away_team} ({f.away_starter_name})"
        h_ops_str = f".{int(f.home_team_ops * 1000):03d}"
        a_ops_str = f".{int(f.away_team_ops * 1000):03d}"

        md = [
            f"# ⚾ KBO 경기 승부 예측: {self.away_team} @ {self.home_team} ({self.game_date})",
            "",
            f"- **경기 ID**: `{self.game_id}`",
            f"- **경기 장소**: `{self.stadium}`",
            f"- **예상 승리팀**: **`{self.predicted_winner}`** (신뢰도: `{self.confidence_tier}`)",
            "",
            "## 📊 승률 및 예상 스코어 분석",
            "",
            f"| 구분 | {self.home_team} (홈) | {self.away_team} (원정) |",
            "|:---|:---:|:---:|",
            f"| **예상 승률** | **`{self.home_win_prob * 100:.1f}%`** | **`{self.away_win_prob * 100:.1f}%`** |",
            f"| **예상 득점** | **`{self.predicted_home_score:.2f}점`** | **`{self.predicted_away_score:.2f}점`** |",
            f"| **예상 총 득점 (O/U)** | colspan=2 **`{self.predicted_total_runs:.2f}점`** |",
            "",
            "## ⚔️ 선발 투수 및 팀 전력 비교",
            "",
            f"| 세이버 지표 | {h_starter_label} | {a_starter_label} |",
            "|:---|:---:|:---:|",
            f"| **선발 투수 FIP** | `{f.home_starter_fip:.2f}` | `{f.away_starter_fip:.2f}` |",
            f"| **선발 투수 WHIP** | `{f.home_starter_whip:.2f}` | `{f.away_starter_whip:.2f}` |",
            f"| **팀 공격력 (wRC+)** | `{f.home_team_wrc_plus:.1f}` | `{f.away_team_wrc_plus:.1f}` |",
            f"| **팀 OPS** | `{h_ops_str}` | `{a_ops_str}` |",
            f"| **불펜 평균자책점** | `{f.home_bullpen_era:.2f}` | `{f.away_bullpen_era:.2f}` |",
            f"| **상대 전적 (시즌)** | `{f.h2h_home_wins}승` | `{f.h2h_away_wins}승` |",
            "",
            "## 🎯 핵심 승부 요인 (Key Matchup Factors)",
            "",
        ]
        md.extend(f"- 💡 {factor}" for factor in self.key_factors)
        md.append("")
        return "\n".join(md)

    def to_dict(self) -> dict[str, Any]:
        """Convert prediction result to dictionary."""
        return {
            "game_id": self.game_id,
            "game_date": self.game_date,
            "home_team": self.home_team,
            "away_team": self.away_team,
            "stadium": self.stadium,
            "home_win_prob": round(self.home_win_prob, 4),
            "away_win_prob": round(self.away_win_prob, 4),
            "predicted_home_score": round(self.predicted_home_score, 2),
            "predicted_away_score": round(self.predicted_away_score, 2),
            "predicted_total_runs": round(self.predicted_total_runs, 2),
            "predicted_winner": self.predicted_winner,
            "confidence_tier": self.confidence_tier,
            "key_factors": self.key_factors,
            "features": self.features.to_dict(),
        }


__all__ = [
    "MatchupFeatureVector",
    "MatchupPredictionResult",
]
