"""Declarative Season Manifest for all 45 KBO seasons (1982~2026) with zero UNKNOWN dispositions."""

from __future__ import annotations

from typing import ClassVar

from src.certification.historical.models import (
    DataDisposition,
    SeasonManifestItem,
    SeasonStatus,
)

CURRENT_ACTIVE_SEASON = 2026


class SeasonManifestRegistry:
    """Registry maintaining historical season contracts, boundaries, and expected data dispositions."""

    _MANIFEST: ClassVar[dict[int, SeasonManifestItem]] = {}

    @classmethod
    def _build_manifest(cls) -> dict[int, SeasonManifestItem]:
        """Construct declarative manifest for all 45 seasons (1982~2026) with empirical dispositions."""
        manifest: dict[int, SeasonManifestItem] = {}

        # 1982: Inaugural season (6 teams: OB, MBC, MBC, Sammi, Lotte, Haitai; 240 regular season games)
        manifest[1982] = SeasonManifestItem(
            season=1982,
            status=SeasonStatus.FINAL,
            expected_games_min=200,
            expected_games_max=260,
            pbp_disposition=DataDisposition.UNAVAILABLE,
            lineup_disposition=DataDisposition.CONDITIONAL,
            boxscore_disposition=DataDisposition.REQUIRED,
            season_totals_disposition=DataDisposition.REQUIRED,
            source_evidence="KBO Inaugural Official Archive (1982 paper scorebooks)",
            notes="Inaugural season (6 teams, 80 games/team)",
        )

        # 1983~1990: Early era (6~7 teams, 300~420 regular season games, pre-digital PBP)
        for yr in range(1983, 1991):
            manifest[yr] = SeasonManifestItem(
                season=yr,
                status=SeasonStatus.FINAL,
                expected_games_min=250,
                expected_games_max=460,
                pbp_disposition=DataDisposition.UNAVAILABLE,
                lineup_disposition=DataDisposition.CONDITIONAL,
                boxscore_disposition=DataDisposition.REQUIRED,
                season_totals_disposition=DataDisposition.REQUIRED,
                source_evidence=f"KBO Annual Official Archive ({yr})",
                notes="Early KBO era (pre-digital relay)",
            )

        # 1991~2000: Expansion era (8 teams, 504~532 games, paper boxscore era)
        for yr in range(1991, 2001):
            manifest[yr] = SeasonManifestItem(
                season=yr,
                status=SeasonStatus.FINAL,
                expected_games_min=450,
                expected_games_max=560,
                pbp_disposition=DataDisposition.UNAVAILABLE,
                lineup_disposition=DataDisposition.CONDITIONAL,
                boxscore_disposition=DataDisposition.REQUIRED,
                season_totals_disposition=DataDisposition.REQUIRED,
                source_evidence=f"KBO Official Archive ({yr})",
                notes="Classic 8-team era (pre-web PBP)",
            )

        # 2001~2009: Web portal boxscore era (lineups available online, PBP still unavailable/fragmented)
        for yr in range(2001, 2010):
            manifest[yr] = SeasonManifestItem(
                season=yr,
                status=SeasonStatus.FINAL,
                expected_games_min=450,
                expected_games_max=560,
                pbp_disposition=DataDisposition.UNAVAILABLE,
                lineup_disposition=DataDisposition.REQUIRED,
                boxscore_disposition=DataDisposition.REQUIRED,
                season_totals_disposition=DataDisposition.REQUIRED,
                source_evidence=f"KBO Web Archive ({yr})",
                notes="Web portal era with starter lineups",
            )

        # 2010~2012: Digital text-relay transition era (8 teams, PBP partially recorded)
        for yr in range(2010, 2013):
            manifest[yr] = SeasonManifestItem(
                season=yr,
                status=SeasonStatus.FINAL,
                expected_games_min=480,
                expected_games_max=560,
                pbp_disposition=DataDisposition.CONDITIONAL,
                lineup_disposition=DataDisposition.REQUIRED,
                boxscore_disposition=DataDisposition.REQUIRED,
                season_totals_disposition=DataDisposition.REQUIRED,
                source_evidence=f"KBO Digital Boxscore Archive ({yr})",
                notes="Digital transition era with conditional text relay",
            )

        # 2013~2014: 9-team era (NC Dinos joins, 576 regular season games, full PBP required)
        for yr in range(2013, 2015):
            manifest[yr] = SeasonManifestItem(
                season=yr,
                status=SeasonStatus.FINAL,
                expected_games_min=540,
                expected_games_max=600,
                pbp_disposition=DataDisposition.REQUIRED,
                lineup_disposition=DataDisposition.REQUIRED,
                boxscore_disposition=DataDisposition.REQUIRED,
                season_totals_disposition=DataDisposition.REQUIRED,
                source_evidence=f"KBO Digital Archive ({yr})",
                notes="9-team era with full PBP and starter lineups",
            )

        # 2015~2025: 10-team modern era (kt wiz joins, 720 regular season games)
        for yr in range(2015, 2026):
            manifest[yr] = SeasonManifestItem(
                season=yr,
                status=SeasonStatus.FINAL,
                expected_games_min=700,
                expected_games_max=740,
                pbp_disposition=DataDisposition.REQUIRED,
                lineup_disposition=DataDisposition.REQUIRED,
                boxscore_disposition=DataDisposition.REQUIRED,
                season_totals_disposition=DataDisposition.REQUIRED,
                source_evidence=f"KBO Modern 10-Team Archive ({yr})",
                notes="10-team modern era (144 games/team = 720 regular season games)",
            )

        # 2026: ACTIVE season (ongoing, evaluated as-of cutoff)
        manifest[CURRENT_ACTIVE_SEASON] = SeasonManifestItem(
            season=CURRENT_ACTIVE_SEASON,
            status=SeasonStatus.ACTIVE,
            expected_games_min=0,
            expected_games_max=740,
            pbp_disposition=DataDisposition.AS_OF_CUTOFF,
            lineup_disposition=DataDisposition.AS_OF_CUTOFF,
            boxscore_disposition=DataDisposition.AS_OF_CUTOFF,
            season_totals_disposition=DataDisposition.AS_OF_CUTOFF,
            source_evidence="Active KBO Season In-Flight Sync",
            notes="Active ongoing 2026 season evaluated up to current scheduled/played games",
        )

        return manifest

    @classmethod
    def register_manifest(cls, item: SeasonManifestItem) -> None:
        """Register or update a season manifest with empirical source evidence."""
        if not cls._MANIFEST:
            cls._MANIFEST = cls._build_manifest()
        cls._MANIFEST[item.season] = item

    @classmethod
    def get_manifest(cls, season: int) -> SeasonManifestItem:
        """Get declarative manifest for a specific season."""
        if not cls._MANIFEST:
            cls._MANIFEST = cls._build_manifest()
        if season not in cls._MANIFEST:
            status = SeasonStatus.ACTIVE if season >= CURRENT_ACTIVE_SEASON else SeasonStatus.FINAL
            pbp_disp = DataDisposition.AS_OF_CUTOFF if season >= CURRENT_ACTIVE_SEASON else DataDisposition.UNAVAILABLE
            return SeasonManifestItem(
                season=season,
                status=status,
                expected_games_min=0,
                expected_games_max=800,
                pbp_disposition=pbp_disp,
                source_evidence="Dynamic Fallback Registry",
                notes=f"Dynamically generated contract for season {season}",
            )
        return cls._MANIFEST[season]

    @classmethod
    def list_all_seasons(cls, start: int = 1982, end: int = CURRENT_ACTIVE_SEASON) -> list[SeasonManifestItem]:
        """List manifests for all seasons in range [start, end]."""
        if not cls._MANIFEST:
            cls._MANIFEST = cls._build_manifest()
        return [cls.get_manifest(yr) for yr in range(start, end + 1)]


__all__ = [
    "CURRENT_ACTIVE_SEASON",
    "SeasonManifestRegistry",
]
