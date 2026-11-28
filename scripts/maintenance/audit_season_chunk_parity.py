#!/usr/bin/env python3
"""Read-only parity audit comparing legacy #part chunks against generated natural-key season chunks."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from src.cli.rag.build_rag_index import _iter_batting_chunks, _iter_pitching_chunks
from src.db.engine import SessionLocal
from src.models.rag_chunk import RagChunk

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session


@dataclass(frozen=True, slots=True)
class GroupParityResult:
    """Parity outcome for a single ``id=N#part*`` legacy group."""

    source_table: str
    group_key: str
    part_count: int
    player_id: int | None
    season: int | None
    matched_source_row_id: str | None
    legacy_prose_len: int
    generated_content_len: int | None
    richer_or_equal_text: bool
    has_structured_sections: bool
    has_sabermetrics_parity: bool
    has_rich_meta: bool
    legacy_preview: str
    generated_preview: str | None


def _strip_meta_footer(joined_text: str) -> str:
    """Return human-readable prose from joined ``#part`` chunks before ``[META]`` / ``[출처]``."""
    cut_positions = [pos for marker in ("[META]", "[출처]") if (pos := joined_text.find(marker)) != -1]
    prose = joined_text[: min(cut_positions)] if cut_positions else joined_text
    return re.sub(r"\s+", " ", prose).strip()


def _extract_meta_fields(joined_text: str) -> tuple[dict[str, Any], int | None, int | None]:
    """Parse JSON ``[META]`` when complete, falling back to regex for truncated splits."""
    decoder = json.JSONDecoder()
    idx = joined_text.find("[META]")
    meta: dict[str, Any] = {}
    if idx != -1:
        after = joined_text[idx + len("[META]") :].lstrip()
        for candidate in (after, re.sub(r"\n+", "", after)):
            try:
                parsed, _ = decoder.raw_decode(candidate)
                if isinstance(parsed, dict):
                    meta = parsed
                    break
            except ValueError:
                pass
    pid = meta.get("player_id")
    season = meta.get("season")
    if pid is None:
        m_pid = re.search(r'"player_id"\s*:\s*(\d+)', joined_text)
        if m_pid:
            pid = int(m_pid.group(1))
    if season is None:
        m_season = re.search(r'"season"\s*:\s*(\d{4})', joined_text)
        if m_season:
            season = int(m_season.group(1))
    return meta, int(pid) if pid is not None else None, int(season) if season is not None else None


def _score_candidate(
    source_table: str,
    candidate: dict[str, Any],
    legacy_title: str,
    legacy_prose: str,
    legacy_meta: dict[str, Any],
) -> tuple[int, int]:
    """Rank multiple leagues/teams for the same ``(player_id, season)`` by closest evidence match."""
    score = 0
    meta = candidate["meta"]
    content = candidate["content"]
    team_name = str(meta.get("team") or "")
    team_code = str(meta.get("team_code") or "")
    if team_name and (team_name in legacy_title or team_name == legacy_meta.get("team")):
        score += 10
    elif team_code and team_code in str(legacy_meta.get("aliases") or ""):
        score += 6

    if source_table == "player_season_batting":
        avg = meta.get("avg")
        if isinstance(avg, (int, float)) and f"{avg:.3f}" in legacy_title:
            score += 8
        hr = meta.get("hr")
        if hr is not None and f"홈런 {hr}" in legacy_prose:
            score += 3
        pa = meta.get("pa")
        if pa is not None and pa == legacy_meta.get("pa"):
            score += 5
    else:
        era = meta.get("era")
        if isinstance(era, (int, float)) and f"{era:.2f}" in legacy_title:
            score += 8
        wins = meta.get("wins")
        if wins is not None and f"{wins}승" in legacy_title:
            score += 4
        ip = meta.get("ip")
        if isinstance(ip, (int, float)) and ip == legacy_meta.get("ip"):
            score += 5

    return (score, len(content))


def _evaluate_table(session: Session, source_table: str) -> tuple[list[GroupParityResult], int]:
    """Compare all ``REINDEX_REQUIRED`` groups for ``source_table`` against generated natural chunks."""
    iterator = _iter_batting_chunks if source_table == "player_season_batting" else _iter_pitching_chunks
    generated_chunks = list(iterator(session, None, None))
    by_player_season: dict[tuple[int, int], list[dict[str, Any]]] = {}
    for chunk in generated_chunks:
        meta = chunk["meta"]
        pid = meta.get("player_id")
        season = meta.get("season")
        if pid is not None and season is not None:
            by_player_season.setdefault((int(pid), int(season)), []).append(chunk)

    rows = session.execute(
        select(
            RagChunk.source_row_id,
            RagChunk.title,
            RagChunk.season_year,
            RagChunk.content,
        )
        .where(
            RagChunk.source_table == source_table,
            RagChunk.index_status == "REINDEX_REQUIRED",
        )
        .order_by(RagChunk.source_row_id),
    ).all()

    grouped: dict[str, list[tuple[int, str, str, int | None, str]]] = {}
    for source_row_id, title, season_year, content in rows:
        prefix, _sep, part_str = str(source_row_id).partition("#part")
        part_no = int(part_str) if part_str.isdigit() else 0
        grouped.setdefault(prefix, []).append(
            (part_no, str(source_row_id), str(title or ""), season_year, str(content or ""))
        )

    results: list[GroupParityResult] = []
    for prefix, parts in sorted(grouped.items()):
        parts.sort(key=lambda item: item[0])
        joined_text = "\n".join(item[4] for item in parts)
        legacy_title = parts[0][2]
        legacy_meta, pid, season = _extract_meta_fields(joined_text)
        if season is None and parts[0][3] is not None:
            season = int(parts[0][3])
        legacy_prose = _strip_meta_footer(joined_text)

        candidates = by_player_season.get((pid, season), []) if (pid is not None and season is not None) else []
        best: dict[str, Any] | None = None
        if candidates:
            best = max(
                candidates,
                key=lambda cand: _score_candidate(source_table, cand, legacy_title, legacy_prose, legacy_meta),
            )

        if best is None:
            results.append(
                GroupParityResult(
                    source_table=source_table,
                    group_key=prefix,
                    part_count=len(parts),
                    player_id=pid,
                    season=season,
                    matched_source_row_id=None,
                    legacy_prose_len=len(legacy_prose),
                    generated_content_len=None,
                    richer_or_equal_text=False,
                    has_structured_sections=False,
                    has_sabermetrics_parity=False,
                    has_rich_meta=False,
                    legacy_preview=legacy_prose,
                    generated_preview=None,
                )
            )
            continue

        gen_content = str(best["content"])
        gen_meta = best["meta"]
        gen_prose = re.sub(r"\s+", " ", gen_content).strip()
        has_structured = all(tag in gen_content for tag in ("[TL;DR]", "[핵심 문장]", "[상세]"))
        has_saber_in_legacy = any(token in joined_text for token in ("ISO", "BABIP", "XR", "K/9", "BB/9", "K/BB"))
        has_saber_in_gen = "- 세이버메트릭스:" in gen_content
        saber_parity = has_saber_in_gen or not has_saber_in_legacy
        if source_table == "player_season_batting":
            has_rich_meta = bool(gen_meta.get("aliases")) and "avg" in gen_meta and "ops" in gen_meta
        else:
            has_rich_meta = bool(gen_meta.get("aliases")) and "era" in gen_meta and "wins" in gen_meta

        results.append(
            GroupParityResult(
                source_table=source_table,
                group_key=prefix,
                part_count=len(parts),
                player_id=pid,
                season=season,
                matched_source_row_id=str(best["source_row_id"]),
                legacy_prose_len=len(legacy_prose),
                generated_content_len=len(gen_content),
                richer_or_equal_text=len(gen_prose) >= len(legacy_prose),
                has_structured_sections=has_structured,
                has_sabermetrics_parity=saber_parity,
                has_rich_meta=has_rich_meta,
                legacy_preview=joined_text,
                generated_preview=gen_content,
            )
        )

    return results, len(generated_chunks)


def _summarize_table(
    results: Sequence[GroupParityResult],
    total_generated: int,
    sample_count: int,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Summarize parity results and extract optional before/after samples for one table."""
    counts: Counter[str] = Counter()
    failures: list[dict[str, Any]] = []
    for item in results:
        counts["groups"] += 1
        counts["rows"] += item.part_count
        counts["matched" if item.matched_source_row_id is not None else "unmatched"] += 1
        counts["richer_or_equal_text"] += int(item.richer_or_equal_text)
        counts["has_structured_sections"] += int(item.has_structured_sections)
        counts["has_sabermetrics_parity"] += int(item.has_sabermetrics_parity)
        counts["has_rich_meta"] += int(item.has_rich_meta)

        is_passing = (
            item.matched_source_row_id is not None
            and item.richer_or_equal_text
            and item.has_structured_sections
            and item.has_sabermetrics_parity
            and item.has_rich_meta
        )
        if not is_passing and len(failures) < 10:
            failures.append(
                {
                    "group_key": item.group_key,
                    "player_id": item.player_id,
                    "season": item.season,
                    "matched": item.matched_source_row_id,
                    "legacy_len": item.legacy_prose_len,
                    "gen_len": item.generated_content_len,
                    "richer": item.richer_or_equal_text,
                    "structured": item.has_structured_sections,
                    "saber": item.has_sabermetrics_parity,
                    "meta": item.has_rich_meta,
                }
            )

    summary = {
        "generated_kbo1_chunks": total_generated,
        "legacy_groups": counts["groups"],
        "legacy_part_rows": counts["rows"],
        "matched_groups": counts["matched"],
        "unmatched_groups": counts["unmatched"],
        "richer_or_equal_text": counts["richer_or_equal_text"],
        "has_structured_sections": counts["has_structured_sections"],
        "has_sabermetrics_parity": counts["has_sabermetrics_parity"],
        "has_rich_meta": counts["has_rich_meta"],
        "failure_samples": failures,
    }
    table_samples: list[dict[str, Any]] = []
    if sample_count > 0:
        for item in [entry for entry in results if entry.matched_source_row_id is not None][:sample_count]:
            table_samples.append(
                {
                    "group_key": item.group_key,
                    "matched_source_row_id": item.matched_source_row_id,
                    "legacy_text": item.legacy_preview,
                    "generated_text": item.generated_preview,
                }
            )
    return summary, table_samples


def run_parity_audit(sample_count: int = 2) -> dict[str, Any]:
    """Run the read-only parity audit across batting and pitching ``REINDEX_REQUIRED`` groups."""
    tables: dict[str, Any] = {}
    samples: dict[str, list[dict[str, Any]]] = {}

    with SessionLocal() as session:
        for table in ("player_season_batting", "player_season_pitching"):
            results, total_generated = _evaluate_table(session, table)
            summary, table_samples = _summarize_table(results, total_generated, sample_count)
            tables[table] = summary
            if table_samples:
                samples[table] = table_samples

    return {
        "read_only": True,
        "total_kbo1_source_chunks": sum(v["generated_kbo1_chunks"] for v in tables.values()),
        "tables": tables,
        "samples": samples,
    }


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint for read-only season chunk parity auditing."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="Emit full machine-readable JSON output.")
    parser.add_argument("--sample", type=int, default=2, help="Number of before/after samples per table.")
    args = parser.parse_args(argv)

    report = run_parity_audit(sample_count=max(0, args.sample))
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0

    print(f"Total KBO1 natural-key chunks to build: {report['total_kbo1_source_chunks']}")
    for table, summary in report["tables"].items():
        print(
            f"\n[{table}] generated_kbo1={summary['generated_kbo1_chunks']} "
            f"legacy_groups={summary['legacy_groups']} (rows={summary['legacy_part_rows']})"
        )
        print(
            f"  matched={summary['matched_groups']}/{summary['legacy_groups']} "
            f"unmatched={summary['unmatched_groups']} "
            f"richer_or_equal={summary['richer_or_equal_text']}/{summary['legacy_groups']} "
            f"structured={summary['has_structured_sections']}/{summary['legacy_groups']} "
            f"saber_parity={summary['has_sabermetrics_parity']}/{summary['legacy_groups']} "
            f"rich_meta={summary['has_rich_meta']}/{summary['legacy_groups']}"
        )
        if summary["failure_samples"]:
            print("  failure_samples:", summary["failure_samples"][:5])
    for table, items in report["samples"].items():
        if not items:
            continue
        print(f"\n=== Samples ({table}) ===")
        for item in items:
            print(f"-- {item['group_key']} -> {item['matched_source_row_id']} --")
            print("[BEFORE]\n" + str(item["legacy_text"]))
            print("[AFTER]\n" + str(item["generated_text"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
