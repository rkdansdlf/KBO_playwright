"""
Generate a KBO win expectancy matrix from historical game events.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, TypeAlias

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from src.db.engine import SessionLocal

WinExpectancyMatrix: TypeAlias = dict[int, dict[int, dict[int, dict[int, dict[str, float]]]]]


def parse_runners(value: Any) -> str:
    raw = str(value or "")
    if len(raw) != 3:
        return "000"
    return "".join("1" if char != "-" else "0" for char in raw)


def build_matrix(
    *,
    max_inning: int,
    score_cap: int,
    min_sample_size: int,
) -> WinExpectancyMatrix:
    load_dotenv()
    print("📊 Extracting game event states and final outcomes...")

    query = text(
        """
        SELECT
            e.inning,
            e.inning_half,
            e.score_diff,
            e.outs,
            e.bases_before,
            CASE WHEN g.home_score > g.away_score THEN 1 ELSE 0 END AS home_won
        FROM game_events e
        JOIN game g ON e.game_id = g.game_id
        WHERE g.game_status IN ('COMPLETED', 'FINISHED', '종료')
          AND e.inning <= :max_inning
        """
    )

    with SessionLocal() as session:
        df = pd.read_sql(query, session.connection(), params={"max_inning": max_inning})

    print(f"✅ Extracted {len(df)} event points.")
    if df.empty:
        return {}

    df["is_bottom"] = df["inning_half"].apply(lambda value: 1 if str(value).upper() == "BOTTOM" else 0)
    df["runners_str"] = df["bases_before"].apply(parse_runners)
    df["score_diff_clipped"] = df["score_diff"].clip(-score_cap, score_cap)

    state_cols = ["inning", "is_bottom", "score_diff_clipped", "outs", "runners_str"]
    we_matrix = df.groupby(state_cols)["home_won"].agg(["mean", "count"]).reset_index()
    we_matrix.rename(columns={"mean": "win_prob", "count": "sample_size"}, inplace=True)
    we_matrix = we_matrix[we_matrix["sample_size"] >= min_sample_size]

    result: WinExpectancyMatrix = {}
    for _, row in we_matrix.iterrows():
        inning = int(row["inning"])
        is_bottom = int(row["is_bottom"])
        score_diff = int(row["score_diff_clipped"])
        outs = int(row["outs"])
        runners = str(row["runners_str"])
        win_prob = round(float(row["win_prob"]), 4)
        result.setdefault(inning, {}).setdefault(is_bottom, {}).setdefault(score_diff, {}).setdefault(outs, {})[
            runners
        ] = win_prob

    print(f"📈 Total states covered: {len(we_matrix)}")
    return result


def write_matrix(matrix: WinExpectancyMatrix, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as file:
        json.dump(matrix, file, ensure_ascii=False, indent=2)
    print(f"✨ Win expectancy matrix saved to {output_path}")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate a KBO win expectancy matrix JSON file.")
    parser.add_argument(
        "--output",
        type=Path,
        default=PROJECT_ROOT / "data" / "we_matrix_kbo.json",
        help="Output JSON path. Defaults to data/we_matrix_kbo.json.",
    )
    parser.add_argument("--max-inning", type=int, default=9, help="Maximum inning to include.")
    parser.add_argument("--score-cap", type=int, default=7, help="Clip score differential to +/- this value.")
    parser.add_argument("--min-sample-size", type=int, default=1, help="Minimum event count required per state.")
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    matrix = build_matrix(
        max_inning=args.max_inning,
        score_cap=args.score_cap,
        min_sample_size=args.min_sample_size,
    )
    write_matrix(matrix, args.output)


if __name__ == "__main__":
    main()
