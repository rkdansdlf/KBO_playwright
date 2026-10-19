"""Apples-to-apples embedding model comparison on the same random sample (numpy eval)."""
import json
import math
import os
import random
import sys
import time
import traceback

from dotenv import load_dotenv
load_dotenv()

import httpx
import numpy as np
from sqlalchemy import text

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from src.db.vector_engine import get_vector_session

API_KEY = os.getenv("OPENROUTER_API_KEY")
SAMPLE = 1000
SEED = 7
TOP_K = 5

QUERIES = [
    ("2025년 KBO 타격 1위 선수는 누구야?", "stat_rankings"),
    ("김도영 2025 시즌에 홈런 몇 개 쳤어?", "player_season_batting"),
    ("류현진이 현재 소속된 팀은 어디야?", "player_basic"),
    ("2025년 KIA 타이거즈의 정규시즌 순위는?", "team_standings_daily"),
    ("최근 선수 이동이나 트레이드 있었어?", "player_movements"),
    ("두산 베어스 공식 이벤트에 뭐가 등록되어 있어?", "team_events"),
    ("2025년 시즌 평균자책점이 가장 낮았던 구단은?", "team_standings_daily"),
    ("2025년 KBO 투수 평균자책점 1위 투수는?", "stat_rankings"),
    ("지난 시즌 홈런왕이 누구야?", "stat_rankings"),
    ("이번 주에 진행된 경기 결과 알려줘", "game"),
    ("최근 화제가 된 선수 기록 동향은 뭐였지?", "player_movements"),
    ("이번 시즌 KBO 최고 타자는?", "stat_rankings"),
    ("KBO 공식 시상 내역 (MVP 등) 궁금해", "awards"),
    ("KIA 타이거즈의 2025 시즌 성적은?", "team_standings_daily"),
    ("화제 선수의 시즌 성적 궁금해", "player_season_pitching"),
    ("LG 트윈스의 고지가 어디야?", "teams"),
    ("삼성 라이온즈가 우승한 최근 시즌은?", "team_history"),
    ("2026년 두산 베어스의 경기 일정 확인해줘", "game_lineups"),
    ("KBO 금년 시즌 타격 1위 선수?", "stat_rankings"),
    ("최근 경기에서 가장 극적인 순간은?", "game_highlights"),
]

MODELS = {
    "perplexity/pplx-embed-v1-4b": None,
    "nvidia/nemotron-3-embed-1b:free": "END",
}


def embed(model: str, truncate: str | None, texts: list[str]) -> list[list[float]]:
    out: list[list[float]] = []
    n_batches = (len(texts) + 99) // 100
    for bi, i in enumerate(range(0, len(texts), 100), start=1):
        chunk = texts[i : i + 100]
        payload: dict = {"model": model, "input": chunk}
        if truncate:
            payload["truncate"] = truncate
        r = httpx.post(
            "https://openrouter.ai/api/v1/embeddings",
            headers={"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"},
            json=payload,
            timeout=180,
        )
        if r.status_code != 200:
            raise RuntimeError(f"{model} batch {bi}/{n_batches}: {r.status_code} {r.text[:200]}")
        recs = sorted(r.json()["data"], key=lambda x: x.get("index", 0))
        out.extend([x["embedding"] for x in recs])
        print(f"  batch {bi}/{n_batches} ok", flush=True)
    return out


def evaluate(qvecs, dvecs, choices, dim):
    q = np.asarray([v[:dim] + [0.0] * max(0, dim - len(v)) for v in qvecs], dtype=np.float32)
    d = np.asarray([v[:dim] + [0.0] * max(0, dim - len(v)) for v in dvecs], dtype=np.float32)
    q /= np.linalg.norm(q, axis=1, keepdims=True) + 1e-12
    d /= np.linalg.norm(d, axis=1, keepdims=True) + 1e-12
    sims = q @ d.T  # (n_queries, n_docs)
    hits = 0
    mrr = 0.0
    top1 = 0
    for qi, (_qt, expected) in enumerate(QUERIES):
        ranked = np.argsort(-sims[qi])[:TOP_K]
        ranked_sources = [choices[i] for i in ranked]
        try:
            rank = ranked_sources.index(expected) + 1
        except ValueError:
            rank = 0
        if rank:
            hits += 1
            mrr += 1.0 / rank
        if rank == 1:
            top1 += 1
    return hits / len(QUERIES), mrr / len(QUERIES), top1


def main() -> None:
    random.seed(SEED)
    with get_vector_session() as s:
        rows = s.execute(
            text("SELECT content, source_table FROM rag_chunks WHERE embedding IS NOT NULL ORDER BY random() LIMIT :n"),
            {"n": SAMPLE},
        ).all()
    doc_texts = [r[0] for r in rows]
    choices = [r[1] for r in rows]
    print(f"sampled {len(doc_texts)} chunks", flush=True)
    q_texts = [q[0] for q in QUERIES]

    report: dict = {"sample": SAMPLE, "models": {}}
    for model, truncate in MODELS.items():
        print(f"\n=== {model} (truncate={truncate}) ===", flush=True)
        t0 = time.time()
        try:
            qvecs = embed(model, truncate, q_texts)
            dvecs = embed(model, truncate, doc_texts)
        except BaseException as e:  # noqa: BLE001
            traceback.print_exc()
            report["models"][model] = {"error": str(e)}
            continue
        native = len(qvecs[0])
        print(f"embedded in {time.time() - t0:.1f}s, native_dim={native}", flush=True)
        cells = []
        dims = [d for d in (256, 512, 768, 1024, 1536, 2048) if d <= native]
        if native not in dims:
            dims.append(native)
        for dim in dims:
            r5, mrr, top1 = evaluate(qvecs, dvecs, choices, dim)
            cells.append({"dim": dim, "recall@5": round(r5, 4), "mrr": round(mrr, 4), "top1": top1})
            print(f"  dim={dim:<5} recall@5={r5:.3f} mrr={mrr:.3f} top1={top1}/20", flush=True)
        report["models"][model] = {"native_dim": native, "cells": cells}

    with open("bench_common_sample.json", "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print("\nsaved bench_common_sample.json", flush=True)


if __name__ == "__main__":
    try:
        main()
    except BaseException:  # noqa: BLE001
        traceback.print_exc()
        sys.exit(1)
