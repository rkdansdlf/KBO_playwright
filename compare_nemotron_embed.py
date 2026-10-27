#!/usr/bin/env python3
"""
Nemotron 3.5 Embed 비교 테스트
- nvidia/nemotron-3.5-embed-8b:free (8B params, 고품질 임베딩)
- nvidia/nemotron-3.5-embed-1b:free (1B params, 경량 임베딩)

OpenRouter API의 /embeddings 엔드포인트 사용
"""

import time
import json
from typing import List, Dict, Any

# 모델 설정
MODELS = {
    "8b": "nvidia/nemotron-3.5-embed-8b:free",
    "1b": "nvidia/nemotron-3.5-embed-1b:free"
}

TEST_TEXTS = [
    "야구 타자 홈런 기록 분석",  # 한국어 스포츠
    "Pitcher ERA calculation and statistics",  # 영어 통계
    "KBO 리그 2026 시즌 순위 예측",
    "Baseball defensive metrics and WAR",
    "야구 경기 데이터 품질 관리 체계"
]

def benchmark_embedding(model_name: str, texts: List[str], api_key: str) -> Dict[str, Any]:
    """OpenRouter embeddings endpoint 호출"""
    import requests

    url = "https://openrouter.ai/api/v1/embeddings"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    start = time.time()
    embeddings_result = []

    try:
        for text in texts:
            payload = {
                "model": model_name,
                "input": text,
                "encoding_format": "float"
            }
            r = requests.post(url, headers=headers, json=payload, timeout=30)
            result = r.json()

            emb = result.get("data", [{}])[0].get("embedding", [])
            embeddings_result.append({
                "text": text,
                "dim": len(emb),
                "embedding_sample": emb[:5] if emb else None,
                "status": "ok" if emb else "empty",
                "latency_sec": time.time() - start
            })
    except Exception as e:
        embeddings_result.append({"error": str(e)})

    total_time = time.time() - start
    dims = [e["dim"] for e in embeddings_result if "dim" in e]

    return {
        "model": model_name,
        "texts_tested": len(texts),
        "success": len([e for e in embeddings_result if e.get("status") == "ok"]),
        "total_time_sec": total_time,
        "avg_latency_per_text_sec": total_time / len(texts) if texts else 0,
        "embedding_dims": dims[0] if dims else 0,
        "results": embeddings_result
    }

def compare_results(r1: Dict, r2: Dict) -> Dict:
    """두 모델 결과 비교"""
    return {
        "speed_ratio_1b_to_8b": r1["avg_latency_per_text_sec"] / r2["avg_latency_per_text_sec"] if r2["avg_latency_per_text_sec"] > 0 else None,
        "dim_8b": r1.get("embedding_dims"),
        "dim_1b": r2.get("embedding_dims"),
        "time_8b_sec": r1["total_time_sec"],
        "time_1b_sec": r2["total_time_sec"],
        "success_8b": r1["success"],
        "success_1b": r2["success"]
    }

def run_benchmarks(api_key: str) -> Dict[str, Any]:
    """두 모델 모두 벤치마크 실행"""
    results = {}
    
    for key, model_name in MODELS.items():
        print(f"\n{'='*60}")
        print(f"벤치마크 실행: {model_name}")
        print(f"{'='*60}")
        
        result = benchmark_embedding(model_name, TEST_TEXTS, api_key)
        results[key] = result
        
        print(f"\n결과 요약:")
        print(f"  성공: {result['success']}/{result['texts_tested']}")
        print(f"  임베딩 차원: {result['embedding_dims']}")
        print(f"  총 소요 시간: {result['total_time_sec']:.2f}초")
        print(f"  텍스트당 평균: {result['avg_latency_per_text_sec']:.3f}초")
    
    return results

def print_comparison(results: Dict[str, Any]):
    """비교 결과 출력"""
    r8b = results.get("8b", {})
    r1b = results.get("1b", {})
    
    if not r8b or not r1b:
        print("비교할 데이터가 부족합니다.")
        return
    
    comparison = compare_results(r8b, r1b)
    
    print(f"\n{'='*60}")
    print("모델 비교 결과")
    print(f"{'='*60}")
    print(f"\n{'지표':<30} {'8B 모델':<15} {'1B 모델':<15} {'비율/차이'}")
    print("-" * 75)
    print(f"{'임베딩 차원':<30} {comparison['dim_8b']:<15} {comparison['dim_1b']:<15} {comparison['dim_8b'] - comparison['dim_1b']:+d}")
    print(f"{'총 소요 시간 (초)':<30} {comparison['time_8b_sec']:<15.2f} {comparison['time_1b_sec']:<15.2f} {comparison['time_8b_sec'] - comparison['time_1b_sec']:+.2f}")
    print(f"{'성공 건수':<30} {comparison['success_8b']:<15} {comparison['success_1b']:<15} {comparison['success_8b'] - comparison['success_1b']:+d}")
    
    if comparison['speed_ratio_1b_to_8b']:
        print(f"\n속도 비교: 1B 모델이 8B 모델 대비 {comparison['speed_ratio_1b_to_8b']:.2f}배 {'빠름' if comparison['speed_ratio_1b_to_8b'] < 1 else '느림'}")
    
    # 상세 결과 출력
    print(f"\n{'='*60}")
    print("상세 결과")
    print(f"{'='*60}")
    for key, model_name in MODELS.items():
        r = results[key]
        print(f"\n[{model_name}]")
        for i, res in enumerate(r['results']):
            if 'error' in res:
                print(f"  {i+1}. {res['text'][:40]}... -> 에러: {res['error']}")
            else:
                print(f"  {i+1}. {res['text'][:40]}... -> {res['dim']}D, {res['latency_sec']:.3f}초")

def save_results(results: Dict[str, Any], filename: str = "nemotron_benchmark_results.json"):
    """결과를 JSON 파일로 저장"""
    output = {
        "models": MODELS,
        "test_texts": TEST_TEXTS,
        "results": results,
        "comparison": compare_results(results.get("8b", {}), results.get("1b", {}))
    }
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print(f"\n결과 저장됨: {filename}")

if __name__ == "__main__":
    import os
    
    print("=" * 60)
    print("Nemotron 3.5 Embed 성능 비교 테스트")
    print("=" * 60)
    print()
    print("설정된 모델:")
    for k, v in MODELS.items():
        print(f"  {k}: {v}")
    print()
    print("테스트 텍스트 (한국어/영어 혼합):")
    for i, t in enumerate(TEST_TEXTS, 1):
        print(f"  {i}. {t}")
    print()
    
    # API 키 확인
    api_key = os.getenv('OPENROUTER_API_KEY')
    if not api_key:
        print("[경고] OPENROUTER_API_KEY 환경변수가 설정되지 않았습니다.")
        print()
        print("실행 방법:")
        print("  1. OpenRouter API 키 발급: https://openrouter.ai/keys")
        print("  2. 환경변수 설정:")
        print("     Windows (PowerShell): $env:OPENROUTER_API_KEY='your_key'")
        print("     Windows (CMD): set OPENROUTER_API_KEY=your_key")
        print("     Linux/Mac: export OPENROUTER_API_KEY=your_key")
        print("  3. 실행: python compare_nemotron_embed.py")
        print()
        print("비교 항목:")
        print("  - 임베딩 차원 (dim)")
        print("  - 응답 시간 (latency)")
        print("  - 성공율")
        print("  - 품질: 응답 길이/밀도 (수동 평가 필요)")
        exit(1)
    
    print("API 키 확인됨. 벤치마크 시작...")
    
    # 벤치마크 실행
    results = run_benchmarks(api_key)
    
    # 비교 출력
    print_comparison(results)
    
    # 결과 저장
    save_results(results)
    
    print("\n[완료] 벤치마크 완료!")
