"""
AI Prompt Engineering & Quality Test Script
Converts raw JSON context into human-readable prompts for LLM.
"""
import json
import os
import sys
from sqlalchemy import create_engine, text

def format_preview_prompt(data):
    if not data: return "No preview data available."
    
    h2h = data.get('matchup_h2h', {})
    away_l10 = data.get('away_recent_l10', {})
    home_l10 = data.get('home_recent_l10', {})
    away_m = data.get('away_metrics', {})
    home_m = data.get('home_metrics', {})
    
    prompt = f"""
### [경기 프리뷰 분석 데이터]
1. 대진: {data.get('away_team_name')} vs {data.get('home_team_name')}
2. 상대 전적: {h2h.get('summary_text', '정보 없음')}
3. 팀별 최근 흐름 (최근 10경기):
   - {data.get('away_team_name')}: {away_l10.get('l10_text')} (현재 {away_l10.get('streak')})
   - {data.get('home_team_name')}: {home_l10.get('l10_text')} (현재 {home_l10.get('streak')})
4. 팀별 주요 지표 (최근 10경기 평균):
   - {data.get('away_team_name')}: 타율 {away_m.get('avg')}, 평균자책점 {away_m.get('era')}, 불펜ERA {away_m.get('bullpen_era')}
   - {data.get('home_team_name')}: 타율 {home_m.get('avg')}, 평균자책점 {home_m.get('era')}, 불펜ERA {home_m.get('bullpen_era')}

위 데이터를 바탕으로 오늘의 관전 포인트 3가지를 전문적인 야구 해설가 톤으로 분석해줘.
"""
    return prompt

def format_review_prompt(data):
    if not data: return "No review data available."
    
    moments = data.get('crucial_moments', [])
    if not moments: return "No crucial moments found for this game."
    
    moments_text = ""
    for m in moments:
        moments_text += f"- [{m['inning']}] {m['description']} (WPA: {m['wpa']})\n"
        
    prompt = f"""
### [경기 종료 후 주요 승부처 분석]
* 경기 결과: {data.get('final_score', '정보 없음')}
* 핵심 승부처 (WPA 기여도 순):
{moments_text}

위 승부처 리스트를 바탕으로 오늘 경기의 승리 요인과 패배 요인을 상세히 분석하는 기사를 작성해줘.
특히 WPA 수치가 높은 결정적인 장면들을 강조해서 설명해줘.
"""
    return prompt

def main():
    # Use OCI if available, else local
    url = os.getenv("OCI_DB_URL")
    if not url:
        print("OCI_DB_URL not set. Please set it to test with real data.")
        return

    engine = create_engine(url)
    
    # Test with a specific high-quality 2026 game (e.g., 20260412SKLG0)
    game_id = '20260412SKLG0'
    
    query = f"""
    SELECT preview_data, review_wpa_data 
    FROM v_ai_game_context 
    WHERE game_id = '{game_id}'
    """
    
    with engine.connect() as conn:
        result = conn.execute(text(query)).fetchone()
        if not result:
            print(f"Game {game_id} not found in OCI.")
            return
            
        preview_json = result[0]
        review_json = result[1]
        
        print(f"\n{'='*20} AI PROMPT TEST: {game_id} {'='*20}")
        
        print("\n--- PREVIEW PROMPT ---")
        print(format_preview_prompt(preview_json))
        
        print("\n--- REVIEW PROMPT ---")
        print(format_review_prompt(review_json))

if __name__ == "__main__":
    main()
