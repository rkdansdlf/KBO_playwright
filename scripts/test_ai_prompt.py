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
    
    # New: Add Movements & Roster Changes
    away_mv = data.get('away_movements', [])
    home_mv = data.get('home_movements', [])
    away_rc = data.get('away_roster_changes', {"added": [], "removed": []})
    home_rc = data.get('home_roster_changes', {"added": [], "removed": []})
    
    def format_mv(mv_list):
        if not mv_list: return "없음"
        return ", ".join([f"{m['player']}({m['section']})" for m in mv_list[:3]])

    def format_rc(rc_dict):
        added = ", ".join(rc_dict.get('added', [])) or "없음"
        removed = ", ".join(rc_dict.get('removed', [])) or "없음"
        return f"등록: {added} / 말소: {removed}"

    prompt = f"""
### [경기 프리뷰 분석 데이터]
1. 대진: {data.get('away_team_name')} vs {data.get('home_team_name')}
2. 상대 전적: {h2h.get('summary_text', '정보 없음')}
3. 팀별 최근 흐름 (최근 10경기):
   - {data.get('away_team_name')}: {away_l10.get('l10_text')} (현재 {away_l10.get('streak')})
   - {data.get('home_team_name')}: {home_l10.get('l10_text')} (현재 {home_l10.get('streak')})
4. 팀별 주요 지표 (최근 10경기 평균):
   - {data.get('away_team_name')}: 타율 {away_m.get('avg')}, ERA {away_m.get('era')}, 불펜ERA {away_m.get('bullpen_era')}
   - {data.get('home_team_name')}: 타율 {home_m.get('avg')}, ERA {home_m.get('era')}, 불펜ERA {home_m.get('bullpen_era')}
5. 주요 전력 변동 (최근 7일):
   - {data.get('away_team_name')} 부상/이동: {format_mv(away_mv)}
   - {data.get('home_team_name')} 부상/이동: {format_mv(home_mv)}
   - {data.get('away_team_name')} 엔트리 변동: {format_rc(away_rc)}
   - {data.get('home_team_name')} 엔트리 변동: {format_rc(home_rc)}

위 데이터를 바탕으로 오늘의 관전 포인트 3가지를 분석해줘. 
특히 최근 부상자 명단에 오른 선수나 엔트리에서 말소된 주요 선수가 있을 경우, 해당 선수의 공백이 팀 전력 및 경기 양상에 미칠 영향을 해설위원의 시각에서 심도 있게 분석에 포함시켜줘.
"""
    return prompt

def format_review_prompt(data):
    if not data: return "No review data available."
    
    moments = data.get('crucial_moments', [])
    if not moments: return "No crucial moments found for this game."
    
    # New: Add Movements & Roster Changes to Review context
    away_mv = data.get('away_movements', [])
    home_mv = data.get('home_movements', [])
    
    def format_mv(mv_list):
        if not mv_list: return "없음"
        return ", ".join([f"{m['player']}({m['section']})" for m in mv_list[:3]])

    moments_text = ""
    for m in moments:
        moments_text += f"- [{m['inning']}] {m['description']} (WPA: {m['wpa']})\n"
        
    prompt = f"""
### [경기 종료 후 주요 승부처 분석]
* 경기 결과: {data.get('final_score', '정보 없음')}
* 핵심 승부처 (WPA 기여도 순):
{moments_text}
* 경기 전 주요 전력 공백 정보:
  - {data.get('away_team_name')} 부상/이동: {format_mv(away_mv)}
  - {data.get('home_team_name')} 부상/이동: {format_mv(home_mv)}

위 승부처 리스트와 경기 전 전력 상황을 바탕으로 분석 기사를 작성해줘. 
특히 주요 부상 선수의 공백이 경기 결과에 어떤 영향을 미쳤는지(예: 대체 선수의 활약 여부 등)를 분석 포인트로 삼아줘.
WPA 수치가 높은 결정적인 장면들을 중심으로 야구 전문 기자의 시각에서 서술해줘.
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
