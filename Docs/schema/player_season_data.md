# KBO 타자 기록 크롤링 스키마 (실제 구조 기준)

## 정규시즌 (series_value="0")
URL: https://www.koreabaseball.com/Record/Player/HitterBasic/Basic1.aspx
컬럼: AVG,G,PA,AB,R,H,2B,3B,HR,TB,RBI,SAC,SF

https://www.koreabaseball.com/Record/Player/HitterBasic/Basic2.aspx
BB,IBB,HBP,SO,GDP,SLG,OBP,OPS,MH,RISP,PH-BA

## 기타 시리즈 (시범경기, 플레이오프 등)
<option value="1">KBO 시범경기</option>
<option value="4">KBO 와일드카드</option>
<option value="3">KBO 준플레이오프</option>
<option value="5">KBO 플레이오프</option>
<option value="7">KBO 한국시리즈</option>

컬럼: AVG,G,PA,AB,H,2B,3B,HR,RBI,SB,CS,BB,HBP,SO,GDP,E

참고: 기타 시리즈에서는 R(득점), TB(루타), SAC(희생번트), SF(희생플라이) 대신
SB(도루), CS(도루실패), BB(볼넷), HBP(사구), SO(삼진), GDP(병살타), E(에러)가 표시됨


# KBO 투수 기록 크롤링 스키마 (실제 구조 기준)
투수 -> 정규시즌 (선택) -> 연도 (선택) -> <a onmouseenter="tooltip(this)" onmouseout="hideTip()" href="javascript:sort('INN2_CN');" title="이닝">IP</a> 클릭 후 리스트 크롤링

<a href="/Record/Player/PitcherBasic/Basic2.aspx" class="next">다음</a> 클릭 -> <a onmouseenter="tooltip(this)" onmouseout="hideTip()" href="javascript:sort('PIT_CN');" title="투구수">NP</a> 클릭 후 리스트 크롤링

https://www.koreabaseball.com/Record/Player/PitcherBasic/Basic1.aspx
순위,선수명,팀명,ERA,G,W,L,SV,HLD,WPCT,IP,H,HR,BB,HBP,SO,R,ER,WHIP

https://www.koreabaseball.com/Record/Player/PitcherBasic/Basic2.aspx
순위,선수명,팀명,ERA,CG,SHO,QS,BSV,TBF,NP,AVG,2B,3B,SAC,SF,IBB,WP,BK


# 기타 시리즈 (시범경기, 플레이오프)
컬럼 순위,선수명,팀명,ERA,G,CG,SHO,W,L,SV,HLD,WPCT,TBF,IP,H,HR,BB,HBP,SO,R, ER