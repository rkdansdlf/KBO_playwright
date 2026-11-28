"""검색 결과를 근거로 자연어 답변을 만드는 계층.

`rag_ask`는 검색 결과에 검색어를 굵게 표시해 나열할 뿐 LLM을 부르지 않았다.
그래서 "데이터가 있어도 답변이 아니라 목록이 나오는" 상태였고, 검색이 실패하면
"'X'에 관한 검색 결과를 찾지 못했습니다"만 돌려주었다.

이 계층의 계약은 두 가지다:

* **근거에 있는 것만 답한다** — 2001~2007 경기별 박스스코어처럼 원천이 없는
  구간이 있으므로, 그럴듯한 답을 지어내면 안 된다. 근거가 없으면 없다고 답하는
  것이 정답이다.
* **LLM이 실패해도 침묵하지 않는다** — 검색 결과 자체는 유효하므로, 모델 호출이
  실패하면 그 결과를 그대로 보여 주고 왜 요약이 없는지 밝힌다.
"""

from __future__ import annotations

import json
from dataclasses import FrozenInstanceError, dataclass

import httpx
import pytest

from src.services.rag_answer_service import RagAnswer, RagAnswerService


@dataclass
class _Chunk:
    content: str
    title: str = "제목"
    source_url: str | None = None
    category: str | None = None


class _FakeResponse:
    def __init__(self, payload: dict, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code
        self.text = json.dumps(payload)

    def json(self) -> dict:
        return self._payload


class _FakeClient:
    """Record what was sent, so the prompt can be inspected."""

    def __init__(self, response: _FakeResponse | None = None, raises: Exception | None = None) -> None:
        self.response = response
        self.raises = raises
        self.requests: list[dict] = []

    def __enter__(self) -> _FakeClient:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def post(self, url: str, json: dict | None = None, headers: dict | None = None) -> _FakeResponse:
        self.requests.append({"url": url, "json": json or {}, "headers": headers or {}})
        if self.raises is not None:
            raise self.raises
        assert self.response is not None
        return self.response


def _completion(text: str) -> _FakeResponse:
    return _FakeResponse({"choices": [{"message": {"content": text}}]})


def _service(client: _FakeClient) -> RagAnswerService:
    return RagAnswerService(api_key="test-key", model="test-model", client_factory=lambda: client)


class TestWhenThereIsNothingToGroundOn:
    def test_no_chunks_means_no_llm_call(self) -> None:
        """A model asked without evidence can only guess, so it is not asked."""
        client = _FakeClient(_completion("무언가"))
        result = RagAnswerService(api_key="k", client_factory=lambda: client).answer("2005년 롯데 우승?", [])

        assert client.requests == []
        assert result.llm_used is False
        assert "찾지 못했" in result.answer or "확인할 수 없" in result.answer
        assert result.sources == []

    def test_a_missing_api_key_skips_the_call_but_keeps_the_sources(self) -> None:
        """The chunks are still the answer's evidence even when nobody can summarise them."""
        unused = _FakeClient()
        result = RagAnswerService(api_key="", client_factory=lambda: unused).answer(
            "2004년 MVP",
            [_Chunk(content="2004 MVP 배영수", title="2004 MVP 배영수")],
        )

        assert result.llm_used is False
        assert result.fallback_reason == "no_api_key"
        assert len(result.sources) == 1
        assert "배영수" in result.answer


class TestThePromptCarriesTheRules:
    def test_the_evidence_is_in_the_prompt(self) -> None:
        client = _FakeClient(_completion("배영수입니다."))
        _service(client).answer("2004년 MVP", [_Chunk(content="2004 MVP 배영수 (팀: SS)")])

        sent = client.requests[0]["json"]
        prompt = json.dumps(sent, ensure_ascii=False)
        assert "2004 MVP 배영수" in prompt

    def test_the_prompt_forbids_answering_beyond_the_evidence(self) -> None:
        """The rule that makes the missing 2001~2007 box scores safe to ask about."""
        client = _FakeClient(_completion("답"))
        _service(client).answer("2005년 5월 3일 경기 점수", [_Chunk(content="2005-05-03 KBO 경기: LT ? - SS ?")])

        prompt = json.dumps(client.requests[0]["json"], ensure_ascii=False)
        assert "근거" in prompt
        assert "추측" in prompt or "지어내" in prompt

    def test_only_the_requested_number_of_chunks_is_sent(self) -> None:
        client = _FakeClient(_completion("답"))
        chunks = [_Chunk(content=f"청크{i}") for i in range(20)]

        _service(client).answer("질문", chunks, max_chunks=3)

        prompt = json.dumps(client.requests[0]["json"], ensure_ascii=False)
        assert "청크0" in prompt and "청크2" in prompt
        assert "청크3" not in prompt


class TestTheAnswerAndItsSources:
    def test_the_model_answer_is_returned_with_its_sources(self) -> None:
        client = _FakeClient(_completion("2004년 MVP는 배영수입니다."))
        result = _service(client).answer(
            "2004년 MVP",
            [_Chunk(content="2004 MVP 배영수", title="2004 MVP 배영수", source_url="https://x/1")],
        )

        assert result.llm_used is True
        assert result.answer == "2004년 MVP는 배영수입니다."
        assert result.sources[0]["title"] == "2004 MVP 배영수"
        assert result.sources[0]["source_url"] == "https://x/1"

    def test_an_empty_model_answer_is_not_treated_as_an_answer(self) -> None:
        """Falling back beats returning an empty string as if it were one."""
        client = _FakeClient(_completion("   "))
        result = _service(client).answer("질문", [_Chunk(content="근거 내용")])

        assert result.llm_used is False
        assert result.fallback_reason == "empty_completion"
        assert "근거 내용" in result.answer


class TestWhenTheModelIsUnavailable:
    def test_a_transport_error_falls_back_to_the_chunks(self) -> None:
        client = _FakeClient(raises=httpx.ConnectError("no route"))
        result = _service(client).answer("질문", [_Chunk(content="근거 내용", title="제목")])

        assert result.llm_used is False
        assert result.fallback_reason is not None
        assert "근거 내용" in result.answer

    def test_a_non_200_response_falls_back(self) -> None:
        client = _FakeClient(_FakeResponse({"error": "rate limited"}, status_code=429))
        result = _service(client).answer("질문", [_Chunk(content="근거 내용")])

        assert result.llm_used is False
        assert result.fallback_reason is not None

    def test_a_malformed_envelope_falls_back(self) -> None:
        client = _FakeClient(_FakeResponse({"unexpected": True}))
        result = _service(client).answer("질문", [_Chunk(content="근거 내용")])

        assert result.llm_used is False
        assert result.fallback_reason is not None

    def test_the_fallback_still_lists_the_sources(self) -> None:
        client = _FakeClient(raises=httpx.ConnectError("no route"))
        result = _service(client).answer("질문", [_Chunk(content="A"), _Chunk(content="B")])

        assert len(result.sources) == 2


class TestTheResultShape:
    def test_it_is_a_frozen_dataclass_with_the_expected_fields(self) -> None:
        from dataclasses import fields

        names = {field.name for field in fields(RagAnswer)}
        assert names == {"answer", "sources", "llm_used", "fallback_reason"}

        with pytest.raises(FrozenInstanceError):
            RagAnswer(answer="x", sources=[], llm_used=False, fallback_reason=None).answer = "y"  # type: ignore[misc]
