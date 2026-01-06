"""Tests for Session abstract class."""

from dataclasses import dataclass

import pytest
from pydantic_ai.messages import (
    ModelRequest,
    ModelResponse,
    TextPart,
    UserPromptPart,
)

from pydantic_ai_stream import Session


@dataclass
class MemorySession(Session):
    async def load(self) -> None:
        pass

    async def save(self) -> None:
        pass


class TestMessageSerialization:
    def test_empty_msgs_roundtrip(self):
        session = MemorySession()
        data = session.msgs_to_json()
        session.msgs_from_json(data)
        assert session.msgs == []

    def test_request_response_roundtrip(self):
        session = MemorySession()
        request = ModelRequest(parts=[UserPromptPart(content="Hello")])
        response = ModelResponse(parts=[TextPart(content="Hi there!")])
        session.add_msgs([request, response])
        data = session.msgs_to_json()
        session2 = MemorySession()
        session2.msgs_from_json(data)
        assert len(session2.msgs) == 2
        assert isinstance(session2.msgs[0], ModelRequest)
        assert isinstance(session2.msgs[1], ModelResponse)

    def test_msgs_to_json_returns_bytes(self):
        session = MemorySession()
        data = session.msgs_to_json()
        assert isinstance(data, bytes)

    def test_add_msgs_extends_list(self):
        session = MemorySession()
        msg1 = ModelRequest(parts=[UserPromptPart(content="First")])
        msg2 = ModelRequest(parts=[UserPromptPart(content="Second")])
        session.add_msgs([msg1])
        session.add_msgs([msg2])
        assert len(session.msgs) == 2


class TestGetUserPrompt:
    def test_empty_msgs_returns_no_title(self):
        session = MemorySession()
        assert session.get_user_prompt() == "No title"

    def test_extracts_user_prompt_content(self):
        session = MemorySession()
        request = ModelRequest(parts=[UserPromptPart(content="What is 2+2?")])
        session.add_msgs([request])
        assert session.get_user_prompt() == "What is 2+2?"

    def test_no_user_prompt_part_returns_no_title(self):
        session = MemorySession()
        response = ModelResponse(parts=[TextPart(content="Answer")])
        session.add_msgs([response])
        assert session.get_user_prompt() == "No title"


class TestNodesFromMsgs:
    def test_empty_msgs(self):
        assert Session.nodes_from_msgs([]) == []

    def test_odd_count_returns_empty(self):
        assert Session.nodes_from_msgs([{"kind": "request", "parts": []}]) == []

    def test_valid_request_response_pair(self):
        msgs = [
            {
                "kind": "request",
                "parts": [{"part_kind": "user-prompt", "content": "Hi"}],
            },
            {"kind": "response", "parts": [{"part_kind": "text", "content": "Hello"}]},
        ]
        nodes = Session.nodes_from_msgs(msgs)
        assert len(nodes) == 1
        assert nodes[0]["kind"] is None
        assert len(nodes[0]["parts"]) == 2

    def test_filters_system_prompt_parts(self):
        msgs = [
            {
                "kind": "request",
                "parts": [{"part_kind": "system-prompt"}, {"part_kind": "user-prompt"}],
            },
            {"kind": "response", "parts": [{"part_kind": "text"}]},
        ]
        nodes = Session.nodes_from_msgs(msgs)
        part_kinds = [p["part_kind"] for p in nodes[0]["parts"]]
        assert "system-prompt" not in part_kinds
        assert len(nodes[0]["parts"]) == 2

    def test_skips_invalid_kind(self):
        msgs = [
            {"kind": "invalid", "parts": []},
            {"kind": "response", "parts": []},
        ]
        nodes = Session.nodes_from_msgs(msgs)
        assert len(nodes) == 0

    def test_multiple_pairs(self):
        msgs = [
            {"kind": "request", "parts": [{"part_kind": "user-prompt"}]},
            {"kind": "response", "parts": [{"part_kind": "text"}]},
            {"kind": "request", "parts": [{"part_kind": "user-prompt"}]},
            {"kind": "response", "parts": [{"part_kind": "text"}]},
        ]
        nodes = Session.nodes_from_msgs(msgs)
        assert len(nodes) == 2

    def test_sets_signature_to_none(self):
        msgs = [
            {
                "kind": "request",
                "parts": [{"part_kind": "user-prompt", "signature": "abc"}],
            },
            {"kind": "response", "parts": []},
        ]
        nodes = Session.nodes_from_msgs(msgs)
        assert nodes[0]["parts"][0]["signature"] is None


class TestSessionABC:
    @pytest.mark.asyncio
    async def test_load_must_be_implemented(self):
        class IncompleteSession(Session):
            async def save(self) -> None:
                pass

        with pytest.raises(TypeError, match="load"):
            IncompleteSession()

    @pytest.mark.asyncio
    async def test_save_must_be_implemented(self):
        class IncompleteSession(Session):
            async def load(self) -> None:
                pass

        with pytest.raises(TypeError, match="save"):
            IncompleteSession()
