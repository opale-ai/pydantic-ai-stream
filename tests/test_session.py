import pytest
from dataclasses import dataclass

from pydantic_ai.messages import (
    ModelRequest,
    ModelResponse,
    TextPart,
    UserPromptPart,
)

from opale.agx import Session


@dataclass
class MemorySession(Session):
    async def load(self) -> None:
        pass

    async def save(self) -> None:
        pass


class TestMsgsToFromJson:
    def test_empty_roundtrip(self):
        session = MemorySession()
        data = session.msgs_to_json()
        session.msgs_from_json(data)
        assert session.msgs == []

    def test_messages_roundtrip(self):
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


class TestGetUserPrompt:
    def test_empty_msgs(self):
        session = MemorySession()
        assert session.get_user_prompt() == "No title"

    def test_with_user_prompt(self):
        session = MemorySession()
        request = ModelRequest(parts=[UserPromptPart(content="What is 2+2?")])
        session.add_msgs([request])
        assert session.get_user_prompt() == "What is 2+2?"


class TestNodesFromMsgs:
    def test_empty_msgs(self):
        assert Session.nodes_from_msgs([]) == []

    def test_odd_msgs_returns_empty(self):
        assert Session.nodes_from_msgs([{"kind": "request", "parts": []}]) == []

    def test_valid_request_response_pair(self):
        msgs = [
            {"kind": "request", "parts": [{"part_kind": "user-prompt", "content": "Hi"}]},
            {"kind": "response", "parts": [{"part_kind": "text", "content": "Hello"}]},
        ]
        nodes = Session.nodes_from_msgs(msgs)
        assert len(nodes) == 1
        assert nodes[0]["kind"] is None
        assert len(nodes[0]["parts"]) == 2

    def test_filters_system_prompt(self):
        msgs = [
            {"kind": "request", "parts": [{"part_kind": "system-prompt"}, {"part_kind": "user-prompt"}]},
            {"kind": "response", "parts": [{"part_kind": "text"}]},
        ]
        nodes = Session.nodes_from_msgs(msgs)
        assert len(nodes[0]["parts"]) == 2

