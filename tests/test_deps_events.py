"""Tests for Deps node event methods: add_node_begin, add_node_end, add_node_event."""

import json
from unittest.mock import MagicMock

import pytest

from pydantic_ai.messages import (
    FinalResultEvent,
    ModelRequest,
    PartDeltaEvent,
    PartStartEvent,
    TextPart,
    TextPartDelta,
    ThinkingPart,
    ThinkingPartDelta,
    ToolCallPart,
    ToolCallPartDelta,
    ToolReturnPart,
    UserPromptPart,
)

from pydantic_ai_stream.deps import Node, Runtime


class TestNodeRuntime:
    def test_node_defaults(self):
        node = Node(idx=0)
        assert node.idx == 0
        assert node.events == {}
        assert node.parts == {}
        assert node.stopped is False

    def test_runtime_defaults(self):
        runtime = Runtime()
        assert runtime.nodes == []

    def test_runtime_tracks_multiple_nodes(self):
        runtime = Runtime()
        runtime.nodes.append(Node(idx=0))
        runtime.nodes.append(Node(idx=1))
        assert len(runtime.nodes) == 2
        assert runtime.nodes[0].idx == 0
        assert runtime.nodes[1].idx == 1


class TestAddNodeBegin:
    @pytest.mark.asyncio
    async def test_emits_llm_begin_event(self, redis, make_deps):
        deps = make_deps()
        node = MagicMock()
        node.request = ModelRequest(parts=[UserPromptPart(content="Hello")])
        await deps.add_node_begin(node)
        entries = await redis.xrange(deps.key())
        assert len(entries) == 1
        _, fields = entries[0]
        body = json.loads(fields[b"body"])
        assert body["event"] == "llm-begin"
        assert body["idx"] == 0

    @pytest.mark.asyncio
    async def test_increments_node_index(self, redis, make_deps):
        deps = make_deps()
        node = MagicMock()
        node.request = ModelRequest(parts=[UserPromptPart(content="Hello")])
        await deps.add_node_begin(node)
        await deps.add_node_end()
        await deps.add_node_begin(node)
        entries = await redis.xrange(deps.key())
        bodies = [json.loads(e[1][b"body"]) for e in entries]
        assert bodies[0]["idx"] == 0
        assert bodies[2]["idx"] == 1

    @pytest.mark.asyncio
    async def test_emits_tool_return_parts(self, redis, make_deps):
        deps = make_deps()
        tool_return = ToolReturnPart(
            tool_name="get_weather",
            tool_call_id="call_123",
            content="Sunny, 72°F",
        )
        node = MagicMock()
        node.request = ModelRequest(parts=[tool_return])
        await deps.add_node_begin(node)
        entries = await redis.xrange(deps.key())
        assert len(entries) == 2
        _, fields = entries[1]
        body = json.loads(fields[b"body"])
        assert body["event"] == "part_start"
        assert body["tool_name"] == "get_weather"
        assert body["tool_call_id"] == "call_123"
        assert body["content"] == "Sunny, 72°F"


class TestAddNodeEnd:
    @pytest.mark.asyncio
    async def test_emits_llm_end_event(self, redis, make_deps):
        deps = make_deps()
        node = MagicMock()
        node.request = ModelRequest(parts=[UserPromptPart(content="Hello")])
        await deps.add_node_begin(node)
        await deps.add_node_end()
        entries = await redis.xrange(deps.key())
        _, fields = entries[-1]
        body = json.loads(fields[b"body"])
        assert body["event"] == "llm-end"
        assert body["idx"] == 0

    @pytest.mark.asyncio
    async def test_marks_node_as_stopped(self, make_deps):
        deps = make_deps()
        node = MagicMock()
        node.request = ModelRequest(parts=[UserPromptPart(content="Hello")])
        await deps.add_node_begin(node)
        assert deps.runtime.nodes[0].stopped is False
        await deps.add_node_end()
        assert deps.runtime.nodes[0].stopped is True


class TestAddNodeEventPartStart:
    @pytest.mark.asyncio
    async def test_text_part_start(self, redis, make_deps):
        deps = make_deps()
        node = MagicMock()
        node.request = ModelRequest(parts=[UserPromptPart(content="Hello")])
        await deps.add_node_begin(node)
        event = PartStartEvent(index=0, part=TextPart(content="Hello world"))
        await deps.add_node_event(event)
        entries = await redis.xrange(deps.key())
        _, fields = entries[-1]
        body = json.loads(fields[b"body"])
        assert body["event"] == "part_start"
        assert body["part_kind"] == "text"
        assert body["content"] == "Hello world"

    @pytest.mark.asyncio
    async def test_thinking_part_start(self, redis, make_deps):
        deps = make_deps()
        node = MagicMock()
        node.request = ModelRequest(parts=[UserPromptPart(content="Hello")])
        await deps.add_node_begin(node)
        event = PartStartEvent(index=0, part=ThinkingPart(content="Let me think..."))
        await deps.add_node_event(event)
        entries = await redis.xrange(deps.key())
        _, fields = entries[-1]
        body = json.loads(fields[b"body"])
        assert body["part_kind"] == "thinking"

    @pytest.mark.asyncio
    async def test_tool_call_part_stored_not_emitted(self, redis, make_deps):
        deps = make_deps()
        node = MagicMock()
        node.request = ModelRequest(parts=[UserPromptPart(content="Hello")])
        await deps.add_node_begin(node)
        tool_call = ToolCallPart(
            tool_name="get_weather",
            args='{"city": "Paris"}',
            tool_call_id="call_456",
        )
        event = PartStartEvent(index=0, part=tool_call)
        await deps.add_node_event(event)
        entries = await redis.xrange(deps.key())
        assert len(entries) == 1
        assert deps.runtime.nodes[0].parts[0]["tool_name"] == "get_weather"


class TestAddNodeEventPartDelta:
    @pytest.mark.asyncio
    async def test_text_delta(self, redis, make_deps):
        deps = make_deps()
        node = MagicMock()
        node.request = ModelRequest(parts=[UserPromptPart(content="Hello")])
        await deps.add_node_begin(node)
        start = PartStartEvent(index=0, part=TextPart(content=""))
        await deps.add_node_event(start)
        delta = PartDeltaEvent(index=0, delta=TextPartDelta(content_delta="chunk"))
        await deps.add_node_event(delta)
        entries = await redis.xrange(deps.key())
        _, fields = entries[-1]
        body = json.loads(fields[b"body"])
        assert body["event"] == "part_delta"
        assert body["content_delta"] == "chunk"

    @pytest.mark.asyncio
    async def test_thinking_delta(self, redis, make_deps):
        deps = make_deps()
        node = MagicMock()
        node.request = ModelRequest(parts=[UserPromptPart(content="Hello")])
        await deps.add_node_begin(node)
        start = PartStartEvent(index=0, part=ThinkingPart(content=""))
        await deps.add_node_event(start)
        delta = PartDeltaEvent(
            index=0, delta=ThinkingPartDelta(content_delta="reasoning")
        )
        await deps.add_node_event(delta)
        entries = await redis.xrange(deps.key())
        _, fields = entries[-1]
        body = json.loads(fields[b"body"])
        assert body["part_delta_kind"] == "thinking"

    @pytest.mark.asyncio
    async def test_tool_call_delta_accumulates_args(self, make_deps):
        deps = make_deps()
        node = MagicMock()
        node.request = ModelRequest(parts=[UserPromptPart(content="Hello")])
        await deps.add_node_begin(node)
        tool_call = ToolCallPart(
            tool_name="",
            args="",
            tool_call_id="call_789",
        )
        start = PartStartEvent(index=0, part=tool_call)
        await deps.add_node_event(start)
        delta1 = PartDeltaEvent(
            index=0,
            delta=ToolCallPartDelta(
                tool_call_id="call_789", tool_name_delta="get_", args_delta='{"'
            ),
        )
        await deps.add_node_event(delta1)
        delta2 = PartDeltaEvent(
            index=0,
            delta=ToolCallPartDelta(
                tool_call_id="call_789",
                tool_name_delta="weather",
                args_delta='city": "NYC"}',
            ),
        )
        await deps.add_node_event(delta2)
        stored = deps.runtime.nodes[0].parts[0]
        assert stored["tool_name"] == "get_weather"
        assert stored["args"] == '{}{"city": "NYC"}'


class TestAddNodeEventFinalResult:
    @pytest.mark.asyncio
    async def test_final_result_emits_answer(self, redis, make_deps):
        deps = make_deps()
        node = MagicMock()
        node.request = ModelRequest(parts=[UserPromptPart(content="Hello")])
        await deps.add_node_begin(node)
        event = FinalResultEvent(tool_name=None, tool_call_id=None)
        await deps.add_node_event(event)
        entries = await redis.xrange(deps.key())
        _, fields = entries[-1]
        body = json.loads(fields[b"body"])
        assert body["event"] == "answer"
