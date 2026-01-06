"""Tests for Deps stream operations: start/stop/add/listen/cancel/is_live."""

import asyncio
import json

import pytest

from pydantic_ai_stream import q


class TestStreamLifecycle:
    @pytest.mark.asyncio
    async def test_start_sets_live_flag(self, make_deps):
        deps = make_deps()
        await deps.start()
        assert await deps.is_live() is True

    @pytest.mark.asyncio
    async def test_start_emits_begin_event(self, redis, make_deps):
        deps = make_deps()
        await deps.start()
        entries = await redis.xrange(deps.key())
        assert len(entries) == 1
        _, fields = entries[0]
        assert fields[b"type"] == b"begin"
        assert fields[b"origin"] == b"pydantic-ai-stream"
        body = json.loads(fields[b"body"])
        assert body["session_id"] == deps.session_id

    @pytest.mark.asyncio
    async def test_stop_clears_live_flag(self, make_deps):
        deps = make_deps()
        await deps.start()
        assert await deps.is_live() is True
        await deps.stop()
        assert await deps.is_live() is False

    @pytest.mark.asyncio
    async def test_stop_emits_end_event(self, redis, make_deps):
        deps = make_deps()
        await deps.start()
        await deps.stop()
        entries = await redis.xrange(deps.key())
        assert len(entries) == 2
        _, fields = entries[1]
        assert fields[b"type"] == b"end"
        assert fields[b"origin"] == b"pydantic-ai-stream"

    @pytest.mark.asyncio
    async def test_stop_sets_ttl(self, redis, make_deps):
        deps = make_deps()
        await deps.start()
        await deps.stop(grace_period=10)
        ttl = await redis.ttl(deps.key())
        assert 0 < ttl <= 10

    @pytest.mark.asyncio
    async def test_is_live_false_when_not_started(self, make_deps):
        deps = make_deps()
        assert await deps.is_live() is False


class TestAdd:
    @pytest.mark.asyncio
    async def test_add_event_with_body(self, redis, make_deps):
        deps = make_deps()
        await deps.add(
            type="event", origin="pydantic-ai", body={"idx": 0, "event": "llm-begin"}
        )
        entries = await redis.xrange(deps.key())
        assert len(entries) == 1
        _, fields = entries[0]
        assert fields[b"type"] == b"event"
        assert fields[b"origin"] == b"pydantic-ai"
        body = json.loads(fields[b"body"])
        assert body == {"idx": 0, "event": "llm-begin"}

    @pytest.mark.asyncio
    async def test_add_event_without_body(self, redis, make_deps):
        deps = make_deps()
        await deps.add(type="end", origin="pydantic-ai-stream")
        entries = await redis.xrange(deps.key())
        _, fields = entries[0]
        assert b"body" not in fields

    @pytest.mark.asyncio
    async def test_add_error_default_origin(self, redis, make_deps):
        deps = make_deps()
        await deps.add_error({"msg": "test error"})
        entries = await redis.xrange(deps.key())
        _, fields = entries[0]
        assert fields[b"type"] == b"error"
        assert fields[b"origin"] == b"developer"

    @pytest.mark.asyncio
    async def test_add_error_custom_origin(self, redis, make_deps):
        deps = make_deps()
        await deps.add_error({"msg": "custom"}, origin="myapp")
        entries = await redis.xrange(deps.key())
        _, fields = entries[0]
        assert fields[b"origin"] == b"myapp"

    @pytest.mark.asyncio
    async def test_add_info(self, redis, make_deps):
        deps = make_deps()
        await deps.add_info({"status": "processing"})
        entries = await redis.xrange(deps.key())
        _, fields = entries[0]
        assert fields[b"type"] == b"info"
        assert fields[b"origin"] == b"developer"


class TestCancel:
    @pytest.mark.asyncio
    async def test_cancel_returns_true_when_live(self, make_deps):
        deps = make_deps()
        await deps.start()
        result = await deps.cancel()
        assert result is True
        assert await deps.is_live() is False

    @pytest.mark.asyncio
    async def test_cancel_returns_false_when_not_live(self, make_deps):
        deps = make_deps()
        result = await deps.cancel()
        assert result is False

    @pytest.mark.asyncio
    async def test_cancel_after_stop_returns_false(self, make_deps):
        deps = make_deps()
        await deps.start()
        await deps.stop()
        result = await deps.cancel()
        assert result is False


class TestListen:
    @pytest.mark.asyncio
    async def test_listen_yields_events_until_end(self, make_deps):
        deps = make_deps()
        await deps.start()
        await deps.add(type="event", origin="test", body={"data": "hello"})
        await deps.stop()
        events = [e async for e in deps.listen(serialize=False, wait=1, timeout=1)]
        assert len(events) == 2
        assert events[0]["type"] == "begin"
        assert events[1]["type"] == "event"

    @pytest.mark.asyncio
    async def test_listen_serialized_returns_json_strings(self, make_deps):
        deps = make_deps()
        await deps.start()
        await deps.stop()
        events = [e async for e in deps.listen(serialize=True, wait=1, timeout=1)]
        assert len(events) == 1
        parsed = json.loads(events[0])
        assert parsed["type"] == "begin"
        assert isinstance(events[0], str)

    @pytest.mark.asyncio
    async def test_listen_timeout_on_no_stream(self, make_deps):
        deps = make_deps()
        events = [e async for e in deps.listen(wait=1, timeout=1)]
        assert events == []

    @pytest.mark.asyncio
    async def test_listen_parses_empty_body_as_empty_dict(self, redis, make_deps):
        deps = make_deps()
        await redis.xadd(deps.key(), {"type": "info", "origin": "test"})
        await redis.xadd(deps.key(), {"type": "end", "origin": "pydantic-ai-stream"})
        events = [e async for e in deps.listen(serialize=False, wait=1, timeout=1)]
        assert len(events) == 1
        assert events[0]["body"] == {}


class TestQuery:
    @pytest.mark.asyncio
    async def test_q_yields_active_sessions(self, redis, make_deps):
        deps_list = [make_deps(user_id=99) for _ in range(3)]
        await asyncio.gather(*(d.start() for d in deps_list))
        sessions = [s async for s in q(redis, 42, 99)]
        assert len(sessions) == 3
        session_ids = {s[2] for s in sessions}
        assert session_ids == {d.session_id for d in deps_list}

    @pytest.mark.asyncio
    async def test_q_filters_by_user_id(self, redis, make_deps):
        deps_user_1 = make_deps(user_id=1)
        deps_user_2 = make_deps(user_id=2)
        await deps_user_1.start()
        await deps_user_2.start()
        sessions = [s async for s in q(redis, 42, 1)]
        assert len(sessions) == 1
        assert sessions[0][1] == 1

    @pytest.mark.asyncio
    async def test_q_returns_empty_when_no_sessions(self, redis):
        sessions = [s async for s in q(redis, 42, 999)]
        assert sessions == []


class TestKeys:
    def test_key_format(self, make_deps):
        deps = make_deps()
        assert deps.key() == f"pyaix:42:{deps.user_id}:{deps.session_id}"

    def test_key_live_format(self, make_deps):
        deps = make_deps()
        assert deps.key_live() == f"pyaix:42:{deps.user_id}:{deps.session_id}:live"
