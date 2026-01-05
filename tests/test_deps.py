import pytest
import json
from .test_log import AppDeps
import asyncio

from pydantic_ai_stream import q


@pytest.mark.asyncio
async def test_start_sets_live_flag(redis):
    deps = AppDeps(redis=redis, user_id=1, session_id="1")
    await deps.start()
    assert await deps.is_live() is True


@pytest.mark.asyncio
async def test_start_emits_begin_event(redis):
    deps = AppDeps(redis=redis, user_id=1, session_id="2")
    await deps.start()
    entries = await redis.xrange(deps.key())
    assert len(entries) == 1
    _, fields = entries[0]
    assert fields[b"type"] == b"begin"
    assert fields[b"origin"] == b"pydantic-ai-stream"


@pytest.mark.asyncio
async def test_stop_clears_live_flag(redis):
    deps = AppDeps(redis=redis, user_id=1, session_id="3")
    await deps.start()
    assert await deps.is_live() is True
    await deps.stop()
    assert await deps.is_live() is False


@pytest.mark.asyncio
async def test_stop_emits_end_event(redis):
    deps = AppDeps(redis=redis, user_id=1, session_id="4")
    await deps.start()
    assert await deps.is_live() is True
    await deps.stop()
    assert await deps.is_live() is False
    entries = await redis.xrange(deps.key())
    assert len(entries) == 2
    _, fields = entries[1]
    assert fields[b"type"] == b"end"


@pytest.mark.asyncio
async def test_add_event(redis):
    deps = AppDeps(redis=redis, user_id=1, session_id="5")
    await deps.add(
        type="event",
        origin="pydantic-ai",
        body={"idx": 0, "event": "llm-begin"},
    )
    entries = await redis.xrange(deps.key())
    assert len(entries) == 1
    _, fields = entries[0]
    assert fields[b"type"] == b"event"
    body = json.loads(fields[b"body"])
    assert body["idx"] == 0


@pytest.mark.asyncio
async def test_cancel_returns_true_when_live(redis):
    deps = AppDeps(redis=redis, user_id=1, session_id="6")
    await deps.start()
    result = await deps.cancel()
    assert result is True
    assert await deps.is_live() is False


@pytest.mark.asyncio
async def test_cancel_returns_false_when_not_live(redis):
    deps = AppDeps(redis=redis, user_id=1, session_id="7")
    result = await deps.cancel()
    assert result is False


@pytest.mark.asyncio
async def test_listen_yields_events(redis):
    deps = AppDeps(redis=redis, user_id=1, session_id="8")
    await deps.start()
    await deps.add(type="event", origin="test", body={"data": "hello"})
    await deps.stop()
    events = []
    async for event in deps.listen(serialize=False, wait=1, timeout=1):
        events.append(event)
    assert len(events) == 2
    assert events[0]["type"] == "begin"
    assert events[1]["type"] == "event"


@pytest.mark.asyncio
async def test_listen_serialized(redis):
    deps = AppDeps(redis=redis, user_id=1, session_id="9")
    await deps.start()
    await deps.stop()
    events = []
    async for event in deps.listen(serialize=True, wait=1, timeout=1):
        events.append(event)
    assert len(events) == 1
    parsed = json.loads(events[0])
    assert parsed["type"] == "begin"


@pytest.mark.asyncio
async def test_q_yields_active_sessions(redis):
    deps_list = [
        AppDeps(redis=redis, user_id=2, session_id=f"10.{i + 1}") for i in range(3)
    ]
    asyncio.gather(*(deps.start() for deps in deps_list))
    sessions = []
    async for scope_id, user_id, session_id in q(redis, deps_list[0].get_scope_id(), 2):
        sessions.append((scope_id, user_id, session_id))
    assert len(sessions) == 3
    session_ids = {s[2] for s in sessions}
    for session_id in session_ids:
        assert session_id[:3] == "10."
