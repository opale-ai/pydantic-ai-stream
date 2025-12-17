import asyncio

import pytest
import orjson

from opale.agx import add, cancel, is_live, listen, q, start, stop


@pytest.mark.asyncio
async def test_start_sets_live_flag(redis):
    await start(1, 100, "session-1")
    assert await is_live(1, 100, "session-1") is True


@pytest.mark.asyncio
async def test_start_emits_begin_event(redis):
    await start(1, 100, "session-2")
    key = "test:1:100:session-2"
    entries = await redis.xrange(key)
    assert len(entries) == 1
    _, fields = entries[0]
    assert fields[b"type"] == b"begin"
    assert fields[b"origin"] == b"opale"


@pytest.mark.asyncio
async def test_stop_clears_live_flag(redis):
    await start(1, 100, "session-3")
    assert await is_live(1, 100, "session-3") is True
    await stop(1, 100, "session-3")
    assert await is_live(1, 100, "session-3") is False


@pytest.mark.asyncio
async def test_stop_emits_end_event(redis):
    await start(1, 100, "session-4")
    await stop(1, 100, "session-4")
    key = "test:1:100:session-4"
    entries = await redis.xrange(key)
    assert len(entries) == 2
    _, fields = entries[1]
    assert fields[b"type"] == b"end"


@pytest.mark.asyncio
async def test_add_event(redis):
    await add(1, 100, "session-5", type="event", origin="pydantic-ai", body={"idx": 0, "event": "llm-begin"})
    key = "test:1:100:session-5"
    entries = await redis.xrange(key)
    assert len(entries) == 1
    _, fields = entries[0]
    assert fields[b"type"] == b"event"
    body = orjson.loads(fields[b"body"])
    assert body["idx"] == 0


@pytest.mark.asyncio
async def test_cancel_returns_true_when_live(redis):
    await start(1, 100, "session-6")
    result = await cancel(1, 100, "session-6")
    assert result is True
    assert await is_live(1, 100, "session-6") is False


@pytest.mark.asyncio
async def test_cancel_returns_false_when_not_live(redis):
    result = await cancel(1, 100, "session-7")
    assert result is False


@pytest.mark.asyncio
async def test_listen_yields_events(redis):
    await start(1, 100, "session-8")
    await add(1, 100, "session-8", type="event", origin="test", body={"data": "hello"})
    await stop(1, 100, "session-8")
    events = []
    async for event in listen(1, 100, "session-8", serialize=False, wait=1, timeout=1):
        events.append(event)
    assert len(events) == 2
    assert events[0]["type"] == "begin"
    assert events[1]["type"] == "event"


@pytest.mark.asyncio
async def test_listen_serialized(redis):
    await start(1, 100, "session-9")
    await stop(1, 100, "session-9")
    events = []
    async for event in listen(1, 100, "session-9", serialize=True, wait=1, timeout=1):
        events.append(event)
    assert len(events) == 1
    parsed = orjson.loads(events[0])
    assert parsed["type"] == "begin"


@pytest.mark.asyncio
async def test_q_yields_active_sessions(redis):
    await start(1, 100, "session-a")
    await start(1, 100, "session-b")
    await start(1, 200, "session-c")
    sessions = []
    async for scope_id, user_id, session_id in q(1, 100):
        sessions.append((scope_id, user_id, session_id))
    assert len(sessions) == 2
    session_ids = {s[2] for s in sessions}
    assert "session-a" in session_ids
    assert "session-b" in session_ids

