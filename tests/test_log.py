import pytest
import json
from dataclasses import dataclass

from pydantic_ai_stream import Deps


@dataclass
class AppDeps(Deps):
    def get_scope_id(self) -> int:
        return 42


@pytest.mark.asyncio
async def test_add_error(redis):
    deps = AppDeps(redis=redis, user_id=1, session_id="sess1")
    await deps.add_error({"msg": "test error"})
    entries = await redis.xrange(deps.key())
    assert len(entries) == 1
    _, fields = entries[0]
    assert fields[b"type"] == b"error"
    assert fields[b"origin"] == b"developer"
    body = json.loads(fields[b"body"])
    assert body["msg"] == "test error"


@pytest.mark.asyncio
async def test_add_error_custom_origin(redis):
    deps = AppDeps(redis=redis, user_id=1, session_id="sess2")
    await deps.add_error({"msg": "custom"}, origin="myapp")
    entries = await redis.xrange(deps.key())
    _, fields = entries[0]
    assert fields[b"origin"] == b"myapp"


@pytest.mark.asyncio
async def test_add_info(redis):
    deps = AppDeps(redis=redis, user_id=2, session_id="sess3")
    await deps.add_info({"status": "processing"})
    entries = await redis.xrange(deps.key())
    assert len(entries) == 1
    _, fields = entries[0]
    assert fields[b"type"] == b"info"
    body = json.loads(fields[b"body"])
    assert body["status"] == "processing"
