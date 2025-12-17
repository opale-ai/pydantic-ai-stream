import pytest
from dataclasses import dataclass

import orjson

from opale.agx import Deps, Node, Runtime


@dataclass
class AppDeps(Deps):
    def get_scope_id(self) -> int:
        return 42


@pytest.mark.asyncio
async def test_add_error(redis):
    deps = AppDeps(user_id=1, session_id="sess1")
    await deps.add_error({"msg": "test error"})
    key = "test:42:1:sess1"
    entries = await redis.xrange(key)
    assert len(entries) == 1
    _, fields = entries[0]
    assert fields[b"type"] == b"error"
    assert fields[b"origin"] == b"opale"
    body = orjson.loads(fields[b"body"])
    assert body["msg"] == "test error"


@pytest.mark.asyncio
async def test_add_error_custom_origin(redis):
    deps = AppDeps(user_id=1, session_id="sess2")
    await deps.add_error({"msg": "custom"}, origin="myapp")
    key = "test:42:1:sess2"
    entries = await redis.xrange(key)
    _, fields = entries[0]
    assert fields[b"origin"] == b"myapp"


@pytest.mark.asyncio
async def test_add_info(redis):
    deps = AppDeps(user_id=2, session_id="sess3")
    await deps.add_info({"status": "processing"})
    key = "test:42:2:sess3"
    entries = await redis.xrange(key)
    assert len(entries) == 1
    _, fields = entries[0]
    assert fields[b"type"] == b"info"
    body = orjson.loads(fields[b"body"])
    assert body["status"] == "processing"


class TestNodeTracking:
    def test_node_creation(self):
        node = Node(idx=0)
        assert node.idx == 0
        assert node.events == {}
        assert node.parts == {}
        assert node.stopped is False

    def test_runtime_nodes_list(self):
        runtime = Runtime()
        assert runtime.nodes == []
        runtime.nodes.append(Node(idx=0))
        runtime.nodes.append(Node(idx=1))
        assert len(runtime.nodes) == 2
