import pytest
from dataclasses import dataclass
from unittest.mock import MagicMock

from opale.agx import Deps, Session, is_live


@dataclass
class MockSession(Session):
    loaded: bool = False
    saved: bool = False

    async def load(self) -> None:
        self.loaded = True

    async def save(self) -> None:
        self.saved = True


@dataclass
class MockDeps(Deps):
    def get_scope_id(self) -> int:
        return 1


class MockAgentRun:
    def __init__(self, session_id: str):
        self.result = MagicMock()
        self.result.new_messages.return_value = []
        self.ctx = MagicMock()
        self.ctx.deps.user_deps = MockDeps(user_id=1, session_id=session_id)

    def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration


class MockAgent:
    def __init__(self, session_id: str):
        self.session_id = session_id

    def iter(self, *args, **kwargs):
        return MockAgentContext(self.session_id)

    @staticmethod
    def is_model_request_node(node):
        return False


class MockAgentContext:
    def __init__(self, session_id: str):
        self.agent_run = MockAgentRun(session_id)

    async def __aenter__(self):
        return self.agent_run

    async def __aexit__(self, *args):
        pass


@pytest.mark.asyncio
async def test_run_loads_session(redis):
    from opale.agx import run

    session = MockSession()
    agent = MockAgent("test")
    deps = MockDeps(user_id=1, session_id="test")
    await run(session, agent, "hello", deps)
    assert session.loaded is True


@pytest.mark.asyncio
async def test_run_saves_session_on_success(redis):
    from opale.agx import run

    session = MockSession()
    agent = MockAgent("test2")
    deps = MockDeps(user_id=1, session_id="test2")
    await run(session, agent, "hello", deps)
    assert session.saved is True


@pytest.mark.asyncio
async def test_run_starts_stream(redis):
    from opale.agx import run

    session = MockSession()
    agent = MockAgent("test3")
    deps = MockDeps(user_id=1, session_id="test3")
    await run(session, agent, "hello", deps)
    key = "test:1:1:test3"
    entries = await redis.xrange(key)
    assert len(entries) >= 1
    _, fields = entries[0]
    assert fields[b"type"] == b"begin"


@pytest.mark.asyncio
async def test_run_stops_stream(redis):
    from opale.agx import run

    session = MockSession()
    agent = MockAgent("test4")
    deps = MockDeps(user_id=1, session_id="test4")
    await run(session, agent, "hello", deps)
    key = "test:1:1:test4"
    entries = await redis.xrange(key)
    types = [e[1][b"type"] for e in entries]
    assert b"end" in types
    assert await is_live(1, 1, "test4") is False
