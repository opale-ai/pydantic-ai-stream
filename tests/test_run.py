import pytest
from dataclasses import dataclass
from unittest.mock import MagicMock

from pydantic_ai_stream import Deps, Session, run


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
    def __init__(self, deps: MockDeps):
        self.result = MagicMock()
        self.result.new_messages.return_value = []
        self.ctx = MagicMock()
        self.ctx.deps.user_deps = deps

    def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration


class MockAgent:
    def iter(self, *args, deps, **kwargs):
        return MockAgentContext(deps)

    @staticmethod
    def is_model_request_node(node):
        return False


class MockAgentContext:
    def __init__(self, deps: MockDeps):
        self.agent_run = MockAgentRun(deps)

    async def __aenter__(self):
        return self.agent_run

    async def __aexit__(self, *args):
        pass


@pytest.mark.asyncio
async def test_run_loads_session(redis):
    session = MockSession()
    agent = MockAgent()
    deps = MockDeps(redis=redis, user_id=1, session_id="test")
    await run(session, agent, "hello", deps)
    assert session.loaded is True


@pytest.mark.asyncio
async def test_run_saves_session_on_success(redis):
    session = MockSession()
    agent = MockAgent()
    deps = MockDeps(redis=redis, user_id=1, session_id="test2")
    await run(session, agent, "hello", deps)
    assert session.saved is True


@pytest.mark.asyncio
async def test_run_starts_stream(redis):
    session = MockSession()
    agent = MockAgent()
    deps = MockDeps(redis=redis, user_id=1, session_id="test3")
    await run(session, agent, "hello", deps)
    entries = await redis.xrange(deps.key())
    assert len(entries) >= 1
    _, fields = entries[0]
    assert fields[b"type"] == b"begin"


@pytest.mark.asyncio
async def test_run_stops_stream(redis):
    session = MockSession()
    agent = MockAgent()
    deps = MockDeps(redis=redis, user_id=1, session_id="test4")
    await run(session, agent, "hello", deps)
    entries = await redis.xrange(deps.key())
    types = [e[1][b"type"] for e in entries]
    assert b"end" in types
    assert await deps.is_live() is False
