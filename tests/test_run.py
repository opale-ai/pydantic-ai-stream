"""Tests for run() function and AgxCanceledError."""

import json
from dataclasses import dataclass
from unittest.mock import MagicMock

import pytest

from pydantic_ai_stream import AgxCanceledError, Deps, Session, run


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
    def __init__(self, deps: MockDeps, nodes=None):
        self.result = MagicMock()
        self.result.new_messages.return_value = []
        self.ctx = MagicMock()
        self.ctx.deps.user_deps = deps
        self._nodes = nodes or []
        self._iter = iter(self._nodes)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._iter)
        except StopIteration:
            raise StopAsyncIteration


class MockAgent:
    def __init__(self, nodes=None):
        self._nodes = nodes

    def iter(self, *args, deps, **kwargs):
        return MockAgentContext(deps, self._nodes)

    @staticmethod
    def is_model_request_node(node):
        return getattr(node, "_is_model_request", False)


class MockAgentContext:
    def __init__(self, deps: MockDeps, nodes=None):
        self.agent_run = MockAgentRun(deps, nodes)

    async def __aenter__(self):
        return self.agent_run

    async def __aexit__(self, *args):
        pass


class TestRunLifecycle:
    @pytest.mark.asyncio
    async def test_loads_session(self, redis):
        session = MockSession()
        agent = MockAgent()
        deps = MockDeps(redis=redis, user_id=1, session_id="test-load")
        await run(session, agent, "hello", deps)
        assert session.loaded is True

    @pytest.mark.asyncio
    async def test_saves_session_on_success(self, redis):
        session = MockSession()
        agent = MockAgent()
        deps = MockDeps(redis=redis, user_id=1, session_id="test-save")
        await run(session, agent, "hello", deps)
        assert session.saved is True

    @pytest.mark.asyncio
    async def test_starts_stream(self, redis):
        session = MockSession()
        agent = MockAgent()
        deps = MockDeps(redis=redis, user_id=1, session_id="test-start")
        await run(session, agent, "hello", deps)
        entries = await redis.xrange(deps.key())
        _, fields = entries[0]
        assert fields[b"type"] == b"begin"

    @pytest.mark.asyncio
    async def test_stops_stream(self, redis):
        session = MockSession()
        agent = MockAgent()
        deps = MockDeps(redis=redis, user_id=1, session_id="test-stop")
        await run(session, agent, "hello", deps)
        entries = await redis.xrange(deps.key())
        types = [e[1][b"type"] for e in entries]
        assert b"end" in types
        assert await deps.is_live() is False


class TestRunCancellation:
    @pytest.mark.asyncio
    async def test_raises_canceled_error_when_not_live(self, redis):
        session = MockSession()
        deps = MockDeps(redis=redis, user_id=1, session_id="test-cancel")

        class CancelingAgent:
            def iter(self, *args, deps, **kwargs):
                return CancelingContext(deps)

            @staticmethod
            def is_model_request_node(node):
                return False

        class CancelingContext:
            def __init__(self, deps):
                self.deps = deps

            async def __aenter__(self):
                await self.deps.cancel()
                return CancelingRun(self.deps)

            async def __aexit__(self, *args):
                pass

        class CancelingRun:
            def __init__(self, deps):
                self.result = MagicMock()
                self.result.new_messages.return_value = []
                self.ctx = MagicMock()
                self.ctx.deps.user_deps = deps
                self._yielded = False

            def __aiter__(self):
                return self

            async def __anext__(self):
                if self._yielded:
                    raise StopAsyncIteration
                self._yielded = True
                return MagicMock()

        with pytest.raises(AgxCanceledError):
            await run(session, CancelingAgent(), "hello", deps)

    @pytest.mark.asyncio
    async def test_emits_error_on_cancellation(self, redis):
        session = MockSession()
        deps = MockDeps(redis=redis, user_id=1, session_id="test-cancel-error")

        class CancelingAgent:
            def iter(self, *args, deps, **kwargs):
                return CancelingContext(deps)

            @staticmethod
            def is_model_request_node(node):
                return False

        class CancelingContext:
            def __init__(self, deps):
                self.deps = deps

            async def __aenter__(self):
                await self.deps.cancel()
                return CancelingRun(self.deps)

            async def __aexit__(self, *args):
                pass

        class CancelingRun:
            def __init__(self, deps):
                self.result = MagicMock()
                self.ctx = MagicMock()
                self.ctx.deps.user_deps = deps
                self._yielded = False

            def __aiter__(self):
                return self

            async def __anext__(self):
                if self._yielded:
                    raise StopAsyncIteration
                self._yielded = True
                return MagicMock()

        with pytest.raises(AgxCanceledError):
            await run(session, CancelingAgent(), "hello", deps)

        entries = await redis.xrange(deps.key())
        error_entries = [e for e in entries if e[1][b"type"] == b"error"]
        assert len(error_entries) == 1
        body = json.loads(error_entries[0][1][b"body"])
        assert body["msg"] == "canceled"


class TestRunErrorHandling:
    @pytest.mark.asyncio
    async def test_emits_error_on_exception(self, redis):
        session = MockSession()
        deps = MockDeps(redis=redis, user_id=1, session_id="test-crash")

        class CrashingAgent:
            def iter(self, *args, deps, **kwargs):
                return CrashingContext(deps)

            @staticmethod
            def is_model_request_node(node):
                return False

        class CrashingContext:
            def __init__(self, deps):
                self.deps = deps

            async def __aenter__(self):
                return CrashingRun(self.deps)

            async def __aexit__(self, *args):
                pass

        class CrashingRun:
            def __init__(self, deps):
                self.result = MagicMock()
                self.ctx = MagicMock()
                self.ctx.deps.user_deps = deps

            def __aiter__(self):
                return self

            async def __anext__(self):
                raise RuntimeError("Agent crashed!")

        with pytest.raises(RuntimeError, match="Agent crashed!"):
            await run(session, CrashingAgent(), "hello", deps)

        entries = await redis.xrange(deps.key())
        error_entries = [e for e in entries if e[1][b"type"] == b"error"]
        assert len(error_entries) == 1
        body = json.loads(error_entries[0][1][b"body"])
        assert "crashed" in body["msg"]

    @pytest.mark.asyncio
    async def test_stream_stops_on_exception(self, redis):
        session = MockSession()
        deps = MockDeps(redis=redis, user_id=1, session_id="test-crash-stop")

        class CrashingAgent:
            def iter(self, *args, deps, **kwargs):
                return CrashingContext(deps)

            @staticmethod
            def is_model_request_node(node):
                return False

        class CrashingContext:
            def __init__(self, deps):
                self.deps = deps

            async def __aenter__(self):
                return CrashingRun(self.deps)

            async def __aexit__(self, *args):
                pass

        class CrashingRun:
            def __init__(self, deps):
                self.result = MagicMock()
                self.ctx = MagicMock()
                self.ctx.deps.user_deps = deps

            def __aiter__(self):
                return self

            async def __anext__(self):
                raise ValueError("boom")

        with pytest.raises(ValueError):
            await run(session, CrashingAgent(), "hello", deps)

        assert await deps.is_live() is False
        entries = await redis.xrange(deps.key())
        types = [e[1][b"type"] for e in entries]
        assert b"end" in types


class TestAgxCanceledError:
    def test_is_exception(self):
        assert issubclass(AgxCanceledError, Exception)

    def test_can_be_raised_and_caught(self):
        with pytest.raises(AgxCanceledError):
            raise AgxCanceledError()
