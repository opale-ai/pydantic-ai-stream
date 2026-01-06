import pytest_asyncio
from dataclasses import dataclass
from fakeredis import FakeAsyncRedis

from pydantic_ai_stream import Deps


@dataclass
class AppDeps(Deps):
    def get_scope_id(self) -> int:
        return 42


@pytest_asyncio.fixture
async def redis():
    client = FakeAsyncRedis()
    yield client
    await client.aclose()


@pytest_asyncio.fixture
def make_deps(redis):
    _counter = 0

    def _make(user_id: int = 1) -> AppDeps:
        nonlocal _counter
        _counter += 1
        return AppDeps(redis=redis, user_id=user_id, session_id=f"sess-{_counter}")

    return _make
