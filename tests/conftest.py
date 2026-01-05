import pytest_asyncio
from fakeredis import FakeAsyncRedis


@pytest_asyncio.fixture
async def redis():
    client = FakeAsyncRedis()
    yield client
    await client.aclose()
