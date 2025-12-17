import pytest
import pytest_asyncio
from fakeredis import FakeAsyncRedis

from opale.agx import Config, configure


@pytest_asyncio.fixture
async def redis():
    client = FakeAsyncRedis()
    configure(Config(redis=client, key_prefix="test"))
    yield client
    await client.aclose()

