import os
from redis.asyncio import BlockingConnectionPool as AsyncBlockingConnectionPool
from redis.asyncio import Redis as AsyncRedis

REDIS_URL = os.environ["REDIS_URL"]
REDIS_POOL_SIZE = int(os.environ["REDIS_POOL_SIZE"])

AGX_SESSION = "agx:{scope_id}:{user_id}:{session_id}"
AGX_SESSION_LIVE = "agx:{scope_id}:{user_id}:{session_id}:live"

apool = AsyncBlockingConnectionPool.from_url(REDIS_URL, max_connections=REDIS_POOL_SIZE)
aclient = AsyncRedis(connection_pool=apool)
