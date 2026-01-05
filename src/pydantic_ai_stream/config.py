from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from redis.asyncio import Redis as AsyncRedis


@dataclass
class Config:
    redis: "AsyncRedis"
    key_prefix: str = "agx"


_config: Config | None = None


def configure(config: Config) -> None:
    global _config
    _config = config


def get_config() -> Config:
    if _config is None:
        raise RuntimeError("agx not configured. Call configure(Config(...)) first.")
    return _config


def stream_key(scope_id: int, user_id: int, session_id: str) -> str:
    cfg = get_config()
    return f"{cfg.key_prefix}:{scope_id}:{user_id}:{session_id}"


def live_key(scope_id: int, user_id: int, session_id: str) -> str:
    cfg = get_config()
    return f"{cfg.key_prefix}:{scope_id}:{user_id}:{session_id}:live"

