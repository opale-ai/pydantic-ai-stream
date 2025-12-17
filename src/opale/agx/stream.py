from collections.abc import AsyncGenerator
from typing import Any

import orjson

from .config import get_config, live_key, stream_key


async def start(
    scope_id: int,
    user_id: int,
    session_id: str,
) -> None:
    cfg = get_config()
    await cfg.redis.set(live_key(scope_id, user_id, session_id), 1)
    await add(
        scope_id,
        user_id,
        session_id,
        type="begin",
        origin="opale",
        body={"session_id": session_id},
    )


async def stop(
    scope_id: int,
    user_id: int,
    session_id: str,
    grace_period: int = 5,
) -> None:
    cfg = get_config()
    await add(scope_id, user_id, session_id, type="end", origin="opale")
    await cfg.redis.delete(live_key(scope_id, user_id, session_id))
    await cfg.redis.expire(stream_key(scope_id, user_id, session_id), grace_period)


async def add(
    scope_id: int,
    user_id: int,
    session_id: str,
    *,
    type: str,
    origin: str,
    body: dict[str, Any] | None = None,
) -> None:
    cfg = get_config()
    fields: dict[str, Any] = {"type": type, "origin": origin}
    if body is not None:
        fields["body"] = orjson.dumps(body)
    await cfg.redis.xadd(stream_key(scope_id, user_id, session_id), fields)  # type: ignore[arg-type]


async def is_live(
    scope_id: int,
    user_id: int,
    session_id: str,
) -> bool:
    cfg = get_config()
    return await cfg.redis.get(live_key(scope_id, user_id, session_id)) is not None


async def listen(
    scope_id: int,
    user_id: int,
    session_id: str,
    *,
    wait: int = 3,
    timeout: int = 60,
    serialize: bool = True,
) -> AsyncGenerator[dict[str, Any] | str, None]:
    cfg = get_config()
    key = stream_key(scope_id, user_id, session_id)
    counter, last_id = 0, "0"
    while True:
        res = await cfg.redis.xread({key: last_id}, block=1000)
        if len(res) == 0:
            if (last_id == "0" and counter >= wait) or (last_id != "0" and counter >= timeout):
                break
            counter += 1
            continue
        counter = 0
        for _, entries in res:
            for entry_id, entry in entries:
                last_id = entry_id if isinstance(entry_id, str) else entry_id.decode()
                ev_type = entry[b"type"].decode()
                if ev_type == "end":
                    return
                ev_origin = entry[b"origin"].decode()
                ev_body: dict[str, Any] = orjson.loads(entry.get(b"body", b"{}"))
                event: dict[str, Any] = {"type": ev_type, "origin": ev_origin, "body": ev_body}
                if serialize:
                    yield orjson.dumps(event).decode()
                else:
                    yield event


async def cancel(
    scope_id: int,
    user_id: int,
    session_id: str,
) -> bool:
    cfg = get_config()
    return await cfg.redis.getdel(live_key(scope_id, user_id, session_id)) is not None


async def q(
    scope_id: int,
    user_id: int,
) -> AsyncGenerator[tuple[int, int, str], None]:
    cfg = get_config()
    pattern = f"{cfg.key_prefix}:{scope_id}:{user_id}:*:live"
    async for k in cfg.redis.scan_iter(pattern):
        key_str = k if isinstance(k, str) else k.decode()
        parts = key_str.rsplit(":", 4)
        if len(parts) >= 4:
            _, s_id, u_id, sess_id = parts[0], parts[-4], parts[-3], parts[-2]
            yield int(s_id), int(u_id), sess_id
