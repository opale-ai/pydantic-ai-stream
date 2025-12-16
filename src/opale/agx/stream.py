import base64
from typing import AsyncGenerator
import json

from .store import aclient as redis_aclient
from .store import AGX_SESSION, AGX_SESSION_LIVE


async def start(
    scope_id: int,
    user_id: int,
    session_id: str,
):
    await redis_aclient.set(
        AGX_SESSION_LIVE.format(
            scope_id=scope_id, user_id=user_id, session_id=session_id
        ),
        1,
    )
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
    grace_period=5,
):
    await add(
        scope_id,
        user_id,
        session_id,
        type="end",
        origin="opale",
    )
    await redis_aclient.delete(
        AGX_SESSION_LIVE.format(
            scope_id=scope_id, user_id=user_id, session_id=session_id
        )
    )
    await redis_aclient.expire(
        AGX_SESSION.format(scope_id=scope_id, user_id=user_id, session_id=session_id),
        grace_period,
    )


async def add(
    scope_id: int,
    user_id: int,
    session_id: str,
    *,
    type: str,
    origin: str,
    body: dict | None = None,
):
    fields = {
        "type": type,
        "origin": origin,
    }
    if body is not None:
        fields["body"] = json.dumps(body)
    await redis_aclient.xadd(
        AGX_SESSION.format(scope_id=scope_id, user_id=user_id, session_id=session_id),
        fields,
    )


async def is_live(
    scope_id: int,
    user_id: int,
    session_id: str,
) -> bool:
    return (
        await redis_aclient.get(
            AGX_SESSION_LIVE.format(
                scope_id=scope_id, user_id=user_id, session_id=session_id
            )
        )
        is not None
    )


async def listen(
    scope_id: int,
    user_id: int,
    session_id: str,
    *,
    wait: int = 3,  # time to wait for processing to start
    timeout: int = 60,  # time to wait for no logs before leaving
    serialize: bool = True,
) -> AsyncGenerator[dict | bytes, None]:
    key = AGX_SESSION.format(scope_id=scope_id, user_id=user_id, session_id=session_id)
    counter, id = 0, 0
    while True:
        res = await redis_aclient.xread({key: id}, block=1000)
        if len(res) == 0:
            if id == 0 and counter == wait or id != 0 and counter == timeout:
                break
            counter += 1
        else:
            counter = 0
            for _, entries in res:
                for id, entry in entries:
                    type = entry[b"type"].decode()
                    if type == "end":
                        return
                    origin = entry[b"origin"].decode()
                    body = json.loads(entry[b"body"])
                    entry = {"type": type, "origin": origin, "body": body}
                    if not serialize:
                        yield entry
                    else:
                        entry["body"] = base64.b64encode(
                            json.dumps(body).encode()
                        ).decode("ascii")
                        yield json.dumps(entry)


async def cancel(
    scope_id: int,
    user_id: int,
    session_id: str,
) -> bool:
    return (
        await redis_aclient.getdel(
            AGX_SESSION_LIVE.format(
                scope_id=scope_id, user_id=user_id, session_id=session_id
            )
        )
    ) is not None


async def q(
    *,
    scope_id: int | None = None,
    user_id: int | None = None,
) -> AsyncGenerator[tuple[int, int, str], None]:
    async for k in redis_aclient.scan_iter(
        AGX_SESSION_LIVE.format(
            scope_id=str(scope_id) if scope_id is not None else "*",
            user_id=str(user_id) if user_id is not None else "*",
            session_id="*",
        )
    ):
        _, scope_id, user_id, session_id, *_ = k.decode().split(":")
        yield int(scope_id), int(user_id), session_id
