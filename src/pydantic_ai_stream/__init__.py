import logging

from pydantic_ai import Agent
from typing import Any, AsyncGenerator
from redis.asyncio import Redis as AsyncRedis

from .settings import settings
from .deps import Deps
from .session import Session


__all__ = ["settings", "Deps", "Session", "AgxCanceledError", "run", "q"]

logger = logging.getLogger(__name__)


class AgxCanceledError(Exception):
    pass


async def run(
    session: Session,
    agent: Agent[Any, Any],
    user_prompt: str,
    deps: Deps,
    **kwargs: Any,
) -> None:
    await session.load()
    await deps.start()
    try:
        async with agent.iter(
            user_prompt, deps=deps, message_history=session.msgs, **kwargs
        ) as agent_run:  # type: ignore[arg-type]
            async for node in agent_run:
                if not await deps.is_live():
                    raise AgxCanceledError()
                if Agent.is_model_request_node(node):
                    await deps.add_node_begin(node)
                    async with node.stream(agent_run.ctx) as node_stream:
                        async for event in node_stream:
                            await agent_run.ctx.deps.user_deps.add_node_event(event)
                    await deps.add_node_end()
            if agent_run.result is not None:
                session.add_msgs(agent_run.result.new_messages())
            await session.save()
    except AgxCanceledError:
        await deps.add_error({"msg": "canceled"})
        raise
    except Exception as e:
        await deps.add_error({"msg": f"crashed - {e}"})
        raise
    finally:
        await deps.stop()


async def q(
    redis: AsyncRedis,
    scope_id: int,
    user_id: int,
) -> AsyncGenerator[tuple[int, int, str], None]:
    async for k in redis.scan_iter(
        f"{settings.redis_prefix}:{scope_id}:{user_id}:*:live"
    ):
        key_str = k if isinstance(k, str) else k.decode()
        parts = key_str.rsplit(":", 4)
        if len(parts) >= 4:
            _, s_id, u_id, sess_id = parts[0], parts[-4], parts[-3], parts[-2]
            yield int(s_id), int(u_id), sess_id
