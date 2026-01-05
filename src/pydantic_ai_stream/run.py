import logging
from typing import Any

from pydantic_ai.agent import Agent

from .deps import Deps
from .session import Session
from .stream import is_live, start, stop

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
    await start(deps.get_scope_id(), deps.user_id, deps.session_id)
    try:
        async with agent.iter(
            user_prompt, deps=deps, message_history=session.msgs, **kwargs
        ) as agent_run:
            async for node in agent_run:
                if not await is_live(deps.get_scope_id(), deps.user_id, deps.session_id):
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
        await stop(deps.get_scope_id(), deps.user_id, deps.session_id)
