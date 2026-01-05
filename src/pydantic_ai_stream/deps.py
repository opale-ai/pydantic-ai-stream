import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any
from collections.abc import AsyncGenerator
import json

from pydantic_ai.messages import (
    FinalResultEvent,
    PartDeltaEvent,
    PartStartEvent,
    PartEndEvent,
    TextPart,
    TextPartDelta,
    ThinkingPart,
    ThinkingPartDelta,
    ToolCallPart,
    ToolCallPartDelta,
    ToolReturnPart,
)

from .settings import settings

if TYPE_CHECKING:
    from redis.asyncio import Redis as AsyncRedis
    from pydantic_ai._agent_graph import ModelRequestNode

logger = logging.getLogger(__name__)


@dataclass(kw_only=True)
class Node:
    idx: int
    events: dict[int, str] = field(default_factory=dict)
    parts: dict[int, dict[str, Any]] = field(default_factory=dict)
    stopped: bool = False


@dataclass(kw_only=True)
class Runtime:
    nodes: list[Node] = field(default_factory=list)


@dataclass(kw_only=True)
class Deps(ABC):
    redis: "AsyncRedis"
    user_id: int
    session_id: str
    runtime: Runtime = field(default_factory=Runtime)

    @abstractmethod
    def get_scope_id(self) -> int:
        raise NotImplementedError()

    def key(self) -> str:
        return f"{settings.redis_prefix}:{self.get_scope_id()}:{self.user_id}:{self.session_id}"

    def key_live(self) -> str:
        return f"{settings.redis_prefix}:{self.get_scope_id()}:{self.user_id}:{self.session_id}:live"

    async def add(self, *, type: str, origin: str, body: dict[str, Any] | None = None) -> None:
        fields: dict[str, Any] = {"type": type, "origin": origin}
        if body is not None:
            fields["body"] = json.dumps(body)
        await self.redis.xadd(self.key(), fields)  # type: ignore[arg-type]

    async def add_node_begin(self, node: "ModelRequestNode[Any, Any]") -> None:
        new = Node(idx=len(self.runtime.nodes))
        self.runtime.nodes.append(new)
        await self.add(
            type="event",
            origin="pydantic-ai",
            body={"idx": new.idx, "event": "llm-begin"},
        )
        for part in node.request.parts:
            if isinstance(part, ToolReturnPart):
                await self.add(
                    type="event",
                    origin="pydantic-ai",
                    body={
                        "idx": new.idx,
                        "event": "part_start",
                        "part_kind": part.part_kind,
                        "tool_name": part.tool_name,
                        "tool_call_id": part.tool_call_id,
                        "content": part.content,
                    },
                )

    async def add_node_end(self) -> None:
        current = self.runtime.nodes[-1]
        assert not current.stopped
        await self.add(
            type="event",
            origin="pydantic-ai",
            body={"idx": current.idx, "event": "llm-end"},
        )
        current.stopped = True

    async def add_node_event(self, event: PartStartEvent | PartDeltaEvent | FinalResultEvent | Any) -> None:
        current = self.runtime.nodes[-1]
        body: dict[str, Any] = {"idx": current.idx}
        if isinstance(event, PartStartEvent):
            current.events[event.index] = event.event_kind
            part = event.part
            if isinstance(part, (TextPart, ThinkingPart)):
                await self.add(
                    type="event",
                    origin="pydantic-ai",
                    body=body
                    | {
                        "event": event.event_kind,
                        "event_idx": event.index,
                        "part_kind": part.part_kind,
                        "content": part.content,
                    },
                )
            elif isinstance(part, ToolCallPart):
                current.parts[event.index] = {
                    "part_kind": part.part_kind,
                    "tool_name": part.tool_name,
                    "tool_call_id": part.tool_call_id,
                    "args": part.args_as_json_str(),
                }
        elif isinstance(event, PartDeltaEvent):
            delta = event.delta
            if isinstance(delta, (TextPartDelta, ThinkingPartDelta)):
                await self.add(
                    type="event",
                    origin="pydantic-ai",
                    body=body
                    | {
                        "event": event.event_kind,
                        "event_idx": event.index,
                        "part_delta_kind": delta.part_delta_kind,
                        "content_delta": delta.content_delta,
                    },
                )
            elif isinstance(delta, ToolCallPartDelta):
                stored_part = current.parts[event.index]
                assert stored_part["tool_call_id"] == delta.tool_call_id
                if delta.tool_name_delta:
                    stored_part["tool_name"] += delta.tool_name_delta
                if delta.args_delta:
                    stored_part["args"] += delta.args_delta
        elif isinstance(event, PartEndEvent):
            if isinstance(event.part, ToolCallPart):
                part = current.parts[event.index]
                assert part["tool_call_id"] == ToolCallPart, event.part.tool_call_id
                part["args"] = json.loads(part["args"])
                await self.add(
                    type="event",
                    origin="pydantic-ai",
                    body=body
                    | {
                        "event": current.events[event.index],
                        "event_idx": event.index,
                    }
                    | part,
                )
        elif isinstance(event, FinalResultEvent):
            await self.add(
                type="event",
                origin="pydantic-ai",
                body=body | {"event": "answer"},
            )
        else:
            logger.error(f"Unknown event type - {type(event).__name__}")

    async def add_error(self, body: dict[str, Any], origin: str = "developer") -> None:
        await self.add(
            type="error",
            origin=origin,
            body=body,
        )

    async def add_info(self, body: dict[str, Any], origin: str = "developer") -> None:
        await self.add(
            type="info",
            origin=origin,
            body=body,
        )

    async def start(self) -> None:
        await self.redis.set(self.key_live(), 1)
        await self.add(
            type="begin",
            origin="pydantic-ai-stream",
            body={"session_id": self.session_id},
        )

    async def stop(self, grace_period: int = 5) -> None:
        await self.add(type="end", origin="pydantic-ai-stream")
        await self.redis.delete(self.key_live())
        await self.redis.expire(self.key(), grace_period)

    async def is_live(self) -> bool:
        return await self.redis.get(self.key_live()) is not None

    async def listen(
        self, *, wait: int = 3, timeout: int = 60, serialize: bool = True
    ) -> AsyncGenerator[dict[str, Any] | str, None]:
        counter, last_id = 0, "0"
        while True:
            res = await self.redis.xread({self.key(): last_id}, block=1000)
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
                    ev_body: dict[str, Any] = json.loads(entry.get(b"body", "{}"))
                    event: dict[str, Any] = {"type": ev_type, "origin": ev_origin, "body": ev_body}
                    if serialize:
                        yield json.dumps(event)
                    else:
                        yield event

    async def cancel(self) -> bool:
        return await self.redis.getdel(self.key_live()) is not None
