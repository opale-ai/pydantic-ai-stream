import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import orjson
from pydantic_ai.messages import (
    FinalResultEvent,
    PartDeltaEvent,
    PartStartEvent,
    TextPart,
    TextPartDelta,
    ThinkingPart,
    ThinkingPartDelta,
    ToolCallPart,
    ToolCallPartDelta,
    ToolReturnPart,
)

from .stream import add

if TYPE_CHECKING:
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
    user_id: int
    session_id: str
    runtime: Runtime = field(default_factory=Runtime)

    @abstractmethod
    def get_scope_id(self) -> int:
        pass

    async def add_node_begin(self, node: "ModelRequestNode[Any, Any]") -> None:
        new = Node(idx=len(self.runtime.nodes))
        self.runtime.nodes.append(new)
        await add(
            self.get_scope_id(),
            self.user_id,
            self.session_id,
            type="event",
            origin="pydantic-ai",
            body={"idx": new.idx, "event": "llm-begin"},
        )
        for part in node.request.parts:
            if isinstance(part, ToolReturnPart):
                await add(
                    self.get_scope_id(),
                    self.user_id,
                    self.session_id,
                    type="event",
                    origin="pydantic-ai",
                    body={
                        "idx": new.idx,
                        "part_kind": part.part_kind,
                        "tool_name": part.tool_name,
                        "tool_call_id": part.tool_call_id,
                        "content": part.content,
                    },
                )

    async def add_node_end(self) -> None:
        current = self.runtime.nodes[-1]
        assert not current.stopped
        for idx in sorted(current.parts.keys()):
            event = current.events[idx]
            part = current.parts[idx]
            args_str = part["args"]
            if args_str.startswith("{}"):
                args_str = args_str[2:]
            part["args"] = orjson.loads(args_str)
            await add(
                self.get_scope_id(),
                self.user_id,
                self.session_id,
                type="event",
                origin="pydantic-ai",
                body={"idx": current.idx, "event": event, "event_idx": idx} | part,
            )
        await add(
            self.get_scope_id(),
            self.user_id,
            self.session_id,
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
            body |= {"event": event.event_kind, "event_idx": event.index}
            part = event.part
            if isinstance(part, (TextPart, ThinkingPart)):
                await add(
                    self.get_scope_id(),
                    self.user_id,
                    self.session_id,
                    type="event",
                    origin="pydantic-ai",
                    body=body | {"part_kind": part.part_kind, "content": part.content},
                )
            elif isinstance(part, ToolCallPart):
                current.parts[event.index] = {
                    "part_kind": part.part_kind,
                    "tool_name": part.tool_name,
                    "tool_call_id": part.tool_call_id,
                    "args": part.args_as_json_str(),
                }
        elif isinstance(event, PartDeltaEvent):
            body |= {"event": event.event_kind, "event_idx": event.index}
            delta = event.delta
            if isinstance(delta, (TextPartDelta, ThinkingPartDelta)):
                await add(
                    self.get_scope_id(),
                    self.user_id,
                    self.session_id,
                    type="event",
                    origin="pydantic-ai",
                    body=body | {"part_delta_kind": delta.part_delta_kind, "content_delta": delta.content_delta},
                )
            elif isinstance(delta, ToolCallPartDelta):
                stored_part = current.parts[event.index]
                assert stored_part["tool_call_id"] == delta.tool_call_id
                if delta.tool_name_delta:
                    stored_part["tool_name"] += delta.tool_name_delta
                if delta.args_delta:
                    stored_part["args"] += delta.args_delta
        elif isinstance(event, FinalResultEvent):
            await add(
                self.get_scope_id(),
                self.user_id,
                self.session_id,
                type="event",
                origin="pydantic-ai",
                body=body | {"event": "answer"},
            )
        else:
            logger.error(f"Unknown event type - {type(event).__name__}")

    async def add_error(self, body: dict[str, Any], origin: str = "opale") -> None:
        await add(
            self.get_scope_id(),
            self.user_id,
            self.session_id,
            type="error",
            origin=origin,
            body=body,
        )

    async def add_info(self, body: dict[str, Any], origin: str = "opale") -> None:
        await add(
            self.get_scope_id(),
            self.user_id,
            self.session_id,
            type="info",
            origin=origin,
            body=body,
        )
