import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

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

from .stream import add as stream_add

logger = logging.getLogger(__name__)

event_meta = {"type": "event", "origin": "pydantic-ai"}


@dataclass(kw_only=True)
class Node:
    idx: int
    events: dict[int, str] = field(default_factory=dict)
    parts: dict[int, dict] = field(default_factory=dict)
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

    async def add_node_begin(self, node):
        new = Node(idx=len(self.runtime.nodes))
        self.runtime.nodes.append(new)
        await stream_add(
            self.get_scope_id(),
            self.user_id,
            self.session_id,
            body={"idx": new.idx, "event": "llm-begin"},
            **event_meta,
        )
        body = {
            "idx": new.idx,
        }
        for part in node.request.parts:
            if isinstance(part, ToolReturnPart):
                await stream_add(
                    self.get_scope_id(),
                    self.user_id,
                    self.session_id,
                    body=body
                    | {
                        "part_kind": part.part_kind,
                        "tool_name": part.tool_name,
                        "tool_call_id": part.tool_call_id,
                        "content": part.content,
                    },
                    **event_meta,
                )

    async def add_node_end(self):
        current = self.runtime.nodes[-1]
        assert not current.stopped
        for idx in sorted(current.parts.keys()):
            event = current.events[idx]
            part = current.parts[idx]
            if part["args"].startswith("{}"):
                part["args"] = part["args"][2:]
            part["args"] = json.loads(part["args"])
            await stream_add(
                self.get_scope_id(),
                self.user_id,
                self.session_id,
                body={"idx": current.idx} | {"event": event, "event_idx": idx} | part,
                **event_meta,
            )
        await stream_add(
            self.get_scope_id(),
            self.user_id,
            self.session_id,
            body={"idx": current.idx, "event": "llm-end"},
            **event_meta,
        )
        current.stopped = True

    async def add_node_event(self, event):
        current = self.runtime.nodes[-1]
        body = {
            "idx": current.idx,
        }
        if isinstance(event, PartStartEvent):
            current.events[event.index] = event.event_kind
            body = body | {
                "event": event.event_kind,
                "event_idx": event.index,
            }
            part = event.part
            if isinstance(part, (TextPart, ThinkingPart)):
                await stream_add(
                    self.get_scope_id(),
                    self.user_id,
                    self.session_id,
                    body=body
                    | {
                        "part_kind": part.part_kind,
                        "content": part.content,
                    },
                    **event_meta,
                )
            elif isinstance(part, ToolCallPart):
                current.parts[event.index] = {
                    "part_kind": part.part_kind,
                    "tool_name": part.tool_name,
                    "tool_call_id": part.tool_call_id,
                    "args": part.args_as_json_str(),
                }
        elif isinstance(event, PartDeltaEvent):
            body = body | {
                "event": event.event_kind,
                "event_idx": event.index,
            }
            delta = event.delta
            if isinstance(delta, (TextPartDelta, ThinkingPartDelta)):
                await stream_add(
                    self.get_scope_id(),
                    self.user_id,
                    self.session_id,
                    body=body
                    | {
                        "part_delta_kind": delta.part_delta_kind,
                        "content_delta": delta.content_delta,
                    },
                    **event_meta,
                )
            elif isinstance(delta, ToolCallPartDelta):
                part = current.parts[event.index]
                assert part["tool_call_id"] == delta.tool_call_id
                if delta.tool_name_delta:
                    part["tool_name"] += delta.tool_name_delta
                if delta.args_delta:
                    part["args"] += delta.args_delta
        elif isinstance(event, FinalResultEvent):
            await stream_add(
                self.get_scope_id(),
                self.user_id,
                self.session_id,
                body=body | {"event": "answer"},
                **event_meta,
            )
        else:
            logger.error(f"Unknown event type - {type(event).__name__}")

    async def add_error(self, body: dict, origin="opale"):
        await stream_add(
            self.get_scope_id(),
            self.user_id,
            self.session_id,
            type="error",
            origin=origin,
            body=body,
        )

    async def add_info(self, body: dict, origin="opale"):
        await stream_add(
            self.get_scope_id(),
            self.user_id,
            self.session_id,
            type="info",
            origin=origin,
            body=body,
        )
