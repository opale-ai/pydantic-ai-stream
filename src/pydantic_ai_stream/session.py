from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from itertools import chain
from typing import Any

from pydantic_ai.messages import ModelMessage, ModelMessagesTypeAdapter, UserPromptPart


@dataclass(kw_only=True)
class Session(ABC):
    msgs: list[ModelMessage] = field(default_factory=list)

    def add_msgs(self, msgs: list[ModelMessage]) -> None:
        self.msgs.extend(msgs)

    def msgs_to_json(self) -> bytes:
        return ModelMessagesTypeAdapter.dump_json(self.msgs)

    def msgs_from_json(self, data: bytes) -> None:
        self.msgs = ModelMessagesTypeAdapter.validate_json(data)

    def get_user_prompt(self) -> str:
        if not self.msgs:
            return "No title"
        msg: ModelMessage = self.msgs[0]
        for part in msg.parts:
            if isinstance(part, UserPromptPart):
                content = part.content
                if isinstance(content, str):
                    return content
                return "No title"
        return "No title"

    @staticmethod
    def nodes_from_msgs(msgs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if len(msgs) % 2 != 0:
            return []
        nodes: list[dict[str, Any]] = []
        for n in range(len(msgs) // 2):
            req = msgs[2 * n]
            if req.get("kind") != "request":
                continue
            res = msgs[2 * n + 1]
            if res.get("kind") != "response":
                continue
            node: dict[str, Any] = {**res, "kind": None, "parts": []}
            nodes.append(node)
            for part in chain(req.get("parts", []), res.get("parts", [])):
                if part.get("part_kind") != "system-prompt":
                    node["parts"].append({**part, "signature": None})
        return nodes

    @abstractmethod
    async def load(self) -> None:
        pass

    @abstractmethod
    async def save(self) -> None:
        pass
