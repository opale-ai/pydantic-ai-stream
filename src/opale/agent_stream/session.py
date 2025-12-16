from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from itertools import chain

from pydantic_ai.messages import ModelMessage, ModelMessagesTypeAdapter, UserPromptPart


@dataclass(kw_only=True)
class Session(ABC):
    msgs: list[ModelMessage] = field(default_factory=list)

    def add_msgs(self, msgs):
        self.msgs.extend(msgs)

    def msgs_to_json(self) -> bytes:
        return ModelMessagesTypeAdapter.dump_json(self.msgs)

    def msgs_from_json(self, data: bytes):
        self.msgs = ModelMessagesTypeAdapter.validate_json(data)

    def get_user_prompt(self) -> str:
        msg: ModelMessage = self.msgs[0]
        for part in msg.parts:
            if isinstance(part, UserPromptPart):
                return part.content
        return "No title"

    @classmethod
    def nodes_from_msgs(cls, msgs: list[dict]):
        assert len(msgs) % 2 == 0
        nodes = []
        for n in range(len(msgs) // 2):
            req = msgs[2 * n]
            assert req["kind"] == "request"
            res = msgs[2 * n + 1]
            assert res["kind"] == "response"
            node = {} | res | {"kind": None, "parts": []}
            nodes.append(node)
            for part in chain(req["parts"], res["parts"]):
                if part["part_kind"] not in ("system-prompt"):
                    node["parts"].append({} | part | {"signature": None})
        return nodes

    @abstractmethod
    async def load(self):
        pass

    @abstractmethod
    async def save(self):
        pass
