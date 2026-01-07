# pydantic-ai-stream

Production runtime for [pydantic-ai](https://ai.pydantic.dev/) agents. Provides structured event streaming via Redis Streams, session persistence, and cancellation support.

![img](imgs/ai-app.svg)

## Install

```bash
pip install pydantic-ai-stream
```

## Quick Start

```python
from dataclasses import dataclass
from redis.asyncio import Redis
from pydantic_ai import Agent

from pydantic_ai_stream import Deps, Session, run

# 1. Define your deps (includes Redis client)
@dataclass
class MyDeps(Deps):
    def get_scope_id(self) -> int:
        return 1

# 2. Implement session persistence
@dataclass
class MySession(Session):
    session_id: str

    async def load(self) -> None:
        pass  # Load from your storage

    async def save(self) -> None:
        pass  # Save to your storage

# 3. Create agent and run
agent = Agent("openai:gpt-4o-mini", deps_type=MyDeps)
redis = Redis.from_url("redis://localhost:6379")

async def main():
    deps = MyDeps(redis=redis, user_id=1, session_id="session-1")
    await run(
        MySession(session_id="session-1"),
        agent,
        "Hello, world!",
        deps=deps,
    )

# 4. Stream events (in another coroutine/process)
async def consume():
    deps = MyDeps(redis=redis, user_id=1, session_id="session-1")
    async for event in deps.listen():
        print(event)
```

## Protocol Reference

### Stream Format

Events are stored in Redis Streams with three fields:

| Field | Type | Description |
|-------|------|-------------|
| `type` | string | Event type |
| `origin` | string | Event source |
| `body` | JSON | Event payload |

### Event Types

| type | origin | Usage |
|------|--------|-------|
| `begin` | pydantic-ai-stream | Session start |
| `event` | pydantic-ai | LLM interaction events |
| `error` | developer / custom | Error during execution |
| `info` | developer / custom | Informational |
| `end` | pydantic-ai-stream | Session complete |

### Event Body Schema (type=event)

| Field | Type | When |
|-------|------|------|
| `idx` | int | Always — node index |
| `event` | str | `llm-begin`, `llm-end`, `part_start`, `part_delta`, `answer` |
| `event_idx` | int | Part events — part index |
| `part_kind` | str | `text`, `thinking`, `tool-call`, `tool-return` |
| `content` | str | Start events — full content |
| `content_delta` | str | Delta events — incremental |
| `tool_name` | str | Tool call/return |
| `tool_call_id` | str | Tool correlation |
| `args` | dict | Tool call — emitted at part end |

## Configuration

Configure the Redis key prefix via settings:

```python
from pydantic_ai_stream import settings

settings.set_redis_prefix("myapp")  # default: "pyaix"
```

### Key Patterns

```
{prefix}:{scope_id}:{user_id}:{session_id}       # stream
{prefix}:{scope_id}:{user_id}:{session_id}:live  # live flag
```

## API Reference

### Core

```python
async def run(session, agent, user_prompt, deps, **kwargs) -> None
```
Execute agent with streaming. Wraps `Agent.iter()`, emits events, handles cancellation.

```python
class AgxCanceledError(Exception)
```
Raised when execution is cancelled via `deps.cancel()`.

### Session

```python
class Session(ABC):
    msgs: list[ModelMessage]

    async def load(self) -> None: ...       # Load from storage
    async def save(self) -> None: ...       # Save to storage
    def msgs_to_json(self) -> bytes         # Serialize messages
    def msgs_from_json(self, data: bytes)   # Deserialize messages
    def get_user_prompt(self) -> str        # Extract initial prompt
    @staticmethod
    def nodes_from_msgs(msgs) -> list       # Reconstruct node structure
```

### Deps

```python
@dataclass
class Deps(ABC):
    redis: AsyncRedis
    user_id: int
    session_id: str

    @abstractmethod
    def get_scope_id(self) -> int: ...

    # Stream operations
    async def start(self) -> None
    async def stop(self, grace_period: int = 5) -> None
    async def is_live(self) -> bool
    async def listen(self, *, wait=3, timeout=60, serialize=True) -> AsyncGenerator
    async def cancel(self) -> bool

    # Event emission
    async def add(self, *, type: str, origin: str, body: dict | None = None) -> None
    async def add_error(self, body: dict, origin: str = "developer") -> None
    async def add_info(self, body: dict, origin: str = "developer") -> None

    # Node tracking (called by run())
    async def add_node_begin(self, node) -> None
    async def add_node_end(self) -> None
    async def add_node_event(self, event) -> None
```

### Query Active Sessions

```python
async def q(redis, scope_id, user_id) -> AsyncGenerator[tuple[int, int, str], None]
```
Scan for active sessions (those with live flag set).

## Example: FastAPI SSE

See `examples/fastapi_sse.py` for a complete example with:
- SSE streaming endpoint
- Cancellation support
- Tool usage

```python
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from pydantic_ai_stream import Deps, Session, run

@app.post("/chat")
async def chat(prompt: str, session_id: str):
    deps = MyDeps(redis=redis, user_id=1, session_id=session_id)

    # Start agent in background
    asyncio.create_task(run(MySession(...), agent, prompt, deps=deps))

    # Stream events via SSE
    async def stream():
        async for event in deps.listen():
            yield f"data: {event}\n\n"

    return StreamingResponse(stream(), media_type="text/event-stream")

@app.post("/chat/{session_id}/cancel")
async def cancel_chat(session_id: str):
    deps = MyDeps(redis=redis, user_id=1, session_id=session_id)
    return {"cancelled": await deps.cancel()}
```

## License

MIT
