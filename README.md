# opale-agent-stream

Production runtime for [pydantic-ai](https://ai.pydantic.dev/) agents. Provides structured event streaming via Redis Streams, session persistence, and cancellation support.

![img](imgs/ai-app.png)

## Install

```bash
pip install opale-agent-stream
```

## Quick Start

```python
from dataclasses import dataclass
from redis.asyncio import Redis
from pydantic_ai import Agent

from opale.agx import Config, Deps, Session, configure, run, listen

# 1. Configure Redis
redis = Redis.from_url("redis://localhost:6379")
configure(Config(redis=redis))

# 2. Define your deps
@dataclass
class MyDeps(Deps):
    def get_scope_id(self) -> int:
        return 1

# 3. Implement session persistence
@dataclass
class MySession(Session):
    id: str
    
    async def load(self) -> None:
        pass  # Load from your storage
    
    async def save(self) -> None:
        pass  # Save to your storage

# 4. Create agent and run
agent = Agent("openai:gpt-4o-mini", deps_type=MyDeps)

async def main():
    await run(
        MySession(id="session-1"),
        agent,
        "Hello, world!",
        deps=MyDeps(user_id=1, session_id="session-1"),
    )

# 5. Stream events (in another coroutine/process)
async def consume():
    async for event in listen(1, 1, "session-1"):
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
| `begin` | opale | Session start |
| `event` | pydantic-ai | LLM interaction events |
| `error` | opale / custom | Error during execution |
| `info` | opale / custom | Informational |
| `end` | opale | Session complete |

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
| `args` | dict | Tool call — emitted at node end |

## Configuration

```python
from dataclasses import dataclass
from redis.asyncio import Redis
from opale.agx import Config, configure

@dataclass
class Config:
    redis: Redis      # Injected async Redis client
    key_prefix: str   # Key prefix (default: "agx")

# Initialize once at startup
redis = Redis.from_url("redis://localhost:6379")
configure(Config(redis=redis, key_prefix="myapp"))
```

### Key Patterns

```
{prefix}:{scope_id}:{user_id}:{session_id}       # stream
{prefix}:{scope_id}:{user_id}:{session_id}:live  # live flag
```

## API Reference

### Core

```python
async def run(session, agent, user_prompt, deps, **kwargs)
```
Execute agent with streaming. Wraps `Agent.iter()`, emits events, handles cancellation.

```python
class AgxCanceledError(Exception)
```
Raised when execution is cancelled via `cancel()`.

### Session

```python
class Session(ABC):
    msgs: list[ModelMessage]
    
    async def load(self) -> None: ...      # Load from storage
    async def save(self) -> None: ...      # Save to storage
    def msgs_to_json(self) -> bytes        # Serialize messages
    def msgs_from_json(self, data: bytes)  # Deserialize messages
    def get_user_prompt(self) -> str       # Extract initial prompt
    @staticmethod
    def nodes_from_msgs(msgs) -> list      # Reconstruct node structure
```

### Deps

```python
class Deps(ABC):
    user_id: int
    session_id: str
    runtime: Runtime
    
    @abstractmethod
    def get_scope_id(self) -> int: ...
    
    async def add_node_begin(self, node) -> None
    async def add_node_end(self) -> None
    async def add_node_event(self, event) -> None
    async def add_error(self, body: dict, origin: str = "opale") -> None
    async def add_info(self, body: dict, origin: str = "opale") -> None
```

### Stream Operations

```python
async def start(scope_id, user_id, session_id) -> None
async def stop(scope_id, user_id, session_id, grace_period=5) -> None
async def add(scope_id, user_id, session_id, *, type, origin, body=None) -> None
async def is_live(scope_id, user_id, session_id) -> bool
async def listen(scope_id, user_id, session_id, *, wait=3, timeout=60, serialize=True) -> AsyncGenerator
async def cancel(scope_id, user_id, session_id) -> bool
async def q(scope_id, user_id) -> AsyncGenerator[tuple[int, int, str], None]
```

## Example: FastAPI SSE

See `examples/fastapi_sse.py` for a complete example with:
- SSE streaming endpoint
- Cancellation support
- Tool usage

```python
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from opale.agx import configure, Config, run, listen, cancel

app = FastAPI()

@app.post("/chat")
async def chat(prompt: str, session_id: str):
    # Start agent in background
    asyncio.create_task(run(...))
    
    # Stream events via SSE
    async def stream():
        async for event in listen(1, 1, session_id):
            yield f"data: {event}\n\n"
    
    return StreamingResponse(stream(), media_type="text/event-stream")

@app.post("/chat/{session_id}/cancel")
async def cancel_chat(session_id: str):
    return {"cancelled": await cancel(1, 1, session_id)}
```

## License

MIT
