"""FastAPI SSE example for pydantic-ai-stream."""

import asyncio
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime

from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from pydantic_ai import Agent, RunContext
from redis.asyncio import Redis

from pydantic_ai_stream import Deps, Session, run


@dataclass
class AppDeps(Deps):
    def get_scope_id(self) -> int:
        return 1


@dataclass
class MemorySession(Session):
    session_id: str

    async def load(self) -> None:
        pass

    async def save(self) -> None:
        pass


agent: Agent[AppDeps, str] = Agent(
    "openai:gpt-4o-mini",
    system_prompt="You are a helpful assistant. Be concise.",
    deps_type=AppDeps,
)


@agent.tool
async def get_current_time(ctx: RunContext[AppDeps]) -> str:
    """Get the current time."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


@agent.tool
async def calculate(ctx: RunContext[AppDeps], expression: str) -> str:
    """Evaluate a mathematical expression safely."""
    allowed = set("0123456789+-*/(). ")
    if not all(c in allowed for c in expression):
        return "Invalid expression"
    try:
        return str(eval(expression))  # noqa: S307
    except Exception:
        return "Error evaluating expression"


redis_client: Redis | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client
    redis_client = Redis.from_url("redis://localhost:6379", decode_responses=False)
    yield
    if redis_client:
        await redis_client.aclose()


app = FastAPI(lifespan=lifespan)


class PromptRequest(BaseModel):
    prompt: str
    session_id: str | None = None


@app.post("/chat")
async def chat(req: PromptRequest, request: Request):
    assert redis_client is not None
    session_id = req.session_id or str(uuid.uuid4())
    deps = AppDeps(redis=redis_client, user_id=1, session_id=session_id)

    async def run_agent():
        await run(
            MemorySession(session_id=session_id),
            agent,
            req.prompt,
            deps=deps,
        )

    asyncio.create_task(run_agent())
    await asyncio.sleep(0.1)

    async def event_stream():
        async for event in deps.listen(serialize=True):
            if await request.is_disconnected():
                await deps.cancel()
                break
            yield f"data: {event}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@app.post("/chat/{session_id}/cancel")
async def cancel_chat(session_id: str):
    assert redis_client is not None
    deps = AppDeps(redis=redis_client, user_id=1, session_id=session_id)
    cancelled = await deps.cancel()
    return {"cancelled": cancelled}


@app.get("/chat/{session_id}/stream")
async def stream_chat(session_id: str, request: Request):
    assert redis_client is not None
    deps = AppDeps(redis=redis_client, user_id=1, session_id=session_id)

    async def event_stream():
        async for event in deps.listen(serialize=True):
            if await request.is_disconnected():
                break
            yield f"data: {event}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
