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

from opale.agx import (
    Config,
    Deps,
    Session,
    cancel,
    configure,
    listen,
    run,
)


@dataclass
class AppDeps(Deps):
    def get_scope_id(self) -> int:
        return 1


class FileSession(Session):
    def __init__(self, session_id: str):
        super().__init__()
        self.session_id = session_id

    async def load(self) -> None:
        pass

    async def save(self) -> None:
        pass


agent = Agent(
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
        return str(eval(expression))
    except Exception:
        return "Error evaluating expression"


redis_client: Redis | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client
    redis_client = Redis.from_url("redis://localhost:6379", decode_responses=False)
    configure(Config(redis=redis_client, key_prefix="agx"))
    yield
    if redis_client:
        await redis_client.aclose()


app = FastAPI(lifespan=lifespan)


class PromptRequest(BaseModel):
    prompt: str
    session_id: str | None = None


@app.post("/chat")
async def chat(req: PromptRequest, request: Request):
    session_id = req.session_id or str(uuid.uuid4())

    async def run_agent():
        await run(
            FileSession(session_id),
            agent,
            req.prompt,
            deps=AppDeps(user_id=1, session_id=session_id),
        )

    asyncio.create_task(run_agent())
    await asyncio.sleep(0.1)

    async def event_stream():
        async for event in listen(1, 1, session_id, serialize=True):
            if await request.is_disconnected():
                await cancel(1, 1, session_id)
                break
            yield f"data: {event}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@app.post("/chat/{session_id}/cancel")
async def cancel_chat(session_id: str):
    cancelled = await cancel(1, 1, session_id)
    return {"cancelled": cancelled}


@app.get("/chat/{session_id}/stream")
async def stream_chat(session_id: str, request: Request):
    async def event_stream():
        async for event in listen(1, 1, session_id, serialize=True):
            if await request.is_disconnected():
                break
            yield f"data: {event}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)

