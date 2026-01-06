import os
from dataclasses import dataclass
from pathlib import Path
from functools import wraps
import asyncio
import json

import typer
from pydantic_ai import Agent
from redis.asyncio import Redis as AsyncRedis

from pydantic_ai_stream import run, Deps, Session

DATA = Path(__file__).parent / "data"
DATA.mkdir(exist_ok=True)

# Typer stuff for cli

app = typer.Typer(pretty_exceptions_enable=False)


def run_async(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))

    return wrapper


# Session class (mainly to implement save and load for persistence)


@dataclass
class MySession(Session):
    id: str

    @property
    def path(self):
        return DATA / f"{self.id}.sess"

    async def load(self):
        if self.path.exists():
            with self.path.open("rb") as f:
                self.msgs_from_json(f.read())

    async def save(self):
        with self.path.open("wb") as f:
            f.write(self.msgs_to_json())

    async def parse_nodes(self):
        if self.path.exists():
            with self.path.open("rb") as f:
                return self.nodes_from_msgs(json.load(f))


# Pydantic AI Agent definition


@dataclass
class MyDeps(Deps):
    def get_scope_id(self):
        return 0  # Not needed in this simple example


agent = Agent(
    "gateway/groq:openai/gpt-oss-120b",
    system_prompt="You are a helpful personal agent",
    deps_type=MyDeps,
)


async def agent_ask(deps: MyDeps, session_id: str, prompt: str):
    agent.name = session_id
    async with agent:
        await run(MySession(id=session_id), agent, prompt, deps)


async def agent_listen(deps: MyDeps):
    async for entry in deps.listen():
        print(entry)


@app.command()
@run_async
async def prompt(session_id: str, prompt: str):
    redis = AsyncRedis.from_url(os.environ["REDIS_URL"])
    deps = MyDeps(
        redis=redis,
        user_id=0,
        session_id=session_id,
    )
    t1 = asyncio.create_task(agent_ask(deps, session_id, prompt))
    t2 = asyncio.create_task(agent_listen(deps))
    await asyncio.gather(t1, t2)


@app.command()
@run_async
async def read(session_id: str):
    nodes = await MySession(id=session_id).parse_nodes()
    print(json.dumps(nodes, indent=2))


if __name__ == "__main__":
    app()
