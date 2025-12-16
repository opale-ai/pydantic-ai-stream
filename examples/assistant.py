from dataclasses import dataclass
from typing import AsyncGenerator
from pathlib import Path
import typer
from functools import wraps
import asyncio
import json

from opale.agx import run as agx_run
from opale.agx.deps import Deps as AgxDeps
from opale.agx.session import Session as AgxSession
from opale.agx.stream import listen as agx_stream_listen

from pydantic_ai import Agent


DATA = Path(__file__).parent / "data"
DATA.mkdir(exist_ok=True)

# Typer stuff for cli

app = typer.Typer(pretty_exceptions_enable=False)


def run_async(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))

    return wrapper


# Pydantic AI Agent definition


@dataclass
class MyDeps(AgxDeps):
    def get_scope_id(self):
        return 0  # Not needed in this simple example


agent = Agent(
    "gateway/groq:openai/gpt-oss-120b",
    system_prompt="You are a helpful personal agent",
    deps_type=MyDeps,
)


# Agx Session class (mainly to implement save and load)


@dataclass
class MySession(AgxSession):
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


@app.command()
@run_async
async def prompt(
    session_id: str,
    prompt: str,
):
    agent.name = session_id
    async with agent:
        await agx_run(
            MySession(id=session_id),
            agent,
            prompt,
            deps=MyDeps(
                user_id=0,
                session_id=session_id,
            ),
        )


@app.command()
@run_async
async def stream_listen(
    session_id: str,
) -> AsyncGenerator[dict | bytes, None]:
    async for entry in agx_stream_listen(0, 0, session_id):
        yield entry


@app.command()
@run_async
async def session_read(session_id: str):
    nodes = await MySession(id=session_id).parse_nodes()
    print(json.dumps(nodes, indent=2))


if __name__ == "__main__":
    app()
