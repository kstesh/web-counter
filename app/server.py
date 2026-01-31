from fastapi import FastAPI
from .config import get_configured_counter
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.counter = get_configured_counter()
    yield

app = FastAPI(lifespan=lifespan)


@app.get("/inc")
def increment():
    app.state.counter.inc()
    return {"status": "ok"}


@app.get("/count")
def get_count():
    return {"count": app.state.counter.get()}
