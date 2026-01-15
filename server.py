from fastapi import FastAPI
from in_memory_counter import InMemoryCounter
from file_counter import FileCounter
from pupupu import load_config
from contextlib import asynccontextmanager
import logging

@asynccontextmanager
async def lifespan(app: FastAPI):
    config = load_config("backend")
    logging.info(f"starting with config: {config}")

    match config["type"]:
        case "file":
            if "file_path" not in config:
                logging.error(f"file path not found in config: {config}")
                raise RuntimeError("No 'file_path' variable in config")
            app.state.counter = FileCounter(config["file_path"])

        case "memory":
            app.state.counter = InMemoryCounter()

        case _:
            logging.error(f"unknown storage type {config['type']}")
            raise RuntimeError(f"Unsupported STORAGE type: {config['type']}")

    yield

app = FastAPI(lifespan=lifespan)



@app.get("/inc")
def increment():
    app.state.counter.inc()
    return {"status": "ok"}


@app.get("/count")
def get_count():
    return {"count": app.state.counter.get()}
