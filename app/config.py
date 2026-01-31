from yaml import safe_load
import logging
from .counters.acounter import ACounter
from .counters.in_memory_counter import InMemoryCounter
from .counters.file_counter import FileCounter


def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return safe_load(f)


def get_configured_counter() -> ACounter:
    config = load_config("./config/config.yaml")
    logging.info(f"starting with config: {config}")

    match config["type"]:
        case "file":
            if "file_path" not in config:
                logging.error(f"file path not found in config: {config}")
                raise RuntimeError("No 'file_path' variable in config")
            counter = FileCounter(config["file_path"])

        case "memory":
            counter = InMemoryCounter()

        case _:
            logging.error(f"unknown storage type {config['type']}")
            raise RuntimeError(f"Unsupported STORAGE type: {config['type']}")

    return counter

