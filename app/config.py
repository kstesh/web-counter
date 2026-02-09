from yaml import safe_load
import logging
from .counters.acounter import ACounter
from .counters.in_memory_counter import InMemoryCounter
from .counters.file_counter import FileCounter
from .counters.postgres_counter import PostgresCounter
from .counters.mongo_counter import MongoCounter

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

        case "postgres":
            if "postgres" not in config:
                logging.error("Postgres config section missing")
                raise RuntimeError("Missing 'postgres' section in config")

            pg_cfg = config["postgres"]
            required_keys = ["host", "port", "dbname", "user", "password"]
            missing = [k for k in required_keys if k not in pg_cfg]

            if missing:
                logging.error(f"missing PostgreSQL config keys: {missing}")
                raise RuntimeError(f"Missing PostgreSQL config keys: {missing}")

            counter = PostgresCounter(
                host=pg_cfg["host"],
                port=int(pg_cfg["port"]),
                dbname=pg_cfg["dbname"],
                user=pg_cfg["user"],
                password=pg_cfg["password"],
                minconn=int(pg_cfg.get("minconn", 5)),
                maxconn=int(pg_cfg.get("maxconn", 20))
            )
        case "mongo":
            if "mongo" not in config:
                logging.error("Mongo config section missing")
                raise RuntimeError("Missing 'mongo' section in config")
            mongo_cfg = config["mongo"]
            required_keys = ["host", "port", "user", "password"]
            missing = [k for k in required_keys if k not in mongo_cfg]

            if missing:
                logging.error(f"missing MongoDB config keys: {missing}")
                raise RuntimeError(f"Missing MongoDB config keys: {missing}")

            uri = f"mongodb://{mongo_cfg['user']}:{mongo_cfg['password']}@{mongo_cfg['host']}:{mongo_cfg['port']}/?"
            counter = MongoCounter(uri=uri)

        case _:
            logging.error(f"unknown storage type {config['type']}")
            raise RuntimeError(f"Unsupported STORAGE type: {config['type']}")

    return counter

