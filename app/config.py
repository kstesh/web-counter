from yaml import safe_load
import logging
from .counters.acounter import ACounter
from .counters.in_memory_counter import InMemoryCounter
from .counters.file_counter import FileCounter
from .counters.postgres_counter import PostgresCounter
from .counters.mongo_counter import MongoCounter
from .counters.cassandra_counter import CassandraCounter
from .counters.cassandra_cluster_counter import CassandraClusterCounter
from .counters.mongo_cluster_counter import MongoClusterCounter

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

        case "cassandra":
            if "cassandra" not in config:
                logging.error("Cassandra config section missing")
                raise RuntimeError("Missing 'Cassandra' section in config")
            cass_cfg = config["cassandra"]
            required_keys = ["host", "port", "keyspace"]
            missing = [k for k in required_keys if k not in cass_cfg]

            if missing:
                logging.error(f"missing Cassandra config keys: {missing}")
                raise RuntimeError(f"Missing Cassandra config keys: {missing}")

            counter = CassandraCounter(host=cass_cfg["host"], port=cass_cfg["port"], keyspace=cass_cfg["keyspace"])

        case "cassandracluster":
            if "cassandracluster" not in config:
                logging.error("Cassandracluster config section missing")
                raise RuntimeError("Missing 'cassandracluster' section in config")
            cc_cfg = config["cassandracluster"]
            required_keys = ["hosts", "port", "keyspace", "consistency_level"]
            missing = [k for k in required_keys if k not in cc_cfg]
            if missing:
                logging.error(f"missing cassandracluster config keys: {missing}")
                raise RuntimeError(f"Missing cassandracluster config keys: {missing}")

            counter = CassandraClusterCounter(
                hosts=cc_cfg["hosts"],
                port=int(cc_cfg["port"]),
                keyspace=cc_cfg["keyspace"],
                consistency_level=cc_cfg["consistency_level"],
            )

        case "mongocluster":
            if "mongocluster" not in config:
                logging.error("Mongocluster config section missing")
                raise RuntimeError("Missing 'mongocluster' section in config")
            mc_cfg = config["mongocluster"]
            required_keys = ["hosts", "replica_set", "write_concern"]
            missing = [k for k in required_keys if k not in mc_cfg]
            if missing:
                logging.error(f"missing mongocluster config keys: {missing}")
                raise RuntimeError(f"Missing mongocluster config keys: {missing}")

            write_concern = mc_cfg["write_concern"]
            counter = MongoClusterCounter(
                hosts=mc_cfg["hosts"],
                replica_set=mc_cfg["replica_set"],
                write_concern=write_concern
            )

        case _:
            logging.error(f"unknown storage type {config['type']}")
            raise RuntimeError(f"Unsupported STORAGE type: {config['type']}")

    return counter

