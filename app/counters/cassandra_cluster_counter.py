from .acounter import ACounter
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra.policies import RoundRobinPolicy
from cassandra import ConsistencyLevel


CONSISTENCY_LEVELS = {
    "ONE": ConsistencyLevel.ONE,
    "QUORUM": ConsistencyLevel.QUORUM,
}


class CassandraClusterCounter(ACounter):
    def __init__(self, hosts: list[str], port: int, keyspace: str, consistency_level: str):
        self.__cluster = Cluster(
            contact_points=hosts,
            port=port,
            load_balancing_policy=RoundRobinPolicy(),
            protocol_version=4,
        )
        self.__session = self.__cluster.connect()

        self.__session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspace}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '3'}}
        """)
        self.__session.set_keyspace(keyspace)

        self.__session.execute("""
            CREATE TABLE IF NOT EXISTS user_counter (
                user_id int PRIMARY KEY,
                likes   counter
            )
        """)

        cl = CONSISTENCY_LEVELS.get(consistency_level.upper())
        if cl is None:
            raise ValueError(
                f"Unsupported consistency_level '{consistency_level}'. Use ONE or QUORUM."
            )

        self.__inc_stmt = self.__session.prepare(
            "UPDATE user_counter SET likes = likes + 1 WHERE user_id = 1"
        )
        self.__inc_stmt.consistency_level = cl

        self.__get_stmt = self.__session.prepare(
            "SELECT likes FROM user_counter WHERE user_id = 1"
        )

    def inc(self) -> None:
        self.__session.execute(self.__inc_stmt)

    def get(self) -> int:
        result = self.__session.execute(self.__get_stmt).one()
        return result.likes if result else 0

    def close(self) -> None:
        self.__session.shutdown()
        self.__cluster.shutdown()
