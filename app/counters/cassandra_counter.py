from .acounter import ACounter
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra.policies import RoundRobinPolicy

class CassandraCounter(ACounter):
    def __init__(self, host, port, keyspace):
        self.__cluster = Cluster(contact_points=[host],
                                 port=port,
                                 load_balancing_policy=RoundRobinPolicy(),
                                 protocol_version=4)
        self.__session = self.__cluster.connect()

        self.__session.execute(f"""
              CREATE KEYSPACE IF NOT EXISTS {keyspace}
              WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': '1' }}
              """)
        self.__session.set_keyspace(keyspace)

        self.__session.execute("""
              CREATE TABLE IF NOT EXISTS user_counter (
                  user_id int PRIMARY KEY,
                  counter counter
              )
              """)



    def inc(self) -> None:
        self.__session.execute(f"""
            UPDATE user_counter SET counter = counter + 1 WHERE user_id = 1
        """)

    def get(self):
        result = self.__session.execute(f"""
        SELECT counter FROM user_counter WHERE user_id = 1
        """).one()
        return result.counter if result else 0

    def close(self) -> None:
        self.__session.shutdown()



