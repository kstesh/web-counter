from .acounter import ACounter
from contextlib import contextmanager
import psycopg2
from psycopg2 import pool

class PostgresCounter(ACounter):
    def __init__(self, host: str, port: int, dbname: str, user: str, password: str, minconn: int = 1, maxconn: int = 10):
        self._pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=minconn,
            maxconn=maxconn,
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )

    @contextmanager
    def _db_session(self):
        conn = self._pool.getconn()
        conn.autocommit = True
        cur = conn.cursor()
        try:
            yield cur, conn
        finally:
            cur.close()
            self._pool.putconn(conn)

    def inc(self) -> None:
        with self._db_session() as (cur, conn):
            cur.execute(
                "UPDATE user_counter SET counter = counter + 1 WHERE user_id = 1"
            )

    def get(self) -> int:
        with self._db_session() as (cur, conn):
            cur.execute(
                "SELECT counter FROM user_counter WHERE user_id = 1"
            )
            return cur.fetchone()[0]

    def close(self):
        self._pool.closeall()
