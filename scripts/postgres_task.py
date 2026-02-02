from contextlib import contextmanager
import psycopg2
import threading
import time



@contextmanager
def db_session(isolation_level=None):
    conn = psycopg2.connect(
        host="localhost",
        port=5433,
        dbname="counter_db",
        user="counter_user",
        password="counter_pass"
    )
    conn.autocommit = False

    if isolation_level is not None:
        conn.set_session(isolation_level=isolation_level)

    cur = conn.cursor()
    try:
        yield cur, conn
    finally:
        cur.close()
        conn.close()


def lost_update(transactions_number=10000, isolation_level=None):
    with db_session(isolation_level=isolation_level) as (cursor, conn):
        for _ in range(transactions_number):
            cursor.execute("SELECT counter FROM user_counter WHERE user_id = 1")
            counter = cursor.fetchone()[0]

            counter = counter + 1

            cursor.execute(
                "UPDATE user_counter SET counter = %s WHERE user_id = %s",
                (counter, 1)
            )
            conn.commit()


def serializable_safe_update(transactions_number=10000):
    with db_session(isolation_level="SERIALIZABLE") as (cursor, conn):
        for _ in range(transactions_number):
            while True:
                try:
                    cursor.execute(
                        "SELECT counter FROM user_counter WHERE user_id = 1"
                    )
                    counter = cursor.fetchone()[0]

                    counter = counter + 1

                    cursor.execute(
                        "UPDATE user_counter SET counter = %s WHERE user_id = %s",
                        (counter, 1)
                    )
                    conn.commit()
                    break

                except psycopg2.errors.SerializationFailure:
                    conn.rollback()
                    time.sleep(0.001)

def inplace_update(transactions_number=10000):
    with db_session() as (cursor, conn):
        for _ in range(transactions_number):
            cursor.execute(
                "UPDATE user_counter SET counter = counter + 1 WHERE user_id = %s",
                (1,)
            )
            conn.commit()

def row_level_locking(transactions_number=10000):
    with db_session() as (cursor, conn):
        for _ in range(transactions_number):
            cursor.execute("SELECT counter FROM user_counter WHERE user_id = 1 FOR UPDATE")
            counter = cursor.fetchone()[0]
            counter = counter + 1
            cursor.execute("UPDATE user_counter SET counter = %s WHERE user_id = %s", (counter, 1))
            conn.commit()


def optimistic_concurrency_control(transactions_number=10000):
    with db_session() as (cursor, conn):
        for _ in range(transactions_number):
            while True:
                cursor.execute("SELECT counter, version FROM user_counter WHERE user_id = 1")
                counter, version = cursor.fetchone()
                counter = counter + 1
                cursor.execute("update user_counter set counter = %s, version = %s where user_id = %s and version = %s",
                               (counter, version + 1, 1, version))
                conn.commit()
                count = cursor.rowcount
                if count > 0:
                    break



def get_counter():
    with db_session() as (cursor, conn):
        cursor.execute("SELECT counter FROM user_counter WHERE user_id = 1")
        return cursor.fetchone()[0]

def reset_counter():
    with db_session() as (cursor, conn):
        cursor.execute(
            """
             INSERT INTO user_counter (user_id, counter, version)
             VALUES (1, 0, 0)
             ON CONFLICT (user_id)
             DO UPDATE SET
                 counter = EXCLUDED.counter,
                 version = EXCLUDED.version
             """,
        )
        conn.commit()

def run_parallel(test_func, name, threads=10):
    print(f"\n=== {name} ===")

    reset_counter()
    before = get_counter()

    start = time.perf_counter()

    workers = []
    for _ in range(threads):
        t = threading.Thread(target=test_func)
        workers.append(t)
        t.start()

    for t in workers:
        t.join()

    end = time.perf_counter()

    after = get_counter()

    print(f"Time: {end - start:.2f} sec")
    print(f"Counter before: {before}")
    print(f"Counter after:  {after}")
    print(f"Delta:          {after - before}")


if __name__ == "__main__":
    run_parallel(lost_update, "Lost update (READ COMMITTED)")
    try:
        run_parallel(lambda: lost_update(isolation_level="SERIALIZABLE"),
                 "Lost update (SERIALIZABLE, without retry)")
    except psycopg2.errors.SerializationFailure as e:
        print(e)
    run_parallel(serializable_safe_update,
                 "SERIALIZABLE with retry")
    run_parallel(inplace_update,
                 "In-place UPDATE (counter = counter + 1)")
    run_parallel(row_level_locking,
                 "Row-level locking (SELECT FOR UPDATE)")
    run_parallel(optimistic_concurrency_control,
                 "Optimistic concurrency control")


