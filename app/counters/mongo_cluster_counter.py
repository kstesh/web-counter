from .acounter import ACounter
from pymongo import MongoClient, WriteConcern


class MongoClusterCounter(ACounter):
    def __init__(self, hosts: str, replica_set: str, write_concern: int | str):
        uri = f"mongodb://{hosts}/?replicaSet={replica_set}"
        self.client = MongoClient(uri, maxPoolSize=100, minPoolSize=10)
        self.collection = (
            self.client
            .counter_db
            .get_collection(
                "user_counter",
                write_concern=WriteConcern(w=write_concern)
            )
        )
        self.collection.update_one(
            {"user_id": 1},
            {"$setOnInsert": {"counter": 0}},
            upsert=True
        )

    def inc(self) -> None:
        self.collection.find_one_and_update(
            {"user_id": 1},
            {"$inc": {"counter": 1}}
        )

    def get(self) -> int:
        doc = self.collection.find_one({"user_id": 1}, {"counter": 1})
        return doc["counter"] if doc else 0

    def close(self) -> None:
        self.client.close()
