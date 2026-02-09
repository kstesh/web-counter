from .acounter import ACounter
from pymongo import MongoClient

class MongoCounter(ACounter):
    def __init__(self, uri: str):
        self.client = MongoClient(
            uri,
            maxPoolSize=100,
            minPoolSize=10
        )
        self.db = self.client.counter_db
        self.collection = self.db.user_counter


    def inc(self):
        self.collection.update_one(
            {"user_id": 1},
            {"$inc": {"counter": 1}},
            upsert=True
        )

    def get(self) -> int:
        doc = self.collection.find_one(
            {"user_id": 1},
            {"counter": 1}
        )
        return doc["counter"] if doc is not None else 0

    def close(self) -> None:
        self.client.close()