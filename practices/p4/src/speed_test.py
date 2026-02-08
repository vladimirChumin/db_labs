import os
from datetime import datetime, timezone
import time
import functools
from bson import ObjectId
from datetime import datetime, timezone

from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:q@mongo:27017/?authSource=admin")
client = MongoClient(MONGO_URI)
db = client.messenger

test_message =   {
    "fromUserId": ObjectId('69883308680b633adf8ffa34'),
    "toUserId": ObjectId('69883308680b633adf8ffa36'),
    "text": 'Спасибо!',
    "createdAt": datetime.now(timezone.utc)
  }


def timed(iteration=3):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start = time.perf_counter()
            for _ in range(iteration):
                func(*args, **kwargs)
            end = time.perf_counter()
            return (end - start) / iteration
        return wrapper
    return decorator

@timed(iteration=3)
def insert_values(iter_count: int):
    for i in range(iter_count):
        msg = test_message.copy()
        msg["createdAt"] = datetime.now(timezone.utc)
        db.messages.insert_one(msg)

def prepare_ids(iter_count: int) -> list:
    ids = []
    for i in range(iter_count):
        msg = test_message.copy()
        msg["createdAt"] = datetime.now(timezone.utc)
        res = db.messages.insert_one(msg)
        ids.append(res.inserted_id)
    return ids

@timed(iteration=3)
def read_values(ids: list):
    for _id in ids:
        db.messages.find_one({"_id": _id}, projection={"_id": 1})

@timed(iteration=3)
def read_and_insert_values(iter_count: int):
    for i in range(iter_count):
        msg = test_message.copy()
        msg["createdAt"] = datetime.now(timezone.utc)
        res = db.messages.insert_one(msg)
        db.messages.find_one(
            {"_id": res.inserted_id},
            projection={"_id": 1}
        )
