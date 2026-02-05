import os
from datetime import datetime, timezone
import time
import functools

from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:q@mongo:27017/?authSource=admin")
client = MongoClient(MONGO_URI)
db = client.speed_test

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
        db.test_collection.insert_one({
            "text": "test value",
            "count": i
        })

def prepare_ids(iter_count: int) -> list:
    ids = []
    for i in range(iter_count):
        res = db.test_collection.insert_one({"text": "test value", "count": i})
        ids.append(res.inserted_id)
    return ids

@timed(iteration=3)
def read_values(ids: list):
    for _id in ids:
        db.test_collection.find_one({"_id": _id}, projection={"_id": 1})

@timed(iteration=3)
def read_and_insert_values(iter_count: int):
    for i in range(iter_count):
        res = db.test_collection.insert_one({
            "text": "test value",
            "count": i
        })
        db.test_collection.find_one(
            {"_id": res.inserted_id},
            projection={"_id": 1}
        )
