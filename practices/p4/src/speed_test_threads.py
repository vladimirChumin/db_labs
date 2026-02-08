import os
from datetime import datetime, timezone
import time
import functools
from datetime import datetime, timezone

from pymongo import MongoClient
from bson import ObjectId
from concurrent.futures import ThreadPoolExecutor, as_completed

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
def insert_values_threads(iter_count: int, workers: int = 4):
    col = db.messages
    def one(i: int):
        msg = test_message.copy()
        msg["createdAt"] = datetime.now(timezone.utc)
        db.messages.insert_one(msg)
        
    with ThreadPoolExecutor(max_workers = workers) as ex:
        futures = [ex.submit(one, i) for i in range(iter_count)] 
        for f in as_completed(futures):
            f.result()


@timed(iteration=3)
def read_values_threads(ids: list, workers: int = 4):
    col = db.messages
    def one(_id: ObjectId):
        col.find_one({"_id": _id}, projection={"_id": 1})

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(one, id) for id in ids]
        for f in as_completed(futures):
            f.result()

@timed(iteration=3)
def read_and_insert_values_threads(iter_count: int, workers: int = 4):
    col = db.messages
    def one(i):
        msg = test_message.copy()
        msg["createdAt"] = datetime.now(timezone.utc)
        res = col.insert_one(msg)
        col.find_one(
            {"_id": res.inserted_id},
            projection={"_id": 1}
        )
        
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(one, i) for i in range(iter_count)]
        for f in as_completed(futures):
            f.result()

