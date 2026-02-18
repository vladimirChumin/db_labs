import os
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
ITER = 3

def timed(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> float:
        start = time.perf_counter()
        func(*args, **kwargs)
        end = time.perf_counter()
        return end - start
    return wrapper

def mean_of(n: int = 3):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            total = 0.0
            for _ in range(n):
                db.messages.delete_many({})
                total += func(*args, **kwargs)
            return total / n
        return wrapper
    return decorator

@mean_of(ITER)
@timed
def insert_values_threads(iter_count: int, workers: int = 7):
    col = db.messages
    def one(i: int):
        msg = test_message.copy()
        msg["createdAt"] = datetime.now(timezone.utc)
        col.insert_one(msg)
        
    with ThreadPoolExecutor(max_workers = workers) as ex:
        futures = [ex.submit(one, i) for i in range(iter_count)] 
        for f in as_completed(futures):
            f.result()


@mean_of(ITER)
@timed
def read_values_threads(ids: list, workers: int = 7):
    col = db.messages
    def one(_id: ObjectId):
        col.find_one({"_id": _id}, projection={"_id": 1})

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(one, id) for id in ids]
        for f in as_completed(futures):
            f.result()

@mean_of(ITER)
@timed
def read_and_insert_values_threads(iter_count: int, workers: int = 7):
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
@mean_of(ITER)
@timed
def update_values_threads(ids: list, workers: int = 7):
    col = db.messages

    def one(_id: ObjectId):
        filter = {
            "_id": _id
        }
        update_val = {"$set": {"text": "Update value"}}
        col.update_one(filter=filter, update=update_val)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(one, _id) for _id in ids]
        for f in as_completed(futures):
            f.result()
