import json
from datetime import datetime
from bson import ObjectId
from pymongo import MongoClient
import os

from practices.p5.src.main import MONGO_URI

DB_NAME = "weather"
COLLECTION_NAME = "weather_forecast"
OUTPUTFILE = "practices/p5/data/weather_forecast.json"

def bson_to_json(obj):
    if isinstance(obj, dict):
        return {k: bson_to_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [bson_to_json(v) for v in obj]
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, ObjectId):
        return str(obj)
    return obj

def main():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    docs = list(collection.find())

    json_docs = [bson_to_json(doc) for doc in docs]

    with open(OUTPUTFILE, "w", encoding="utf-8") as f:
        json.dump(json_docs, f, ensure_ascii=False, indent=2)

    print(f"Файл сохранен: {OUTPUTFILE}")

if __name__ == "__main__":
    main()
