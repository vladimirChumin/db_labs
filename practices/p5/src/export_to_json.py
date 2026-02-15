import json
from pymongo import MongoClient
from bson import json_util
import os

def main():
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:q@mongo:27017/?authSource=admin") 
    client = MongoClient(MONGO_URI)
    db = client["weather"]
    collection = db["weather_forecast"]

    docs = list(collection.find())

    with open("practices/p5/data/weather_forecast.json", "w", encoding="utf-8") as f:
        f.write(json_util.dumps(docs, ensure_ascii=False, indent=2))

    print("Файл сохранён")

if __name__ == "__main__":
    main()

