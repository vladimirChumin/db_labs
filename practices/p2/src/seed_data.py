
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime
import os
import random

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:q@mongo:27017/?authSource=admin")
client = MongoClient(MONGO_URI)
db = client.messenger

db.users.delete_many({})
db.friendships.delete_many({})
db.messages.delete_many({})

users = [
    {"name": "Аня",   "login": "anya",   "createdAt": datetime.utcnow()},
    {"name": "Борис", "login": "boris",  "createdAt": datetime.utcnow()},
    {"name": "Вера",  "login": "vera",   "createdAt": datetime.utcnow()},
    {"name": "Глеб",  "login": "gleb",   "createdAt": datetime.utcnow()},
    {"name": "Даша",  "login": "dasha",  "createdAt": datetime.utcnow()},
]

result = db.users.insert_many(users)
user_ids = result.inserted_ids

friendships = []
for i in range(len(user_ids)):
    friendships.append({
        "firstUserId": user_ids[i],
        "secondUserId": user_ids[(i + 1) % len(user_ids)],
        "createdAt": datetime.utcnow()
    })

db.friendships.insert_many(friendships)

texts = [
    "Привет!",
    "Как дела?",
    "Окей",
    "Понял",
    "Спасибо!",
    "Когда созвонимся?",
    "Хорошо",
    "Отлично",
    "Договорились",
    "До связи"
]

messages = []
for _ in range(10):
    from_user, to_user = random.sample(user_ids, 2)
    messages.append({
        "fromUserId": from_user,
        "toUserId": to_user,
        "text": random.choice(texts),
        "createdAt": datetime.utcnow()
    })

db.messages.insert_many(messages)

