from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime, timezone
import os
import random

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:q@mongo:27017/?authSource=admin")
client = MongoClient(MONGO_URI)
db = client.messenger

db.users.delete_many({})
db.friendships.delete_many({})
db.messages.delete_many({})

users = [
    {"name": "Аня",   "login": "anya", "email": "anya@mail.com", "createdAt": datetime.now(timezone.utc)},
    {"name": "Борис", "login": "boris", "email": "boris@mail.com", "createdAt": datetime.now(timezone.utc)},
    {"name": "Вера",  "login": "vera", "email": "vera@mail.com",  "createdAt": datetime.now(timezone.utc)},
    {"name": "Глеб",  "login": "gleb", "email": "gleb@mail.com",  "createdAt": datetime.now(timezone.utc)},
    {"name": "Даша",  "login": "dasha", "email": "dasha@mail.com", "createdAt": datetime.now(timezone.utc)},
]

def make_pair_key(a,b):
    s1 = str(a)
    s2 = str(b)
    return "_".join(sorted([s1, s2]))

result = db.users.insert_many(users)
user_ids = result.inserted_ids

friendships = []
for i in range(len(user_ids)):
    friendships.append({
        "firstUserId": user_ids[i],
        "secondUserId": user_ids[(i + 1) % len(user_ids)],
        "pairKey": make_pair_key(user_ids[i], user_ids[(i + 1) % len(user_ids)]),
        "createdAt": datetime.now(timezone.utc)
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
        "createdAt": datetime.now(timezone.utc)
    })

db.messages.insert_many(messages)

