from pymongo import MongoClient, ASCENDING
from pymongo.errors import OperationFailure
import os

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:q@mongo:27017/?authSource=admin")
client = MongoClient(MONGO_URI)
db = client.messenger

def ensure_collection(name: str) -> None:
    if name not in db.list_collection_names():
        db.create_collection(name)

ensure_collection("messages")
db.command({
    "collMod": "messages",
    "validator": {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["fromUserId", "toUserId", "text", "createdAt"],
            "properties": {
                "fromUserId": {"bsonType": "objectId"},
                "toUserId":   {"bsonType": "objectId"},
                "text":       {"bsonType": "string", "minLength": 1},
                "createdAt":  {"bsonType": "date"}
            }
        }
    },
    "validationLevel": "strict",
    "validationAction": "error"
})

ensure_collection("users")
db.command({
    "collMod": "users",
    "validator": {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["name", "login", "email", "createdAt"],
            "properties": {
                "name":      {"bsonType": "string", "minLength": 1},
                "login":     {"bsonType": "string", "minLength": 1},
                "email":     {"bsonType": "string", "minLength": 1},
                "createdAt": {"bsonType": "date"}
            }
        }
    },
    "validationLevel": "strict",
    "validationAction": "error"
})

ensure_collection("friendships")
db.command({
    "collMod": "friendships",
    "validator": {
        "$and": [
            {
                "$jsonSchema": {
                    "bsonType": "object",
                    "required": ["firstUserId", "secondUserId", "createdAt"],
                    "properties": {
                        "firstUserId":  {"bsonType": "objectId"},
                        "secondUserId": {"bsonType": "objectId"},
                        "createdAt":    {"bsonType": "date"}
                    }
                }
            },
            {
                "$expr": {
                    "$ne": ["$firstUserId", "$secondUserId"]
                }
            }
        ]
    },
    "validationLevel": "strict",
    "validationAction": "error"
})

db.friendships.create_index(
    [("firstUserId", ASCENDING), ("secondUserId", ASCENDING)],
    unique=True
)

db.users.create_index(
    [("email", ASCENDING)],
    unique=True,
    sparse=True
)

db.users.create_index(
    [("login", ASCENDING)],
    unique=True,
    sparse=True
)
print("Validators and indexes applied successfully.")

