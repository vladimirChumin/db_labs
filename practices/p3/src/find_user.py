import os
from bson import ObjectId

def find_user_id(db, value: str) -> ObjectId:
    value = value.strip()
    if ObjectId.is_valid(value):
        user = db.users.find_one({"_id": ObjectId(value)})
        if user:
            return user["_id"]

    user = db.users.find_one({"login": value})
    if user:
        return user["_id"]

    user = db.users.find_one({"email": value})
    if user:
        return user["_id"]

    raise ValueError(f"Пользователь не найден: {value}")
