import argparse
import os
from datetime import datetime, timezone

from pymongo import MongoClient, ASCENDING
from bson import ObjectId

from practices.p3.src.find_user import find_user_id

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:q@mongo:27017/?authSource=admin")
client = MongoClient(MONGO_URI)
db = client.messenger

def parse_dt(s:str) -> datetime:
    s = s.strip()
    try:
        dt = datetime.fromisoformat(s)
        return dt
    except ValueError:
        raise ValueError("Time is incorrect")

def find_author(user_id):
    author_name = db.users.find({"_id": user_id})
    return [n["name"] for n in author_name][0]

def get_messeges(user_value: str, dt_from: datetime = None, dt_to: datetime = None):
    user_id = find_user_id(db, value=user_value)
    query = {
        "$or": [
            {"fromUserId": user_id},
            {"toUserId": user_id}
        ]
    }

    if dt_from != None and dt_to != None:
        if dt_from.tzinfo is None:
            dt_from = dt_from.replace(tzinfo=timezone.utc)
        if dt_to.tzinfo is None:
            dt_to = dt_to.replace(tzinfo=timezone.utc)
        if dt_from > dt_to:
            raise ValueError("dt_from > dt_to")
        query["createdAt"] = {"$gte": dt_from, "$lte": dt_to}

    
    messages = db.messages.find(query).sort("createdAt", ASCENDING)
    return [f"{find_author(m["fromUserId"])}: {m["text"]}" for m in messages]

def get_friends(user_value: str):
    user_id = find_user_id(db, value=user_value)
    user_friendships = db.friendships.find({
        "$or": [
            {"firstUserId": user_id},
            {"secondUserId": user_id}
        ]
    })

    friend_ids = []
    for l in user_friendships:
        if l["firstUserId"] == user_id:
            friend_ids.append(l["secondUserId"])
        else:
            friend_ids.append(l["firstUserId"])
    
    friends = db.users.find({
        "_id": {"$in": friend_ids}
    }).sort("name", ASCENDING)

    return [f["name"] for f in friends]

def count_friends(user_value: str):
    user_id = find_user_id(db, value=user_value)
    count = db.friendships.count_documents({
        "firstUserId": user_id
    })
    return count
    
    


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--user", required=True)
    parser.add_argument("-fl", action="store_true")
    parser.add_argument("-cf", action="store_true")
    parser.add_argument("-m", action="store_true")
    parser.add_argument("-d", action="store_true")

    args = parser.parse_args()

    if args.fl:
        print(get_friends(args.user))
    if args.cf:
        print(count_friends(args.user))
    if args.m:
        dt_from = None
        dt_to   = None
         
        if args.d:
            dt_from = parse_dt(input("Enter start time in datetime format\n"))
            dt_to = parse_dt( input("Enter end time in datetime format\n"))

        print(get_messeges(args.user, dt_from, dt_to))
        


if __name__ == "__main__":
    main()
