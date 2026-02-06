import argparse
import os
from datetime import datetime, timezone

from pymongo import MongoClient

from practices.p3.src.find_user import find_user_id

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:q@mongo:27017/?authSource=admin")
client = MongoClient(MONGO_URI)
db = client.messenger


def add_user(user_name: str, user_login: str, user_mail: str):
    if user_name == "" or user_login == "" or user_mail == "":
        raise ValueError("User name, login or mail is empty")
    db.users.insert_one(
        {
            "name": user_name,
            "login": user_login,
            "email": user_mail,
            "createdAt": datetime.now(timezone.utc),
        },
    )


def add_message(from_user: str, to_user: str, text: str):
    if from_user == to_user:
        raise ValueError("First and second user is same")
    from_user_id = find_user_id(db, value=from_user)
    to_user_id = find_user_id(db, value=to_user)

    db.messages.insert_one(
        {
            "fromUserId": from_user_id,
            "toUserId": to_user_id,
            "text": text,
            "createdAt": datetime.now(timezone.utc),
        }
    )


def add_friendship(first_user: str, second_user: str):
    if first_user == second_user:
        raise ValueError("First and second user is same")
    frst_user_id = find_user_id(db, value=first_user)
    scnd_user_id = find_user_id(db, value=second_user)
    db.friendships.insert_one(
        {
            "firstUserId": frst_user_id,
            "secondUserId": scnd_user_id,
            "createdAt": datetime.now(timezone.utc),
        }
    )


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("-u", "--new-user", action="store_true")
    parser.add_argument("-f", "--new-friendship", action="store_true")
    parser.add_argument("-m", "--new-message", action="store_true")

    args = parser.parse_args()

    if not (args.new_user or args.new_friendship or args.new_message):
        parser.error("Chose action: --new-user / --new-friendship / --new-message")

    if sum([args.new_user, args.new_friendship, args.new_message]) > 1:
        parser.error("You can select only one action per launch.")

    if args.new_user:
        name = input("Enter name of new user: ")
        login = input("Enter login of new user: ")
        mail = input("Enter email of new user: ").strip() or None
        add_user(name, login, mail)

    elif args.new_friendship:
        first_user = input("Enter first user: ")
        second_user = input("Enter second user: ")
        add_friendship(first_user, second_user)

    elif args.new_message:
        author = input("Enter author of message: ")
        recipient = input("Enter recipient of message: ")
        text = input("Enter message text: ")
        add_message(author, recipient, text)


if __name__ == "__main__":
    main()
