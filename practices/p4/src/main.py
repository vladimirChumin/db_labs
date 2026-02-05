from practices.p4.src.speed_test import insert_values, read_values, read_and_insert_values, prepare_ids
from practices.p4.src.speed_test_threads import insert_values_threads, read_values_threads, read_and_insert_values_threads

import os
import pandas as pd
from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:q@mongo:27017/?authSource=admin")
client = MongoClient(MONGO_URI)
db = client.speed_test


def run_for_size(n: int, is_threads: bool = False) -> tuple[float, float, float]:
    if is_threads == False:
        db.test_collection.delete_many({})
        ins = insert_values(n)

        db.test_collection.delete_many({})
        ids = prepare_ids(n)
        read = read_values(ids)

        db.test_collection.delete_many({})
        read_ins = read_and_insert_values(n)
    else:
        db.test_collection.delete_many({})
        ins = insert_values_threads(n)

        db.test_collection.delete_many({})
        ids = prepare_ids(n)
        read = read_values_threads(ids)

        db.test_collection.delete_many({})
        read_ins = read_and_insert_values_threads(n)

    return ins, read, read_ins


def main():
    sizes = [100, 1000, 10_000]

    ord_result = pd.DataFrame(columns=["N", "Вставка", "Чтение", "Вставка&Чтение"])
    thrds_result = pd.DataFrame(columns=["N", "Вставка", "Чтение", "Вставка&Чтение"])

    for n in sizes:
        ins_o, read_o, read_ins_o = run_for_size(n, is_threads=False)
        ins_t, read_t, read_ins_t = run_for_size(n, is_threads=True)
        ord_result.loc[len(ord_result)] = [n, ins_o, read_o, read_ins_o]
        thrds_result.loc[len(thrds_result)] = [n, ins_t, read_t, read_ins_t]

    ord_result = ord_result.round(3)
    thrds_result = thrds_result.round(3)
    ord_result.to_csv("practices/p4/reports/ord_report.csv", index=False, encoding="utf-8")
    thrds_result.to_csv("practices/p4/reports/thrds_report.csv", index=False, encoding="utf-8")
    print(ord_result)
    print(thrds_result)


if __name__ == "__main__":
    main()
