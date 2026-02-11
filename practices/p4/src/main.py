from multiprocessing.pool import worker

from practices.p4.src.speed_test import insert_values, read_values, read_and_insert_values, prepare_ids, update_values
from practices.p4.src.speed_test_threads import insert_values_threads, read_values_threads, read_and_insert_values_threads, update_values_threads

import os
import pandas as pd
from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:q@mongo:27017/?authSource=admin")
client = MongoClient(MONGO_URI)
db = client.messenger

WORKERS = 8

def run_for_size(n: int, is_threads: bool = False) -> tuple[float, float, float, float]:
    if not is_threads:
        ins = insert_values(n)

        ids = prepare_ids(n)
        read = read_values(ids)

        read_ins = read_and_insert_values(n)

        ids = prepare_ids(n)
        update = update_values(ids)
    else:
        ins = insert_values_threads(n, workers=WORKERS)

        ids = prepare_ids(n)
        read = read_values_threads(ids, workers=WORKERS)

        read_ins = read_and_insert_values_threads(n, workers=WORKERS)

        ids = prepare_ids(n)
        update = update_values_threads(ids, workers=WORKERS)

    return ins, read, read_ins, update


def main():
    sizes = [100, 1000, 10_000]

    ord_result = pd.DataFrame(columns=["N", "Вставка", "Чтение", "Изменение","Вставка&Чтение"])
    thrds_result = pd.DataFrame(columns=["N", "Вставка", "Чтение", "Изменение", "Вставка&Чтение"])

    for n in sizes:
        ins_o, read_o, read_ins_o, update_o = run_for_size(n, is_threads=False)
        ins_t, read_t, read_ins_t, update_t = run_for_size(n, is_threads=True)

        ord_result.loc[len(ord_result)] = [n, ins_o, read_o, update_o, read_ins_o]
        thrds_result.loc[len(thrds_result)] = [n, ins_t, read_t, update_t, read_ins_t]

    db.messages.delete_many({})
    ord_result = ord_result.round(3)
    thrds_result = thrds_result.round(3)
    ord_result.to_csv("practices/p4/reports/ord_report.csv", index=False, encoding="utf-8")
    thrds_result.to_csv("practices/p4/reports/thrds_report.csv", index=False, encoding="utf-8")
    print(ord_result)
    print(thrds_result)


if __name__ == "__main__":
    main()
