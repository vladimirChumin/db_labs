import os
import re
import csv
from io import StringIO
from pathlib import Path
from datetime import datetime

from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError

TS_FMT = "%Y-%m-%d %H:%M:%S"
MONGO_COLLECTION = "candles"


def symbol_from_filename(path: Path) -> str:
    return re.sub(r"_minute$", "", path.stem)

def mongo_ensure_index(mongo_db):
    mongo_db[MONGO_COLLECTION].create_index([("symbol", 1), ("ts", 1)], unique=True)


def load_file_mongo(mongo_col, csv_path: Path, symbol: str, batch_size: int = 10_000) -> int:
    ops = []
    total = 0

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            ts = datetime.strptime(r["date"], TS_FMT)

            doc = {
                "symbol": symbol,
                "ts": ts,
                "open": float(r["open"]) if r.get("open") else None,
                "high": float(r["high"]) if r.get("high") else None,
                "low": float(r["low"]) if r.get("low") else None,
                "close": float(r["close"]) if r.get("close") else None,
                "volume": int(float(r["volume"])) if r.get("volume") else None,
            }
            ops.append(InsertOne(doc))
            total += 1

            if len(ops) >= batch_size:
                try:
                    mongo_col.bulk_write(ops, ordered=False)
                except BulkWriteError:
                    pass
                ops = []

        if ops:
            try:
                mongo_col.bulk_write(ops, ordered=False)
            except BulkWriteError:
                pass

    return total


def main():
    mongo_uri = os.environ.get("MONGO_URI", "mongodb://admin:q@mongo:27017/?authSource=admin")
    mongo_dbname = os.environ.get("MONGO_DB_MARKET", "market")

    data_dir = Path(os.environ.get("MARKET_DATA_DIR", "practices/p6/data"))


    files = sorted(data_dir.glob("*_minute.csv"))
    if not files:
        raise SystemExit(f"No *_minute.csv found in {data_dir}")


    mongo_client = None
    mongo_col = None
    mongo_client = MongoClient(mongo_uri)
    mongo_db = mongo_client[mongo_dbname]
    mongo_ensure_index(mongo_db)
    mongo_col = mongo_db[MONGO_COLLECTION]

    for p in files:
        symbol = symbol_from_filename(p)
        print(f"==> {p.name} (symbol={symbol})")

        if mongo_col is not None:
            n = load_file_mongo(mongo_col, p, symbol)
            print(f"   Mongo loaded:    {n}")

    if mongo_client is not None:
        mongo_client.close()

    print("Done.")


if __name__ == "__main__":
    main()
