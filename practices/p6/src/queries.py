import os
from datetime import datetime

from pymongo import MongoClient
import pandas as pd

ps_conn = None
client = None

MONGO_URI = os.environ.get(
    "MONGO_URI",
    "mongodb://admin:q@mongo:27017/?authSource=admin"
)
MONGO_DB_NAME = os.environ.get("MONGO_DB", "market")
COLLECTION_NAME = "candles"

TS_FMT = "%Y-%m-%d %H:%M:%S"

client = MongoClient(MONGO_URI)
db = client[MONGO_DB_NAME]
col = db[COLLECTION_NAME]


def mongo_range_df(
    symbol: str,
    start_ts: datetime,
    end_ts: datetime
) -> pd.DataFrame:
    q = {"symbol": symbol, "ts": {"$gte": start_ts, "$lt": end_ts}}
    cur = col.find(q, projection={"_id": 0, "ts": 1, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}).sort("ts", 1)
    rows = list(cur)
    return pd.DataFrame(rows)

if __name__ == "__main__":
    start = datetime.strptime("2015-02-02 09:15:00", TS_FMT)
    end   = datetime.strptime("2015-02-02 9:30:00", TS_FMT)

    df_mongo = mongo_range_df("ABB", start, end)
    print(df_mongo)

