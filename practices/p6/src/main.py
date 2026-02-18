from practices.p6.src.queries import mongo_range_df
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import os

DATA_DIR = os.environ.get("MARKET_OUTPUT_DATA" ,"practices/p6/output")
TS_FMT = "%Y-%m-%d %H:%M:%S"

def main():
    company = "ABB"
    start = datetime.strptime("2015-01-01 09:15:00", TS_FMT)
    end   = datetime.strptime("2016-01-01 9:30:00", TS_FMT)

    df = mongo_range_df(company, start, end)
    df.sort_values("ts", inplace=True)
    df.set_index("ts", inplace=True)

    # df_1m = df.resample("1ME").agg({
    #     "open": "first",
    #     "high": "max",
    #     "low": "min",
    #     "close": "last",
    #     "volume": "sum"
    # }).dropna().copy()
    #
    # stock_fig = px.line(df_1m, x=df_1m.index, y="close", title=f"{company} stock price")
    # stock_fig.write_html(f"{DATA_DIR}/{company}_stock.html")

    df_15m = df.resample("15min").agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }).dropna().copy()

    candle_fig = go.Figure(data=[go.Candlestick(
        x=df_15m.index,
        open=df_15m["open"],
        high=df_15m["high"],
        low=df_15m["low"],
        close=df_15m["close"]
    )])
    candle_fig.write_html(f"{DATA_DIR}/{company}_candle.html")

if __name__ == "__main__":
    main()
