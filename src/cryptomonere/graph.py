#!/usr/bin/env python

import polars as pl
from matplotlib import pyplot as plt

from cryptomonere.SqlHandler import SqlHandler


def graph_price_history(symbol):
    SQL = SqlHandler()
    history = SQL.sql_df(f"Select EndDate,Open,Close,Low,High from historical where symbol like '{symbol}'")
    history = history.with_columns(pl.col("EndDate").str.to_date().cast(pl.Datetime))
    prices = SQL.sql_df(f"Select last_updated,Price from quote where symbol like '{symbol}'")
    prices = prices.with_columns(pl.col("last_updated").cast(pl.Datetime))

    plt.plot(history["EndDate"], history["Close"])
    plt.plot(prices["last_updated"], prices["price"])

    plt.show()
