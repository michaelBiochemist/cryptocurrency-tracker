#!/usr/bin/env python

import logging

import polars as pl
from matplotlib import pyplot as plt

from cryptomonere.SqlHandler import SqlHandler

logger = logging.getLogger(__name__)


def graph_price_history(symbol):
    SQL = SqlHandler()
    history = SQL.sql_df(f"Select EndDate,Open,Close,Low,High from historical where symbol like '{symbol}' order by EndDate asc")
    if history.height == 0:
        logger.warning(f'There is nothing in the history table for currency "{symbol}", perhaps you might want to load it?')
    else:
        history = history.with_columns(pl.col("EndDate").str.to_date().cast(pl.Datetime))
        plt.plot(history["EndDate"], history["Close"])

    prices = SQL.sql_df(f"Select last_updated,Price from quote where symbol like '{symbol}' order by last_updated asc")
    if prices.height == 0:
        logger.warning(f'There is nothing in the quotes table for currency "{symbol}", you might want to add it to config.json and begin tracking it.')
    else:
        prices = prices.with_columns(pl.col("last_updated").cast(pl.Datetime))
        plt.plot(prices["last_updated"], prices["price"])
    plt.show()


def graph_price_comparison(symbol1, symbol2):
    SQL = SqlHandler()
    price_comparison_sql = f"""
with prices_cte as (
    select
        datey,
        symbol,
        avg(price) as price
    from
        (
            select
                date(last_updated) as datey,
                symbol,
                avg(price) as price
            from quote
            where symbol like '{symbol1}' OR symbol like '{symbol2}'
            group by date(last_updated), symbol
            union all
            select
                enddate as datey,
                symbol,
                close as price
            from historical
            where symbol like '{symbol1}' OR symbol like '{symbol2}'
        ) m
    group by datey, symbol
)

select
    a.datey,
    a.price / b.price as price_ratio
from prices_cte a
inner join prices_cte b on a.datey = b.datey
where a.symbol like '{symbol1}'
    and b.symbol like '{symbol2}'
order by a.datey asc
"""
    comparison = SQL.sql_df(price_comparison_sql)
    if comparison.height == 0:
        logger.warning(
            f'There is nothing to compare for the currencies "{symbol1}" and "{symbol2}", perhaps you misspelled the symbol or have not been tracking it?'
        )
        return 0
    plt.plot(
        comparison["datey"],
        comparison["price_ratio"],
    )
    plt.title(f"Historical Price Comparision {symbol1}/{symbol2}")
    plt.show()
