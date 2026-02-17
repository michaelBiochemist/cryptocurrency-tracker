#!/usr/bin/env python

import logging

from cryptomonere.SqlHandler import SqlHandler

logger = logging.getLogger(__name__)


def quote_latest(*args):
    SQL = SqlHandler()
    header_space = {"Time": 16, "SYMB": 7, "Name": 20, "price": 8, "%24h": 9, "%7d": 7, "%30d": 7, "%60d": 7, "%90d": 7, "%V24h": 10}
    header = ""
    for key in header_space.keys():
        header += key.ljust(header_space[key])
    print(header)
    SQL.sql_file(
        "quote_latest_report.sql",
        row_factory=lambda Cursor, Row: print(
            f"{Row[0][:16]} {Row[1]:6s}{Row[2]:16s}{Row[3]:>10.4f} {Row[4]:>5.2f} {Row[5]:7.2f} {Row[6]:7.2f} {Row[7]:7.2f} {Row[8]:7.2f} {Row[9] * 100:6.1f}"
        ),
    )


def last_at(symbol, price):
    last_at_sql = f"""
with price_progression_cte as (
select
    last_updated,
    price,
    lead(price) over (win) price_next,
    lead(last_updated) over (win) updated_next
from quotes
where symbol like '{symbol}'
window win as (order by last_updated asc)
order by last_updated desc
), next_cte as (
SELECT date(last_updated) datey from price_progression_cte
where {price} between min(price,price_next) and max(price,price_next)
UNION
SELECT EndDate from historical
where symbol like '{symbol}' and {price} between low and high
)
select max(datey) as datey from next_cte"""
    results = SqlHandler().listQuery(last_at_sql)
    if len(results) == 0:
        print(f"Currency {symbol} was never at this price")
    else:
        print(f"{results[0]} was the last date for the price {price}")


def all_last_at(*args):
    print("Symbol  Name              price  last_double   last_half  last_updated")
    SqlHandler().sql_file(
        "quote_latest_last_at_report.sql",
        lambda Cursor, Row: print(f"{Row[1]:6s}{Row[0]:16s}{Row[2]:>9.3f}   {Row[4]:^10s}   {Row[5]:^10s}   {Row[3][:16]:16s}"),
    )
