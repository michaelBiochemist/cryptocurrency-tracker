#!/usr/bin/env python

import sqlite3 as sql


cx = sql.connect("data/crypto.db")

create_tables = """
CREATE TABLE historical 
        (
            Symbol varchar(6),
            StartDate Date,
            EndDate Date,
            Open Real,
            High Real,
            Low Real,
            Close Real,
            Volume Real,
            Market_Cap Real
        );
CREATE TABLE currencies
        (
            name varchar(50),
            symbol varchar(6),
            date_added datetime

        );
CREATE TABLE quotes
        (
            id int,
            timestamp datetime,
            name varchar(50),
            symbol varchar(6),
            date_added datetime,
            max_supply Bigint,
            circulating_supply real,
            is_active tinyint,
            infinite_supply tinyint,
            minted_market_cap real,
            cmc_rank smallint,
            is_fiat tinyint,
            self_reported_circulating_supply numeric,
            self_reported_market_cap real,
            last_updated datetime,
            price real,
            volume_24h real,
            volume_change_24h real,
            percent_change_1h real,
            percent_change_24h real,
            percent_change_7d real,
            percent_change_30d real,
            percent_change_60d real,
            percent_change_90d real,
            market_cap real,
            market_cap_dominance real,
            fully_diluted_market_cap real
        );
        """


if len(cx.execute("select name from sqlite_master").fetchall()) == 0:
    print("Database is empty and has no schema. Creating tables...")
    cx.executescript(create_tables)
else:
    a = cx.execute("select sql from sqlite_master where type='table'").fetchall()
    b = "\n".join([a[0][0], a[1][0], a[2][0]])
    if b.strip() != create_tables.strip():
        print(
            "Python-defined schema does not match database schema. Rebuilding database..."
        )
        print(f"Db schema:\n{b.strip()}\nPython-schema:\n{create_tables.strip()}")
