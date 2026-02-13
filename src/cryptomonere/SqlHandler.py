#!/usr/bin/env python

import logging
import pathlib
import sqlite3 as sqlite
from itertools import chain
from typing import List

from cryptomonere.config import get_config

logger = logging.getLogger(__name__)
create_tables = """
CREATE TABLE history
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
CREATE TABLE quote
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


class SqlHandler:
    cx: sqlite.Connection
    table_list = List[str]

    def listQuery(self, query):
        self.cx.row_factory = None
        a = self.cx.execute(query).fetchall()
        return list(chain(*a))

    def sql_df(self, query):
        import polars

        self.cx.row_factory = None
        return polars.read_database(query, self.cx)

    def bulk_insert(self, insert_into, values):
        if logger.getEffectiveLevel() == logging.DEBUG:
            for value in values:
                self.sql(f"{insert_into}\n{value};", is_update=True)

        else:
            self.sql(insert_into + "\n" + ",".join(values) + ";", is_update=True)

    def sql_file(self, filename, row_factory=None):
        self.cx.row_factory = row_factory
        with open(pathlib.Path(__file__).parent.joinpath(f"sql/{filename}")) as SQLFILE:
            query = SQLFILE.read()
        logger.debug(query)
        if row_factory is not None:
            temp = self.cx.execute(query)
            return temp.fetchall()
        self.cx.executescript(query)

    def sql(self, query, row_factory=None, is_update=False):
        self.cx.row_factory = row_factory
        logger.debug(query)
        temp = self.cx.execute(query)
        if is_update:
            self.cx.commit()
        else:
            return temp.fetchall()

    def __init__(self):
        config = get_config()
        self.cx = sqlite.connect(f"{config.data_dir}/crypto.db")
        self.table_list = self.listQuery("select name from sqlite_master where type='table'")
