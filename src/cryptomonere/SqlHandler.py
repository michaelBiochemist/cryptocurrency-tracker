#!/usr/bin/env python

import logging
import sqlite3 as sqlite
from itertools import chain
from typing import List

logger = logging.getLogger(__name__)
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


class SqlHandler:
    cx: sqlite.Connection
    table_list = List[str]

    def listQuery(self, query):
        self.cx.row_factory = None
        a = self.cx.execute(query).fetchall()
        return list(chain(*a))

    def sql(self, query, row_factory=None, is_update=False):
        self.cx.row_factory = row_factory
        logger.debug(query)
        temp = self.cx.execute(query)
        if is_update:
            self.cx.commit()
        else:
            return temp.fetchall()

    def __init__(self, config):
        self.cx = sqlite.connect(f"{config['data_dir']}/crypto.db")
        self.table_list = self.listQuery("select name from sqlite_master where type='table'")
        if len(self.table_list) == 0:
            logger.warning("Database is empty and has no schema. Creating tables...")
            self.cx.executescript(create_tables)
            self.table_list = self.listQuery("select name from sqlite_master where type='table'")
