#!/usr/bin/env python

import argparse
import json
import logging
import logging.config
import pathlib
import shutil
import sys
from datetime import datetime, timezone

# from . import coinmarketcap as ccap, sqlite as sql
from cryptomonere import coinmarketcap as ccap, sqlite as sql
from cryptomonere.alerts_json import AlertRules
from cryptomonere.SqlHandler import SqlHandler

logger = logging.getLogger("monere")
"""
Log levels:
    ERROR
    WARNING
    INFO
    DEBUG
"""


config_directory = f"{pathlib.Path.home()}/.config/cryptotracker"


def parse_args(args_raw):
    parser = argparse.ArgumentParser(description="Cryptocurrency Price Tracker")
    parser.add_argument(
        "-L",
        "--log-level",
        help="set log level. Options are ERROR, WARNING, INFO, and DEBUG",
        default="INFO",
    )
    parser.add_argument(
        "-c",
        "--config-directory",
        default=f"{pathlib.Path.home()}/.config/cryptotracker",
        help=f"Use a custom directory for relevant config files and api keys. Default is:\n{pathlib.Path.home()}/.config/cryptotracker",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    parser_load = subparsers.add_parser(
        "load-historic",
        help="Load history file from CSV into database. Expects the format in Coincodex export files",
    )
    parser_load.set_defaults(func=load_historic)
    parser_load.add_argument(
        "symbol",
        help='Cryptocurrency ticker symbol (e.g. "BTC" for Bitcoin)',
    )
    parser_load.add_argument("filename", help="Name of file")
    parser_get = subparsers.add_parser("get", help="Get latest quotes")
    parser_get.set_defaults(func=fetch_and_insert_latest_quotes)
    parser_get.add_argument("-N", "--no-upload", default=False)
    parser_get.add_argument("-n", "--no-update-alert", default=False)

    parser_map = subparsers.add_parser("map", help="Get mapping of CoinmarketCap Ids to All listed cryptocurrencies")
    parser_map.set_defaults(func=fetch_map)
    parser_map.add_argument("-N", "--no-upload", default=False)

    parser_search = subparsers.add_parser("search", help="Query Cryptocurrency Mappings")
    parser_search.set_defaults(func=query_map)
    parser_search.add_argument("search_query")

    parser_search = subparsers.add_parser("alert", help="Check alerts")
    parser_search.set_defaults(func=alert)

    args = parser.parse_args(args_raw)
    return args


def load_historic(args: argparse.Namespace):
    fname = args.filename
    symbol = args.symbol
    with open(fname) as R:
        rawfile = R.readlines()

    inserts = ""
    for line in rawfile[1:]:
        liney = line.strip().split(",")
        liney_remainder = ",".join(liney[2:])
        inserts += f'("{symbol}","{liney[0]}","{liney[1]}",{liney_remainder}),'

    sql.cx.execute(
        """
    Insert into historical
    (Symbol, StartDate, EndDate, Open, High, Low, Close, Volume, Market_Cap)
    Values
    """
        + inserts[:-1]
    )
    sql.cx.commit()
    logger.info("file uploaded successfully")


def fetch_map(args: argparse.Namespace):
    data = ccap.fetch_api_json(ccap.map_url, f"{config['data_dir']}/map.json")
    if args.no_upload:
        return 0
    recreate_cryptocurrency_map = """
    Drop table if exists cryptocurrency_map;
    Create table cryptocurrency_map (
    id int unique,
    currency_rank int,
    name varchar(50),
    symbol varchar(10),
    slug varchar(50),
    is_active tinyint,
    status tinyint,
    first_historical_data datetime,
    last_historical_data datetime,
    platform_id int,
    platform_name varchar(50),
    platform_symbol varchar(10),
    platform_slug varchar(50)
    );
    """
    logger.debug(recreate_cryptocurrency_map)
    sql.cx.executescript(recreate_cryptocurrency_map)
    for row in data["data"]:
        platform_query = "NULL, NULL, NULL, NULL"
        if row["platform"] is not None:
            platform_query = f"{row['platform']['id']}, \"{row['platform']['name']}\", \"{row['platform']['symbol']}\", \"{row['platform']['slug']}\""

        insert_string = f"""
insert into cryptocurrency_map (
    id,
    currency_rank,
    name,
    symbol,
    slug,
    is_active,
    status,
    first_historical_data,
    last_historical_data,
    platform_id,
    platform_name,
    platform_symbol,
    platform_slug
) VALUES ({row['id']}, {row['rank']}, \"{row['name'].strip("\"")}\", \"{row['symbol']}\", \"{row['slug']}\", {row['is_active']}, {row['status']}, \"{row['first_historical_data']}\", \"{row['last_historical_data']}\", {platform_query});
        """.replace(
            "None", "NULL"
        )
        logger.debug(insert_string)
        sql.cx.execute(insert_string)
        sql.cx.commit()

        sql.sql_file("dedupe_cryptocurrency_map.sql")


def query_map(args: argparse.Namespace):
    SQL = SqlHandler(config)
    if "cryptocurrency_map" not in SQL.table_list:
        logger.info("Table cryptocurrency_map does not exist. This data will take a minute to fetch and upload...")
        fetch_map(args)
    headers = ["Cap_Id", "Symbol", "Name", "slug", "rank"]
    print(f"{headers[0].ljust(6)} {headers[4].ljust(6)}  {headers[1].ljust(10)}{headers[2].ljust(30)};{headers[3].ljust(30)}")
    SQL.sql(
        f"""
    Select Id, Symbol, Name, slug, currency_rank from currency
    WHERE (symbol like \"%{args.search_query}%\")
        OR (Name like \"%{args.search_query}%\")
        OR (slug like \"%{args.search_query}%\")
    """,
        row_factory=lambda Cursor, Row: print(f"{str(Row[0]).ljust(6)} {str(Row[4]).ljust(6)}  {Row[1].ljust(10)}{Row[2].ljust(30)};{Row[3].ljust(30)}"),
    )


def fetch_and_insert_latest_quotes(args: argparse.Namespace):
    # Set parameters in decreasing order of specificity, from coinmarketcap_id, to slug, to symbol
    if "symbols" not in config.keys():
        logging.warn(
            "There is no list of cryptocurrency symbols in your config file, and thus nothing to query. Please update your config file if you want this command to work."
        )
        return 1
    SQL = SqlHandler(config)
    symbol_list = '","'.join(config["symbols"])
    id_list = SQL.listQuery(
        f"""
    Select Id from currency
    where symbol in (\"{symbol_list}\")
    """
    )

    parameters = {"id": ",".join([str(cmc_id) for cmc_id in id_list])}
    timestamp = datetime.now(timezone.utc).isoformat()
    data = ccap.fetch_api_json(ccap.quotes_url, f"{config['data_dir']}/quotes_latest.json", parameters=parameters)
    if args.no_upload:
        return 0
    insert_string = """
    insert into quote
    (id,
     name,
     symbol,
     timestamp,
     date_added,
     max_supply,
     circulating_supply,
     is_active,
     infinite_supply,
     minted_market_cap,
     cmc_rank,
     is_fiat,
     self_reported_circulating_supply,
     self_reported_market_cap,
     last_updated,
     price,
     volume_24h,
     volume_change_24h,
     percent_change_1h,
     percent_change_24h,
     percent_change_7d,
     percent_change_30d,
     percent_change_60d,
     percent_change_90d,
     market_cap,
     market_cap_dominance,
     fully_diluted_market_cap)
    VALUES
    """

    values_strings = []
    for key in data["data"].keys():
        a = data["data"][key]
        values_strings.append(
            f"""
     ({a['id']},
     \"{a['name']}\",
     \"{a['symbol']}\",
     \"{timestamp}\",
     \"{a['date_added']}\",
     {a['max_supply']},
     {a['circulating_supply']},
     {a['is_active']},
     {a['infinite_supply']},
     {a['minted_market_cap']},
     {a['cmc_rank']},
     {a['is_fiat']},
     {a['self_reported_circulating_supply']},
     {a['self_reported_market_cap']},
     \"{a['last_updated']}\",
     {a['quote']['USD']['price']},
     {a['quote']['USD']['volume_24h']},
     {a['quote']['USD']['volume_change_24h']},
     {a['quote']['USD']['percent_change_1h']},
     {a['quote']['USD']['percent_change_24h']},
     {a['quote']['USD']['percent_change_7d']},
     {a['quote']['USD']['percent_change_30d']},
     {a['quote']['USD']['percent_change_60d']},
     {a['quote']['USD']['percent_change_90d']},
     {a['quote']['USD']['market_cap']},
     {a['quote']['USD']['market_cap_dominance']},
     {a['quote']['USD']['fully_diluted_market_cap']})\n""".replace(
                "None", "NULL"
            )
        )
    SQL.bulk_insert(insert_string, values_strings)
    SQL.sql_file("quote_to_quote_latest.sql")

    if not args.no_update_alert:
        alert(args)


def alert(args: argparse.Namespace):
    ar = AlertRules(pathlib.Path(args.config_directory).expanduser().joinpath("alert_rules.json"))
    SQL = SqlHandler(config)
    SQL.sql(ar.range_rules_to_sql(), row_factory=lambda Cursor, Row: print(f"{Row[0]} {Row[2]} {Row[1]}"))


def init(args: argparse.Namespace):
    global config
    path = pathlib.Path(args.config_directory).expanduser()
    if not path.exists():
        logger.info(f"Path {path}. does not currently exist. Creating it...")
        try:
            path.mkdir(parents=True)
        except Exception as e:
            logger.error(f"Path {path} cannot be created due to an exception:\n{e}\nPlease run again with a usable path (or allow the default to be created)")
            exit()

    config_path = path.joinpath("config.json")
    if not config_path.exists():
        logger.info("Config file does not exist. Creating default config file.")
        shutil.copy(pathlib.Path(__file__).parent.joinpath("config_default.json"), config_path)
        shutil.copy(pathlib.Path(__file__).parent.joinpath("alerts_sample.json"), config_path.parent.joinpath("alert_rules.json"))
        logger.info(
            f"""Config file has been created at {config_path}.\n
        Please open your config file and update your api key.
        Please Also update your alert_rules.json file to get correct alerts"""
        )
        exit()
    with open(config_path) as R:
        config = json.load(R)
    config["data_dir"] = pathlib.Path(config["data_dir"]).expanduser()
    if not config["data_dir"].exists():
        config["data_dir"].mkdir(parents=True)
    ccap.init(config)
    SQL = SqlHandler(config)
    if "currency" not in SQL.table_list:
        logger.info("currency table (table listing all supported cryptocurrencies) does not exist. Creating it... (this may take a minute)")
        fetch_map(args)


def main(args_raw):
    # logging.config.dictConfig(config=logging_config)
    args = parse_args(args_raw)
    logging.basicConfig(level=args.log_level)
    init(args)
    args.func(args)


def run():
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
