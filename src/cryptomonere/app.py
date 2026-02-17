#!/usr/bin/env python

import argparse
import functools
import json
import logging
import logging.config
import sys
from datetime import datetime, timezone

# from . import coinmarketcap as ccap, sqlite as sql
from cryptomonere import coinmarketcap as ccap, report
from cryptomonere.alerts import AlertRules
from cryptomonere.config import get_config
from cryptomonere.SqlHandler import SqlHandler

logger = logging.getLogger("monere")
"""
Log levels:
    ERROR
    WARNING
    INFO
    DEBUG
"""


def parse_args(args_raw):
    parser = argparse.ArgumentParser(description="Cryptocurrency Price Tracker")
    parser.add_argument(
        "-L",
        "--log-level",
        help="set log level. Options are ERROR, WARNING, INFO, and DEBUG",
        default="INFO",
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
    parser_map.set_defaults(func=fetch_map_main)
    parser_map.add_argument("-N", "--no-upload", default=False)

    parser_search = subparsers.add_parser("search", help="Query Cryptocurrency Mappings")
    parser_search.set_defaults(func=query_map)
    parser_search.add_argument("search_query")

    parser_search = subparsers.add_parser("alert", help="Check alerts")
    parser_search.set_defaults(func=alert)

    parser_report = subparsers.add_parser("report", help="Generate (text) Reports")
    subparsers_report = parser_report.add_subparsers(dest="report_type", required=True)
    parser_report_quote_latest = subparsers_report.add_parser("latest", help="View latest quotes")
    parser_report_quote_latest.set_defaults(func=report.quote_latest)

    parser_report_last_at = subparsers_report.add_parser("last_at", help="View most recent date a given currency was at a given price")
    parser_report_last_at.set_defaults(func=report_last_at_wrapper)
    parser_report_last_at.add_argument(
        "symbol",
        help='Cryptocurrency ticker symbol (e.g. "BTC" for Bitcoin; NOT case-sensitive)',
    )
    parser_report_last_at.add_argument(
        "price",
        help="The price you are are cheking for.",
    )

    parser_report_double_half = subparsers_report.add_parser(
        "doubles_and_halves", help="View latest quotes - and last time they were at double/half their current price"
    )
    parser_report_double_half.set_defaults(func=report.all_last_at)

    parser_graph = subparsers.add_parser("graph", help="Generate (graphical) Reports")
    subparsers_graph = parser_graph.add_subparsers(dest="Subcommand", required=True)
    parser_graph_price_history = subparsers_graph.add_parser("price_full", help="Generate graph of complete price history for a specific cryptocurrency.")
    parser_graph_price_history.set_defaults(func=graph_price_history_wrapper)
    parser_graph_price_history.add_argument(
        "symbol",
        help='Cryptocurrency ticker symbol (e.g. "BTC" for Bitcoin; NOT case-sensitive)',
    )
    parser_graph_history_comparison = subparsers_graph.add_parser("comparison", help="Generate graph of comparing prices of two cryptocurrencies.")
    parser_graph_history_comparison.set_defaults(func=graph_price_comparison_wrapper)
    parser_graph_history_comparison.add_argument(
        "symbol",
        help='Cryptocurrency ticker symbol (e.g. "BTC" for Bitcoin; NOT case-sensitive)',
    )
    parser_graph_history_comparison.add_argument(
        "symbol2",
        help="Second Cryptocurrency ticker symbol ",
    )

    args = parser.parse_args(args_raw)
    return args


def depends_graph(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            from cryptomonere import graph  # lazy import

        except ImportError:
            logger.error(
                """
Graph functionality requires optional dependencies.\n
Install with:\n
    pip install cryptomonere[graph]\n
or manually install matplotlib and polars."""
            )
            sys.exit(1)

        return func(graph, *args, **kwargs)

    return wrapper


def load_historic(args: argparse.Namespace):
    fname = args.filename
    symbol = args.symbol
    with open(fname) as R:
        rawfile = R.readlines()

    inserts = []
    for line in rawfile[1:]:
        liney = line.strip().split(",")
        liney_remainder = ",".join(liney[2:])
        inserts.append(f'("{symbol}","{liney[0]}","{liney[1]}",{liney_remainder})')

    SQL = SqlHandler()
    SQL.bulk_insert(
        """
    Insert into historical
    (Symbol, StartDate, EndDate, Open, High, Low, Close, Volume, Market_Cap)
    Values
    """,
        inserts,
    )
    logger.info("file uploaded successfully")


def fetch_map_main(args: argparse.Namespace):
    fetch_map(args.no_upload)


def fetch_map(no_upload=False):
    config = get_config()
    # data = ccap.fetch_api_json(ccap.map_url, f"{config.data_dir}/map.json")
    with open(config.data_dir.joinpath("map.json")) as readJson:
        data = json.load(readJson)
    if no_upload:
        return 0
    SQL = SqlHandler()
    SQL.sql_file("recreate_cryptocurrency_map.sql")

    inserts = []
    for row in data["data"]:
        platform_query = "NULL, NULL, NULL, NULL"
        if row["platform"] is not None:
            platform_query = f"{row['platform']['id']}, \"{row['platform']['name']}\", \"{row['platform']['symbol']}\", \"{row['platform']['slug']}\""
        inserts.append(
            f"""({row['id']}, {row['rank']}, \"{row['name'].strip("\"")}\", \"{row['symbol']}\", \"{row['slug']}\", {row['is_active']}, {row['status']}, \"{row['first_historical_data']}\", \"{row['last_historical_data']}\", {platform_query})""".replace(
                "None", "NULL"
            )
        )
    SQL.bulk_insert(
        """
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
) VALUES """,
        inserts,
    )
    SQL.sql_file("dedupe_currency_map.sql")


def query_map(args: argparse.Namespace):
    SQL = SqlHandler()
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
    config = get_config()
    if len(config.symbols) == 0:
        logging.warn(
            "There is no list of cryptocurrency symbols in your config file, and thus nothing to query. Please update your config file if you want this command to work."
        )
        return 1
    SQL = SqlHandler()
    symbol_list = '","'.join(config.symbols)
    id_list = SQL.listQuery(
        f"""
    Select Id from currency
    where symbol in (\"{symbol_list}\")
    """
    )

    parameters = {"id": ",".join([str(cmc_id) for cmc_id in id_list])}
    timestamp = datetime.now(timezone.utc).isoformat()
    data = ccap.fetch_api_json(ccap.quotes_url, f"{config.data_dir}/quotes_latest.json", parameters=parameters)
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

    report.quote_latest(args)


def alert(args: argparse.Namespace):
    config = get_config()
    ar = AlertRules(config.config_dir.joinpath("alert_rules.json"))
    SQL = SqlHandler()
    lines = SQL.sql(ar.range_rules_to_sql(), row_factory=lambda Cursor, Row: f"{Row[0]:6s} {Row[2]:10.3f} {Row[1]}\n")
    with open(config.config_dir.joinpath("alerts"), "w") as WRITE:
        WRITE.write("".join(lines))
    ar.variability_rules_to_table()

    lines = SQL.sql_file("variability_alerts.sql", row_factory=lambda Cursor, Row: f"{Row[0]:10s}{Row[1][:16]:20s}{Row[2][:10]:14s}{Row[3]:>6.2f}%\n")
    with open(config.config_dir.joinpath("alerts"), "a") as APPEND:
        APPEND.write("".join(lines))


@depends_graph
def graph_price_history_wrapper(graph, args: argparse.Namespace):
    graph.graph_price_history(args.symbol.upper())


@depends_graph
def graph_price_comparison_wrapper(graph, args: argparse.Namespace):
    graph.graph_price_comparison(args.symbol.upper(), args.symbol2.upper())


def report_last_at_wrapper(args: argparse.Namespace):
    report.last_at(args.symbol, args.price)


def init_db():
    SQL = SqlHandler()
    if len(SQL.table_list) == 0:
        logger.warning("Database is empty and has no schema. Creating tables...")
        SQL.cx.executescript(SQL.create_tables)
        SQL.table_list = SQL.listQuery("select name from sqlite_master where type='table'")
    if "currency" not in SQL.table_list:
        logger.info("currency table (table listing all supported cryptocurrencies) does not exist. Creating it... (this may take a minute)")
        fetch_map()  # Calling "monere map"


def main(args_raw):
    # logging.config.dictConfig(config=logging_config)
    args = parse_args(args_raw)
    logging.basicConfig(level=args.log_level)
    init_db()
    args.func(args)


def run():
    main(sys.argv[1:])


if __name__ == "__main__":
    print(sys.argv[1:])
    run()
