#!/usr/bin/env python

import argparse
import json
import logging
import logging.config
import pathlib
import shutil
import sys

from . import coinmarketcap as ccap, sqlite as sql

logger = logging.getLogger("master")
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
    print("file uploaded successfully")


def fetch_and_insert_latest_quotes(args: argparse.Namespace):
    data = ccap.fetch_api_json(ccap.quotes_url, f"{config['data_dir']}/quotes_latest.json")
    for key in data["data"].keys():
        for a in data["data"][key]:
            insert_string = f"""
    insert into quotes
    (id, name, symbol, date_added, max_supply, circulating_supply, is_active, infinite_supply, minted_market_cap, cmc_rank, is_fiat, self_reported_circulating_supply, self_reported_market_cap, last_updated, price, volume_24h, volume_change_24h, percent_change_1h, percent_change_24h, percent_change_7d, percent_change_30d, percent_change_60d, percent_change_90d, market_cap, market_cap_dominance, fully_diluted_market_cap)
    VALUES
        ({a['id']}, \"{a['name']}\", \"{a['symbol']}\", \"{a['date_added']}\", {a['max_supply']}, {a['circulating_supply']}, {a['is_active']}, {a['infinite_supply']}, {a['minted_market_cap']}, {a['cmc_rank']}, {a['is_fiat']}, {a['self_reported_circulating_supply']}, {a['self_reported_market_cap']}, \"{a['last_updated']}\", {a['quote']['USD']['price']}, {a['quote']['USD']['volume_24h']}, {a['quote']['USD']['volume_change_24h']}, {a['quote']['USD']['percent_change_1h']}, {a['quote']['USD']['percent_change_24h']}, {a['quote']['USD']['percent_change_7d']}, {a['quote']['USD']['percent_change_30d']}, {a['quote']['USD']['percent_change_60d']}, {a['quote']['USD']['percent_change_90d']}, {a['quote']['USD']['market_cap']}, {a['quote']['USD']['market_cap_dominance']}, {a['quote']['USD']['fully_diluted_market_cap']})\n""".replace(
                "None", "NULL"
            )
            logger.debug(insert_string)
            sql.cx.execute(insert_string)
            sql.cx.commit()


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
        logger.info(f"Config file has been created at {config_path}.\nPlease open your config file and update your api key.")
        exit()
    with open(config_path) as R:
        config = json.load(R)
    config["data_dir"] = pathlib.Path(config["data_dir"]).expanduser()
    if not config["data_dir"].exists():
        config["data_dir"].mkdir(parents=True)
    ccap.init(config)
    sql.init(config)


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
