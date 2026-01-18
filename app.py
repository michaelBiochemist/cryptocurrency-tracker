#!/usr/bin/env python

import argparse
import sqlite as sql
import coinmarketcap as ccap


def parse_args():
    parser = argparse.ArgumentParser(description="Cryptocurrency Price Tracker")
    subparsers = parser.add_subparsers(dest="command", required=True)

    parser_load = subparsers.add_parser(
        "load-historic",
        help="Load history file from CSV into database. Expects the format in Coincodex export files",
    )
    parser_load.add_argument(
        "symbol",
        help='Cryptocurrency ticker symbol (e.g. "BTC" for Bitcoin)',
    )
    parser_load.add_argument("filename", help="Name of file")
    parser_get = subparsers.add_parser("get", help="Get latest quotes")

    args = parser.parse_args()
    return args


def fetch_and_insert_latest_quotes():
    data = ccap.fetch_api_json(ccap.quotes_url, "data/quotes_latest.json")
    for key in data["data"].keys():
        for a in data["data"][key]:
            insert_string = f"""
    insert into quotes
    (id, name, symbol, date_added, max_supply, circulating_supply, is_active, infinite_supply, minted_market_cap, cmc_rank, is_fiat, self_reported_circulating_supply, self_reported_market_cap, last_updated, price, volume_24h, volume_change_24h, percent_change_1h, percent_change_24h, percent_change_7d, percent_change_30d, percent_change_60d, percent_change_90d, market_cap, market_cap_dominance, fully_diluted_market_cap)
    VALUES
        ({a['id']}, \"{a['name']}\", \"{a['symbol']}\", \"{a['date_added']}\", {a['max_supply']}, {a['circulating_supply']}, {a['is_active']}, {a['infinite_supply']}, {a['minted_market_cap']}, {a['cmc_rank']}, {a['is_fiat']}, {a['self_reported_circulating_supply']}, {a['self_reported_market_cap']}, \"{a['last_updated']}\", {a['quote']['USD']['price']}, {a['quote']['USD']['volume_24h']}, {a['quote']['USD']['volume_change_24h']}, {a['quote']['USD']['percent_change_1h']}, {a['quote']['USD']['percent_change_24h']}, {a['quote']['USD']['percent_change_7d']}, {a['quote']['USD']['percent_change_30d']}, {a['quote']['USD']['percent_change_60d']}, {a['quote']['USD']['percent_change_90d']}, {a['quote']['USD']['market_cap']}, {a['quote']['USD']['market_cap_dominance']}, {a['quote']['USD']['fully_diluted_market_cap']})\n""".replace(
                "None", "NULL"
            )
            print(insert_string)
            sql.cx.execute(insert_string)
            sql.cx.commit()


def main():
    args = parse_args()
    if args.command == "load-historic":
        load_historic()
    elif args.command == "get":
        fetch_and_insert_latest_quotes()


if __name__ == "__main__":
    main()
