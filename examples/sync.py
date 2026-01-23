import argparse
from datetime import datetime
import logging

from .db import get_db,
from .schemas import Quote

logger = logging.getLogger(__name__)
logger.warning("Hey look, a log message from the sync.py")


def runner(args: argparse.Namespace) -> None:
    logger.warning("Hey look, a log message from the sync.py INSIDE the runner function.")
    logger.warning(f"{args.some_sync_arg}")
    get_quotes()


def get_quotes() -> None:
    quotes: list[Quote] = []
    for key in ccap.data["data"].keys():
        for a in ccap.data["data"][key]:
            quote = Quote(
                id=a['id'],
                timestamp=datetime.now(),  # will want to update DDL to use default from DB
                name=a['name'],
                symbol=a['symbol'],
                date_added=a['date_added'],
                max_supply=a['max_supply'],
                circulating_supply=a['circulating_supply'],
                is_active=a['is_active'],
                infinite_supply=a['infinite_supply'],
                minted_market_cap=a['minted_market_cap'],
                cmc_rank=a['cmc_rank'],
                is_fiat=a['is_fiat'],
                self_reported_circulating_supply=a['self_reported_circulating_supply'],
                self_reported_market_cap=a['self_reported_market_cap'],
                last_updated=a['last_updated'],
                price=a['quote']['USD']['price'],
                volume_24h=a['quote']['USD']['volume_24h'],
                volume_change_24h=a['quote']['USD']['volume_change_24h'],
                percent_change_1h=a['quote']['USD']['percent_change_1h'],
                percent_change_24h=a['quote']['USD']['percent_change_24h'],
                percent_change_7d=a['quote']['USD']['percent_change_7d'],
                percent_change_30d=a['quote']['USD']['percent_change_30d'],
                percent_change_60d=a['quote']['USD']['percent_change_60d'],
                percent_change_90d=a['quote']['USD']['percent_change_90d'],
                market_cap=a['quote']['USD']['market_cap'],
                market_cap_dominance=a['quote']['USD']['market_cap_dominance'],
                fully_diluted_market_cap=a['quote']['USD']['fully_diluted_market_cap'],
            )
            quotes.append(quote)
    logger.info(f"A total of {len(quotes)} records have been retrieved")

    con = get_db()
    cur = con.cursor()
    # programmatically extract the column names from the Quote class, extract values dynamically
    data = [
        ("Monty Python Live at the Hollywood Bowl", 1982, 7.9),
        ("Monty Python's The Meaning of Life", 1983, 7.5),
        ("Monty Python's Life of Brian", 1979, 8.0),
    ]
    cur.executemany("INSERT INTO quote VALUES(?, ?, ?)", data)
    con.commit()


# Don't put an "if __name__ == '__main__'" block in every file. It literally does nothing and would never be used/useable.
# You only need it in main.py, because that is __main__, by definition.
