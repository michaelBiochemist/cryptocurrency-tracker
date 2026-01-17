#!/usr/bin/env python


# Documentation at https://coinmarketcap.com/api/documentation/v1/#section/Endpoint-Overview
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json

with open("api_keys.json") as R:
    api_keys = json.load(R)
api_key = api_keys["coinmarketcap"]

test_url = "https://sandbox-api.coinmarketcap.com"
base_url = "https://pro-api.coinmarketcap.com"
quotes_url = f"{base_url}/v2/cryptocurrency/quotes/latest"
ohlcv_url = f"{base_url}/v2/cryptocurrency/ohlcv/latest"
# parameters = {"start": "1", "limit": "5000", "convert": "USD"}
parameters = {"symbol": "BTC,ETH,BCH,XMR,SOL,MINA,ZEC"}  # , "convert": "USD"}
headers = {
    "Accepts": "application/json",
    "X-CMC_PRO_API_KEY": api_key,
}

session = Session()
session.headers.update(headers)


def fetch_api_json(url, output_file):
    try:
        response = session.get(url, params=parameters)
        data = json.loads(response.text)
        print(data)
        with open(output_file, "w") as O:
            json.dump(data, O, indent=2)
        return data
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(e)


data = fetch_api_json(quotes_url, "data/quotes.json")
