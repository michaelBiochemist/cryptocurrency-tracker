#!/usr/bin/env python


import json
import logging

# Documentation at https://coinmarketcap.com/api/documentation/v1/#section/Endpoint-Overview
from requests import Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects

logger = logging.getLogger(__name__)

test_url = "https://sandbox-api.coinmarketcap.com"
base_url = "https://pro-api.coinmarketcap.com"
quotes_url = f"{base_url}/v2/cryptocurrency/quotes/latest"
map_url = f"{base_url}/v1/cryptocurrency/map"
ohlcv_url = f"{base_url}/v2/cryptocurrency/ohlcv/latest"
# parameters = {"start": "1", "limit": "5000", "convert": "USD"}


def init(config):
    global session

    api_key = config["api_keys"]["coinmarketcap"]
    ",".join(config["symbols"])

    headers = {
        "Accepts": "application/json",
        "X-CMC_PRO_API_KEY": api_key,
    }
    session = Session()
    session.headers.update(headers)
    # parameters = {"slug": ",".join(config["names"]).lower().strip()}


def fetch_api_json(url, output_file, parameters={}):
    try:
        response = session.get(url, params=parameters)
        data = json.loads(response.text)
        # print(data)
        with open(output_file, "w") as OutFile:
            json.dump(data, OutFile, indent=2)
        if data["status"]["error_code"] == 0:
            return data
        else:
            logger.error(f"Querying the url: {url} failed with the following error:\n\"{data['status']['error_message']}\"")
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(e)
