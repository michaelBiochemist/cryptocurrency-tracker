# Cryptocurrency Tracker #

This is designed to track cryptocurrency prices of the user's choosing.
The user will then be able to set alerts when the prices go outside of deviated ranges. (Funcionality to be added)

The user can also upload historical data (as found on https://coincodex.com/) to enable reporting and more sophisticated rules for the alerts.

Install the project with PIP "python -m pip install cryptomonere"

To run:
- type monere get once. It will populate a config directory with relevant files and initialize the database
- go to coinmarketcap.com and get an api key. In the config directory (default is $HOME/.config/cryptomonere) edit the config.json file with the correct api key, and modify the list of symbols to ones that you want to track.
- type monere get again. Now, every time it runs that command, it will grab the latest prices of the currencies you want to track.

For alerts:
- In your config directory edit the alert\_rules.json file. Presently only the range rules are implemented. If the currency's price goes outside of the boundary listed with low and high, "monere get" and "monere alert" will print the current price to the screen.

monere search enables you to look up the supported currencies. Do note that the cryptocurrency symbol is used as a unique key. To enforce uniqueness, higher ranking currencies are chosen, so we do not support any shitcoins with the symbol BTC.

Finally load-historical allows you to load history data from a csv file exported on coincodex to a history table, which will then be used when I implement more sophisticated alerts and reporting.
