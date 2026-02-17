#!/usr/bin env python
import json
import logging
from typing import List, Optional

from pydantic import BaseModel

from cryptomonere.SqlHandler import SqlHandler

# from functools import reduce

logger = logging.getLogger(__name__)


class RangeRule(BaseModel):
    currency: str
    low: float
    high: float

    def from_dict(self, a_dict):
        self.currency = a_dict["currency"]
        self.low = a_dict["low"]
        self.high = a_dict["high"]

    def to_sql(self):
        return f'(symbol="{self.currency}" and price not between {self.low} AND {self.high})'


class VariabilityRule(BaseModel):
    currency: Optional[str] = "NULL"
    percent_change: int
    duration: str
    duration_parsed: Optional[float] = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.duration_parsed = self.parse_duration()
        if self.currency == "":
            self.currency = "NULL"

    def parse_duration(self):
        a = self.duration.lower().strip("s")
        delta = {"day": 1, "week": 7, "month": 30, "year": 365}
        for dkey in delta.keys():
            b = a.split(dkey)
            if len(b) == 2:
                # return timedelta(days=int(b[0]) * delta[dkey])
                return float(b[0]) * delta[dkey]
        # handle error

    def to_sql_values(self):
        currency = self.currency
        if self.currency != "NULL":
            currency = f'"{currency}"'
        return f"({currency}, {self.percent_change}, {self.duration_parsed})"


class AlertRules(BaseModel):
    range_rules: List[RangeRule] = []
    variability_rules: List[VariabilityRule] = []

    def __init__(self, fname):
        super().__init__()
        self.read(fname)

    def read(self, fname):
        with open(fname, "r") as TEMPREAD:
            input_json = json.load(TEMPREAD)
        if len(input_json["range-rules"]) != 0:
            for rule in input_json["range-rules"]:
                new_rule = RangeRule(currency=rule["currency"], low=rule["low"], high=rule["high"])
                self.range_rules.append(new_rule)
        if len(input_json["variability-rules"]) != 0:
            for rule in input_json["variability-rules"]:
                if "currency" in rule.keys():
                    new_rule = VariabilityRule(currency=rule["currency"], percent_change=rule["percent_change"], duration=rule["duration"])
                else:
                    new_rule = VariabilityRule(percent_change=rule["percent_change"], duration=rule["duration"])
                self.variability_rules.append(new_rule)

    def range_rules_to_sql(self):
        sql_where = "\n OR ".join([rule.to_sql() for rule in self.range_rules])
        return f"""
SELECT
    symbol,
    name,
    price
FROM quote_latest WHERE {sql_where}
        """

    def variability_rules_to_table(self):
        SQL = SqlHandler()
        SQL.sql_file("table_alert_variability_rule.sql")
        if len(self.variability_rules) == 0:
            logging.debug("The number of parsed varibility rules is 0")
            return 0

        SQL.bulk_insert(
            """
insert into alert_variability_rule
(symbol, percent_change, duration) VALUES
        """,
            [x.to_sql_values() for x in self.variability_rules],
        )


if __name__ == "__main__":
    ar = AlertRules("alerts_sample.json")
    # ar.read("alerts_sample.json")
    print(ar)
    print(ar.range_rules_to_sql())
