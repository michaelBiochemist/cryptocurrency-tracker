#!/usr/bin env python
import json
from datetime import timedelta
from typing import List, Optional

from pydantic import BaseModel


class RangeRule(BaseModel):
    currency: str
    low: float
    high: float

    def from_dict(self, a_dict):
        self.currency = a_dict["currency"]
        self.low = a_dict["low"]
        self.high = a_dict["high"]


class VariabilityRule(BaseModel):
    currency: str
    magnitude: float
    duration: str
    duration_parsed: Optional[timedelta] = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.duration_parsed = self.parse_duration()

    def parse_duration(self):
        a = self.duration.lower().strip("s")
        delta = {"day": 1, "week": 7, "month": 30, "year": 365}
        for dkey in delta.keys():
            b = a.split(dkey)
            if len(b) == 2:
                return timedelta(days=int(b[0]) * delta[dkey])
        # handle error


class AlertRules(BaseModel):
    range_rules: List[RangeRule] = []
    variability_rules: List[VariabilityRule] = []

    def read(self, fname):
        with open(fname, "r") as TEMPREAD:
            input_json = json.load(TEMPREAD)
        if len(input_json["range-rules"]) != 0:
            for rule in input_json["range-rules"]:
                new_rule = RangeRule(currency=rule["currency"], low=rule["low"], high=rule["high"])
                self.range_rules.append(new_rule)
        if len(input_json["variability-rules"]) != 0:
            for rule in input_json["variability-rules"]:
                new_rule = VariabilityRule(currency=rule["currency"], magnitude=rule["magnitude"], duration=rule["duration"])
                self.variability_rules.append(new_rule)


if __name__ == "__main__":
    ar = AlertRules()
    ar.read("alerts_sample.json")
    print(ar)
