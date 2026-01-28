#!/usr/bin env python
# import json
from datetime import timedelta
from typing import List, Optional

from pydantic import BaseModel


class RangeRule(BaseModel):
    currency: str
    low: float
    high: float


class VariabilityRule(BaseModel):
    currency: str
    magnitude: float
    duration: str
    duration_parsed: Optional[timedelta] = None


class AlertRules(BaseModel):
    range_rules: List[RangeRule] = []
    variability_rules: List[VariabilityRule] = []
