#!/usr/bin/env python

import sqlite as sql

fname = "data/monero_2025-12-17_2026-01-16.csv"
symbol = "XMR"

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
