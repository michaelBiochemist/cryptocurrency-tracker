import sqlite3

from .config import get_config
from .schemas import tables

config = get_config()


def get_db() -> sqlite3.Connection:
    return sqlite3.connect(config.sqlite_db_path)


def create_tables() -> None:
    con = get_db()
    cur = con.cursor()
    for table in tables:
        cur.execute(table.ddl())
    con.commit()
    con.close()
