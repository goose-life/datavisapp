import sqlite3
import pandas as pd

from itertools import chain


def get_tables_list(db_name=':memory:'):
    """
    Return a list of all the tables in a database.
    """
    conn = sqlite3.connect(db_name)

    with conn:
        c = conn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table';")
        result = list(chain.from_iterable(c.fetchall()))
        return result