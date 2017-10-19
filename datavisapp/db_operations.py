import sqlite3
import pandas as pd

from itertools import chain


def get_tables_list(conn):
    """
    Return a list of all the tables in a database.
    """
    with conn:
        c = conn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table';")
        result = list(chain.from_iterable(c.fetchall()))
        # chain.from_iterable() is used to collapse
        # the list of 1-element tuples into a flat list.

        return result
