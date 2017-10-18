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


def append_df_to_table(table_name, df, db_name=':memory:'):
    """
    Append the data from a pandas dataframe to a table in the database.
    """
    conn = sqlite3.connect(db_name)

    with conn:
        df.to_sql(table_name, conn, if_exists='append', index=False)
