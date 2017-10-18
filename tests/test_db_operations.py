import pandas as pd
import sqlite3
from pathlib import Path
from datavisapp.db_operations import get_tables_list, append_df_to_table
from datavisapp.create_db import make_table



def test_get_tables_list(tmpdir):
    db = str(tmpdir.join('test.db'))

    col_types = [
        ('DataSet', 'TEXT'),
        ('Date', 'TEXT'),
    ]
    make_table('metadata', col_types, db_name=db)
    tables_list = get_tables_list(db)

    assert 'metadata' in tables_list


def test_append_df_to_table(tmpdir):
    db = str(tmpdir.join('test.db'))

    col_types = [
        ('DataSet', 'TEXT'),
        ('Date', 'TEXT'),
    ]
    make_table('metadata', col_types, db_name=db)
    df = pd.DataFrame({'DataSet': ['01', '02'], 'Date': ['2017-01-01', '2017-01-02']})
    append_df_to_table('metadata', df, db)

    conn = sqlite3.connect(db)

    with conn:
        c = conn.cursor()
        c.execute('SELECT * FROM metadata ORDER BY DataSet DESC LIMIT 1;')
        result = c.fetchone()

    assert result == ('02', '2017-01-02')