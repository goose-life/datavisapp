import sqlite3
from pathlib import Path
from datavisapp.create_db import make_table, get_tables_list


def test_get_tables_list(tmpdir):
    db = str(tmpdir.join('test.db'))

    col_types = [
        ('DataSet', 'str'),
        ('Date', 'str'),
    ]
    make_table('metadata', col_types, db_name=db)
    tables_list = get_tables_list(db)

    assert 'metadata' in tables_list