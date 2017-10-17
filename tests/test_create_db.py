import sqlite3

from datavisapp.create_db import make_table, append_csv_to_table, get_tables_list


def test_make_table(tmpdir):
    db = str(tmpdir.join('test.db'))

    col_types = [
        ('DataSet', 'TEXT'),
        ('Date', 'TEXT'),
    ]
    make_table('metadata', col_types, db_name=db)

    conn = sqlite3.connect(db)
    c = conn.cursor()

    with conn:
        c.execute("select name from sqlite_master where type = 'table'")
        existing_tables = c.fetchall()

    assert ('metadata',) in existing_tables


def test_append_csv_to_table(tmpdir):
    db = str(tmpdir.join('test.db'))

    col_types = [
        ('Sample', 'TEXT'),
        ('MetricA', 'REAL'),
        ('MetricB', 'REAL'),
    ]
    make_table('experiment1_metrics', col_types, db_name=db)
    append_csv_to_table('experiment1_metrics', 'tests/data/DatasetX.csv', db_name=db)

    conn = sqlite3.connect(db)
    c = conn.cursor()

    with conn:
        c.execute("select * from experiment1_metrics")
        table_result = c.fetchall()

    assert ('S01', 0.5, 20.0) and ('S02', 0.9, 45.0) in table_result


def test_get_tables_list(tmpdir):
    db = str(tmpdir.join('test.db'))

    col_types = [
        ('DataSet', 'str'),
        ('Date', 'str'),
    ]
    make_table('metadata', col_types, db_name=db)
    tables_list = get_tables_list(db)

    assert 'metadata' in tables_list


# def test_insert():
#     conn = sqlite3.connect("test_db")
#     cursor = conn.cursor()
#     cursor.execute('select * from metadata')
#     results = cursor.fetchall()
#     assert results == ""
#     conn.close()
#
