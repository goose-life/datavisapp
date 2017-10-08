import sqlite3

from datavisapp.create_db import make_table


def test_make_table(tmpdir):
    db = str(tmpdir.join('test.db'))

    col_types = [
        ('DataSet', 'str'),
        ('Date', 'str'),
    ]
    make_table(db, 'metadata', col_types)

    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    cursor.execute("select name from sqlite_master where type = 'table'")
    existing_tables = cursor.fetchall()
    conn.close()
    assert ('metadata',) in existing_tables

# def test_insert():
#     conn = sqlite3.connect("test_db")
#     cursor = conn.cursor()
#     cursor.execute('select * from metadata')
#     results = cursor.fetchall()
#     assert results == ""
#     conn.close()
#
