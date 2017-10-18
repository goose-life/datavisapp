import sqlite3
from pathlib import Path
from click.testing import CliRunner
from datavisapp.create_db import make_table, append_csv_to_table, main


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


def test_create_db(tmpdir):
     db = str(tmpdir.join('test.db'))
     schema_json = str(Path('tests', 'schema.json'))

     runner = CliRunner()
     result = runner.invoke(main, [db, schema_json])
     assert result.exit_code == 0

     with sqlite3.connect(db) as conn:
         cursor = conn.cursor()
         cursor.execute("select name from sqlite_master where type = 'table'")
         existing_tables = cursor.fetchall()

     assert existing_tables == [
         ('metadata',),
         ('metrics',),
     ]