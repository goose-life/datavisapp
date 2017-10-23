import sqlite3
from pathlib import Path

import records
from click.testing import CliRunner

from datavisapp.create_db import make_table, append_csv_to_table, main


def test_make_table(tmpdir):
    db = str(tmpdir.join('test.db'))

    col_types = [
        ('DataSet', 'str'),
        ('Date', 'str'),
    ]
    make_table(db, 'metadata', col_types)

    with records.Database(f'sqlite:///{db}') as db:
        existing_tables = db.get_table_names()

    assert existing_tables == ['metadata']


def test_append_csv_to_table(tmpdir):
    db = str(tmpdir.join('test.db'))

    col_types = [
        ('Sample', 'TEXT'),
        ('MetricA', 'REAL'),
        ('MetricB', 'REAL'),
    ]
    make_table(db, 'experiment1_metrics', col_types)
    csv_x = Path('tests', 'data', 'DatasetX.csv')
    csv_y = Path('tests', 'data', 'DatasetY.csv')
    append_csv_to_table(db, 'experiment1_metrics', csv_x)
    append_csv_to_table(db, 'experiment1_metrics', csv_y)

    with records.Database(f'sqlite:///{db}') as db:
        table_result = db.query('SELECT * FROM experiment1_metrics', fetchall=True)

    row_values = [row.values() for row in table_result]
    assert row_values == [
        ('S01', 0.5, 20.0),
        ('S02', 0.9, 45.0),
        ('S01', 0.7, 10.0),
        ('S02', 0.9, 20.0),
    ]


def test_create_db(tmpdir):
    db = str(tmpdir.join('test.db'))
    schema_json = str(Path('datavisapp', 'db_schema.json'))

    runner = CliRunner()
    result = runner.invoke(main, [db, schema_json])
    assert result.exit_code == 0

    with records.Database(f'sqlite:///{db}') as db:
        existing_tables = db.get_table_names()

    assert set(existing_tables).issuperset({'Analyses', 'Samples', 'Metrics'})
