from pathlib import Path

import pandas as pd
import records
from click.testing import CliRunner
from datavisapp.create_db import make_table, insert_into_db, create_db, main


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


def test_append_json_mapping_to_table(tmpdir):
    col_types = [
        ('AnalysisID', 'TEXT'),
        ('AnalysisDate', 'TEXT'),
    ]
    db = str(tmpdir.join('test.db'))
    make_table(db, 'Analyses', col_types)
    data = dict(AnalysisID='DatasetX', AnalysisDate='2017-08-20')
    insert_into_db(db, 'Analyses', [data])
    data = dict(AnalysisID='DatasetY', AnalysisDate='2017-09-25')
    insert_into_db(db, 'Analyses', [data])

    with records.Database(f'sqlite:///{db}') as db:
        analysis_records = db.query('SELECT * FROM Analyses', fetchall=True)

    analyses = [row.values() for row in analysis_records]
    assert analyses == [
        ('DatasetX', '2017-08-20'),
        ('DatasetY', '2017-09-25'),
    ]


def test_append_json_array_to_table(tmpdir):
    col_types = [
        ('SampleName', 'TEXT'),
        ('SampleType', 'TEXT'),
    ]
    db = str(tmpdir.join('test.db'))
    make_table(db, 'Samples', col_types)
    data = [
        dict(SampleName='S01', SampleType='Reference'),
        dict(SampleName='S02', SampleType='Test'),
    ]
    insert_into_db(db, 'Samples', data)
    data = [
        dict(SampleName='S01', SampleType='Test'),
        dict(SampleName='S02', SampleType='Reference'),
    ]
    insert_into_db(db, 'Samples', data)

    with records.Database(f'sqlite:///{db}') as db:
        sample_records = db.query('SELECT * FROM Samples', fetchall=True)

    samples = [row.values() for row in sample_records]
    assert samples == [
        ('S01', 'Reference'),
        ('S02', 'Test'),
        ('S01', 'Test'),
        ('S02', 'Reference'),
    ]


def test_append_csv_to_table(tmpdir):
    db = str(tmpdir.join('test.db'))

    col_types = [
        ('Sample', 'TEXT'),
        ('Metric1', 'REAL'),
        ('Metric2', 'REAL'),
        ('Metric3', 'REAL'),
    ]
    make_table(db, 'experiment1_metrics', col_types)
    csv_x = Path('tests', 'data', 'DatasetX.csv')
    csv_y = Path('tests', 'data', 'DatasetY.csv')  # test data with fields in a different order
    csv_z = Path('tests', 'data', 'DatasetZ.csv')  # test data which lacks one field
    data_x = pd.read_csv(csv_x)
    data_y = pd.read_csv(csv_y)
    data_z = pd.read_csv(csv_z)
    insert_into_db(db, 'experiment1_metrics', data_x.to_dict('records'))
    insert_into_db(db, 'experiment1_metrics', data_y.to_dict('records'))
    insert_into_db(db, 'experiment1_metrics', data_z.to_dict('records'))

    with records.Database(f'sqlite:///{db}') as db:
        table_result = db.query('SELECT * FROM experiment1_metrics', fetchall=True)

    row_values = [row.values() for row in table_result]
    assert row_values == [
        ('S01', 0.5, 20.0, 8.7),
        ('S02', 0.9, 45.0, 10.9),
        ('S01', 0.7, 10.0, 6.5),
        ('S02', 0.9, 20.0, 7.2),
        ('S01', 1, 0.4, None),
        ('S02', 2, 0.8, None),
    ]


def test_create_db(tmpdir):
    db_path = str(tmpdir.join('test.db'))
    schema_json = str(Path('tests', 'data', 'db_schema.json'))
    create_db(db_path, schema_json)

    with records.Database(f'sqlite:///{db_path}') as db:
        existing_tables = db.get_table_names()

    assert set(existing_tables).issuperset({'Analyses', 'Samples', 'Metrics'})
