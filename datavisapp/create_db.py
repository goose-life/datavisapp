import json
from collections import OrderedDict

import click
import pandas as pd
import records


def make_table(db_path, table_name, col_types):
    """Create a new table in a SQLite database.

    If the database does not already exist then one will be created.

    Args:
        db_path (str): path to the SQLite database.
        table_name (str): name of the table to be created.
        col_types (List[Tuple[str, str]]): column names and types to be used in the table.

    Returns: None
    """
    columns = ', '.join([f'{col_name} {col_type}' for col_name, col_type in col_types])
    query = f'CREATE TABLE {table_name} ({columns})'
    db = records.Database(f'sqlite:///{db_path}')
    db.query(query)


def append_csv_to_table(db, table_name, csv_path):
    """Append data in CSV file to table in database.

    The CSV header must correspond to field names in the given table.

    Args:
        db (str): path to the SQLite database
        table_name (str): name of the table in which to insert the data
        csv_path (Union[str, Path]): path to the CSV file containing the data to insert

    Returns: None
    """
    df = pd.read_csv(csv_path)
    fields = ', '.join(df.columns)
    placeholders = ', '.join([':' + col for col in df.columns])
    query = f'INSERT INTO {table_name} ({fields}) VALUES({placeholders})'

    with records.Database(f'sqlite:///{db}') as db:
        for record in df.to_dict(orient='records'):
            db.query(query, **record)


def create_db(db, schema_json):
    """Create a database according to schema in JSON format."""
    with open(schema_json) as of:
        schema = json.load(of, object_pairs_hook=OrderedDict)
        # OrderedDict so that tables are created in the order specified,
        # allowing foreign keys to reference previously defined tables

    for table_name, columns in schema.items():
        col_types = columns.items()  # dict -> Iterable[Tuple[str, str]]
        make_table(db, table_name, col_types)


@click.command()
@click.argument('db_path')
@click.argument('schema_json')
def main(db_path, schema_json):
    """Create a database from a schema and populate it with CSV/JSON data.

    The schema is supplied as a JSON file with the following structure:
    {"<table name>": {"<field name>": "<field type> <constraints>"}}
    """
    create_db(db_path, schema_json)
