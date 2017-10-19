import json
import sqlite3
import click as click
import pandas as pd

from collections import OrderedDict


def make_table(table_name, col_types, conn):
    """
    Create a new table in a database using column names and types supplied.
    """
    columns = []
    for col_name, col_type in col_types:
        col_str = f'{col_name} {col_type}'
        columns.append(col_str)
    columns = ', '.join(columns)
    query = f'''create table {table_name} ({columns})'''
    click.echo(f'Creating table {table_name} with SQL command:\n{query}', err=True)

    with conn:
        c = conn.cursor()
        c.execute(query)


def append_csv_to_table(table_name, csv_path, conn):
    """
    Append the data from a csv file to a table in the database.
    """
    with conn:
        df = pd.read_csv(csv_path)
        df.to_sql(table_name, conn, if_exists='append', index=False)


def create_db(conn, schema_json):
    """Create a database according to schema in JSON format."""
    with open(schema_json) as of:
        schema = json.load(of, object_pairs_hook=OrderedDict)
        # OrderedDict so that tables are created in the order specified,
        # allowing foreign keys to reference previously defined tables

    for table_name, columns in schema.items():
        col_types = columns.items()  # dict -> tuple
        make_table(table_name, col_types, conn)


@click.command()
@click.argument('db_path')
@click.argument('schema_json')
def main(db_path, schema_json):
    """Create a database from a schema and populate it with CSV/JSON data.

    The schema is supplied as a JSON file with the following structure:
    {"<table name>": {"<field name>": "<field type> <constraints>"}}
    """
    conn = sqlite3.connect(db_path)
    create_db(conn, schema_json)
