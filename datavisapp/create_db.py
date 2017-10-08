import sqlite3


def make_table(db, table_name, col_types):
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    columns = []
    for col_name, col_type in col_types:
        col_str = f'{col_name} {col_type}'
        columns.append(col_str)
    columns = ', '.join(columns)
    query = f'''create table {table_name} ({columns})'''
    cursor.execute(query)
    conn.close()

