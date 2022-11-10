import sqlite3
from sqlite3 import Error


def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()

def initialize_data_base(formats, data, db):
    conn = sqlite3.connect('../test_database')
    c = conn.cursor()

    c.execute('''
              CREATE TABLE IF NOT EXISTS products
              ([product_id] INTEGER PRIMARY KEY, [product_name] TEXT)
              ''')

    c.execute('''
              CREATE TABLE IF NOT EXISTS prices
              ([product_id] INTEGER PRIMARY KEY, [price] INTEGER)
              ''')

    conn.commit()
    c.execute('''select * from products''')
    print('llegue a incializar')