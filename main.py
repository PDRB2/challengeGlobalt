import datetime as datetime
import os
from os import getcwd

import pandas as pd
import pyspark.sql.types
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import time
import sqlite3
from sqlite3 import Error


def print_hi(name):
    import os

    # Use a breakpoint in the code line below to debug your script.

    csv = pd.read_csv('jobs.csv')
    print(csv)


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


#  print(sc.textFile("pepito.txt").first())


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
   # print_hi('PyCharm')
    conn = sqlite3.connect('test_database')
    c = conn.cursor()

    #create_connection(r"C:\..\..\..\challengeGlobalt\pythonsqlite.db")
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
    df = pd.DataFrame(c.fetchall(), columns=['product_name','price'])
    print(df)
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
