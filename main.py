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
import conn.db as db


def print_hi(name):
    import os

    # Use a breakpoint in the code line below to debug your script.

    csv = pd.read_csv('jobs.csv')
    print(csv)




#  print(sc.textFile("pepito.txt").first())


# Press the green button in the gutter to run the script.
if __name__ == '__main__':


    db.initialize_data_base(formats=1,data=2,db=3)


# See PyCharm help at https://www.jetbrains.com/help/pycharm/
