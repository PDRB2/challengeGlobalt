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
    conn = sqlite3.connect('../globalChallenge')
    c = conn.cursor()

    c.execute('''
              CREATE TABLE IF NOT EXISTS hired_employees
              ([id] INTEGER PRIMARY KEY --comment 'id of the employee'
              ,[name] STRING    -- comment 'Name and surname of the employee'
              ,[datetime] STRING --comment 'Hire datetime in ISO format'
              ,[department_id] INTEGER --comment 'Id of the department which the employee was hired for'
              ,       [job_id] INTEGER --comment 'Id of the job which the employee was hired for'
              )
              ''')

    c.execute('''
                 CREATE TABLE IF NOT EXISTS departments
                 ([id] INTEGER PRIMARY KEY --comment 'id of the department'
                 ,   [department] STRING --comment 'Name  of the department'
                 )
                 ''')

    c.execute('''
              CREATE TABLE IF NOT EXISTS prices
              ([id] INTEGER PRIMARY KEY --comment 'id of the department'
              , [job] INTEGER --comment 'Name  of the job'
              )
              ''')

    conn.commit()
    c.execute('''select * from prices''')
    print(c.fetchall())
    print('llegue a incializar')