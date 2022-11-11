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

def initialize_data_base():
    conn = sqlite3.connect('globalChallenge')
    c = conn.cursor()

    c.execute('''drop table hired_employees ''')
    c.execute('''drop table departments ''')
    c.execute('''drop table jobs ''')

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
              CREATE TABLE IF NOT EXISTS jobs
              ([id] INTEGER PRIMARY KEY --comment 'id of the department'
              , [job] STRING --comment 'Name  of the job'
              )
              ''')

    conn.commit()
    conn.close()


    print('llegue a incializar la base')


def buscarColumnasTabla(csv, tipo_de_tabla):
    if tipo_de_tabla =='jobs':
                    csv.columns = ['id','job']
    elif tipo_de_tabla == 'departments':
                    csv.columns = ['id', 'department']
    else   :
        csv.columns = ['id', 'name', 'datetime', 'department_id', 'job_id']
    return csv


def insertarDatos(csv, tipo_de_tabla):
    conn = sqlite3.connect('../globalChallenge')
    c = conn.cursor()
    csv = buscarColumnasTabla(csv,tipo_de_tabla)
    print(csv)
    csv.to_sql(tipo_de_tabla, conn, if_exists='replace',index=False)
    print('logre insertarme!')

    c.execute('''select * from '''+tipo_de_tabla + '''''')
    print(c.fetchall())
    conn.commit()
    conn.close()

    return None