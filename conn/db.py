import logging
import os
import sqlite3
from sqlite3 import Error

import pandas as pd
import pandavro as pdx

from api import handle

logger2 = logging.getLogger(handle)

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
    logger2.info('llegue a incializar la base')


def buscarColumnasTabla(csv, tipo_de_tabla):
    # se busca directamente los tipos de columnas de esta manera , en caso de cambio se debera moodificar
    if tipo_de_tabla == 'jobs':
        csv.columns = ['id', 'job']
    elif tipo_de_tabla == 'departments':
        csv.columns = ['id', 'department']
    else:
        csv.columns = ['id', 'name', 'datetime', 'department_id', 'job_id']
    return csv


def obtenerMetadatosValidos(tipo_de_tabla):
    # se obtiene hasta encontrar mejor forma de manera ordenada los tipos de datos estos lamentablemente
    # al no tener mas referencia del tipo de longitud , se asocian objetos con strings al no estar limitados

    if tipo_de_tabla == 'jobs':
        metadatos = ['int64', 'object']
    elif tipo_de_tabla == 'departments':
        metadatos = ['int64', 'object']
    else:
        metadatos = ['int64', 'object', 'object', 'int64', 'int64']
    metadatos_validos = pd.DataFrame(metadatos)
    metadatos_validos.columns = ['tipo_dato']
    return metadatos_validos


def son_metadatos_invalidos(csv, tipo_de_tabla):
    # buscamos los metadatos y validamos contra los informados en caso que
    # ya una condicion no cumpla se limita y se reporta el error
    result = csv.dtypes
    metadatos_validos = obtenerMetadatosValidos(tipo_de_tabla)

    i = 0
    es_valido = False

    logger2.info('Metadatos del archivo ' + str(result.iloc[0]))
    logger2.info('Metadatos del schema ' + str(metadatos_validos['tipo_dato'].iloc[0]))
    while i < len(result):
        es_valido = str(result.iloc[i]) == str(metadatos_validos['tipo_dato'].iloc[i])
        if es_valido == False:
            i = len(result)
        i = i + 1

    return es_valido


def generarBackup(conn):
    cursor = conn.cursor()
    TIPO_DE_TABLA = ['jobs', 'departments', 'hired_employees']
    PATH_FINAL = './backups/'

    for i in TIPO_DE_TABLA:
        cursor.execute('''Select * from ''' + i)
        df = pd.DataFrame.from_records(cursor.fetchall(),
                                       columns=[desc[0] for desc in cursor.description])
        pdx.to_avro(PATH_FINAL + i + '.avro', df)
        logger2.info('Backup generado exitosamente para ' + i)

    pass


def existen_los_registros_anteriormente(csv, conn, tipo_de_tabla):
    # en este caso recuperamos los id correspondientes a la tabla tomando la conneccion para
    # verificar la norma de negocio de primary key
    existe = False
    i = 0
    compare_df = pd.read_sql('SELECT DISTINCT(id) FROM ' + tipo_de_tabla + ' where id is not null;', conn)
    while not existe and i < len(csv):
        if csv.iloc[i, 0] in compare_df.values:
            existe = True
        i = i + 1
    return existe


def insertarDatos(csv, tipo_de_tabla):
    # en este metodo vamos a buscar las columnas , validar cantidad de archivos y validar metadatos
    # la coneccion a la base es para abrir y cerrar
    conn = sqlite3.connect('../globalChallenge')
    try:
        csv = buscarColumnasTabla(csv, tipo_de_tabla)
        if len(csv) > 1000:
            logger2.info('Error en la ingesta de ' + tipo_de_tabla + ' se encuentran mas de 1000 registros')
            return 'Error en la ingesta de ' + tipo_de_tabla + ' se encuentran mas de 1000 registros'
        if not (son_metadatos_invalidos(csv, tipo_de_tabla)):
            logger2.info('Error en el lote ingestado de ' + tipo_de_tabla + 'Los metadatos no se corresponden')
            return 'Error en el lote ingestado de ' + tipo_de_tabla + 'Los metadatos no se corresponden'
        if existen_los_registros_anteriormente(csv, conn, tipo_de_tabla):
            logger2.info('Error en los registros insertados existen anteriormente para la tabla ' + tipo_de_tabla)
            return 'Error en los registros insertados existen anteriormente para la tabla ' + tipo_de_tabla

        logger2.info('validaciones completadas')
        csv.set_index('id', inplace=True)
        csv.to_sql(tipo_de_tabla, conn, if_exists='append', index=True)
        generarBackup(conn)
        conn.commit()
        conn.close()
        return 'Logre insertar ' + tipo_de_tabla
    except:
        return 'Error de logicas ' + tipo_de_tabla

def insertardatos_y_bkp(csv, tipo_de_tabla):
    conn = sqlite3.connect('../globalChallenge')
    csv = buscarColumnasTabla(csv, tipo_de_tabla)
    csv.set_index('id', inplace=True)
    csv.to_sql(tipo_de_tabla, conn, if_exists='replace', index=False)
    conn.commit()
    conn.close()
    return None


def restaurarultimo_bkp(conn):
    dir_path = './backups'
    paths = os.listdir(dir_path)
    logger2.info('Se procede a buscar los archivos avro para rearmar la base de 0')
    for i in paths:
            backup = pdx.read_avro(dir_path+ '/' +i, na_dtypes=True)
            sinfilename = (i.split('.',1))
            insertardatos_y_bkp(backup,sinfilename[0])
            logger2.info('Se restauro el backup ' + sinfilename[0])

    pass


def borrar_y_restaurar_bkp():
    #borramos las tablas validamos que no exista nada mas
    initialize_data_base()
    conn = sqlite3.connect('globalChallenge')
    c = conn.cursor()
    restaurarultimo_bkp(conn)
    conn.commit()
    conn.close()
    logger2.info('Se logro reanudar toda la base')
    return None