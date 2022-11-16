import logging
import os
import sqlite3
from sqlite3 import Error

import pandas as pd
import pandavro as pdx

from api import handle

logger2 = logging.getLogger(handle)
CUATRIMESTRES = ['1 and 3', '4 and 6', '7 and 9', '10 and 12']
CONVERT_DICT_METRICAS_1 = {'department': object,
                           'job': object,
                           'Q1': int,
                           'Q2': int,
                           'Q3': int,
                           'Q4': int
                           }
CONVERT_DICT_METRICAS_2 = {'department_id': int,
                           'department': object,
                           'hired': int
                           }
CONSULTAS_METRICAS = [
'''select department, job, cantidad  , mes as mes from (
        Select department , jo.job ,count(hired.id) as cantidad , cast(substr( hired.datetime, 6, 2 ) as int) as mes 
           from
        hired_employees hired inner join
        jobs jo on hired.job_id = jo.id
        inner join departments dep
        on dep.id =hired.department_id  
         where substr( hired.datetime, 1, 4 ) = '2021'
         group by department,jo.job , cast(substr( hired.datetime, 6, 2 ) as int)  
          ) b
       ''' , '''select dep.id as department_id, dep.department as department , count(hired.id)  as cantidad ,(select Avg(hired.id)  from
          hired_employees hired  where substr( hired.datetime, 1, 4 ) = '2021'  )as cantidad_media
        from departments dep
        inner join hired_employees hired on  dep.id =hired.department_id  
        where substr( hired.datetime, 1, 4 ) = '2021' 
        group by dep.id , dep.department
        '''
]

def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        logger2.info(sqlite3.version)
    except Error as e:
        logger2.info(e)
    finally:
        if conn:
            conn.close()


def initialize_data_base():
    conn = getConn()
    c = conn.cursor()

    #c.execute("delete from hired_employees ")
    c.execute('''DROP TABLE IF EXISTS  hired_employees ''')
    c.execute('''DROP TABLE IF EXISTS  departments ''')
    c.execute('''DROP TABLE IF EXISTS jobs ''')
    c.execute('''DROP TABLE IF EXISTS metricas1 ''')
    c.execute('''DROP TABLE IF EXISTS metricas2 ''')
    conn.commit()
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
    c.execute('''
                 CREATE TABLE IF NOT EXISTS metricas1
                 ([id] INTEGER PRIMARY KEY --comment 'id of the department'
                   ,   [department] STRING --comment 'Name  of the department'
                    , [job] STRING --comment 'Name  of the job'
                    ,[cantidad] INTEGER --comment 'month of the metric'
                    ,[mes] INTEGER --comment 'month of the metric'
                    
              )
                 ''')
    c.execute('''
                    CREATE TABLE IF NOT EXISTS metricas2
                    ([id] INTEGER PRIMARY KEY --comment 'id of the department'
                       , [department_id] integer 
                       , [department] STRING --comment 'Name  of the department'
                       , [cantidad] INTEGER --comment 'Name  of the job'
                       ,[cantidad_media] INTEGER --comment 'month of the metric'
                    
                 )
                    ''')
    c.close()
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
    cursor = conn.cursor()
    cursor.execute('''SELECT DISTINCT id FROM ''' + tipo_de_tabla )
    compare_df = pd.DataFrame.from_records(cursor.fetchall(),columns=[desc[0] for desc in cursor.description])
    if csv.iloc[:, 0].isin(compare_df.iloc[:,0]).any():
            existe = True
    cursor.close()
    logger2.info('se  valido si existe')
    return existe


def getConn():
    database = r"globalChallenge.db"
    conn = sqlite3.connect(database)
    return conn



def insertarDatosDeApi(csv, tipo_de_tabla):
    # en este metodo vamos a buscar las columnas , validar cantidad de archivos y validar metadatos
    # la coneccion a la base es para abrir y cerrar
    conn = getConn()
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
    conn =  getConn()
    csv = buscarColumnasTabla(csv, tipo_de_tabla)
    csv.set_index('id', inplace=True)
    csv.to_sql(tipo_de_tabla, conn, if_exists='replace', index=True)
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


def restaurar_bkp():
    #borramos las tablas validamos que no exista nada mas
    initialize_data_base()
    conn = getConn()
    c = conn.cursor()
    restaurarultimo_bkp(conn)
    conn.commit()
    conn.close()
    logger2.info('Se logro reanudar toda la base')
    return None




def cargar_datos_iniciales():
        #se validan si los datos iniciales se encuentran presentes sino se insertan.
        paths = ['./archives/jobs.csv','./archives/departments.csv','./archives/hired_employees.csv']
        tablas = ['jobs','departments','hired_employees']
        i = 0
        conn = getConn()
        cursor = conn.cursor()
        existen = False
        while i < len(paths) and not existen:
            cursor.execute('''SELECT * FROM  ''' + tablas[i])
            csv = pd.read_csv(paths[i])
            df = pd.DataFrame.from_records(cursor.fetchall(),
                                           columns=[desc[0] for desc in cursor.description])
            if df.iloc[:,0].isin(csv.iloc[:,0]).any():
                existen = True
            else:
                insertardatos_y_bkp(csv, tablas[i])
            i = i +1
        conn.commit()
        conn.close()
        if not existen:
            logger2.info('Se insertaron los registros iniciales')
        else :
            logger2.info('Registros iniciales cargados!')
        return existen


def buscarConsultasMetricas(metricas1 ):
    #metodo para buscar en las metricas correspondientes
    # se busca mejor manera de relacionar evitando if, probablemente numerico desde origen
    opcion = 0
    opcion2 = 1
    sql = ''
    if metricas1 == 'metricas1':
        sql = CONSULTAS_METRICAS[opcion]
    elif metricas1 == 'metricas2':
        sql = CONSULTAS_METRICAS[opcion2]
        #sql = CONSULTAS_METRICAS[opcion]
    logger2.info('Se procede a devolver la query de metricas ' + metricas1 )
    return sql


def cargarTablaMetricas(conn, metricas1):
    #con este metodo se va a poblar la tabla que a posterior se presentaran los resultados
    cursor = conn.cursor()
    tabla = metricas1
    query = buscarConsultasMetricas(metricas1)
    logger2.info('Se procede a ingestar la metricas ' + metricas1)
    cursor.execute(query)
    df = pd.DataFrame.from_records(cursor.fetchall(),
                                   columns=[desc[0] for desc in cursor.description])
    df.to_sql(tabla, conn, if_exists='replace', index=True)

    pass



def obtener_datos_del_cuatrimestre(cuatrimestre , numero_de_cuatrimestre,conn):
    #Se genera un dataframe acorde a la query de negocio
    cursor = conn.cursor()
    cursor.execute('''
    select department, job, cantidad as Q'''+numero_de_cuatrimestre+'''
     from metricas1 where mes between ''' + cuatrimestre + '''
                   order by department , job ''')
    df = pd.DataFrame.from_records(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    df['Q'+numero_de_cuatrimestre] = df['Q'+numero_de_cuatrimestre].astype(int)

    return df
    pass




def obtenerMetricas1(metricas):
    conn = getConn()
    cargarTablaMetricas(conn, metricas)
    centinela = 'vacio'
    j = 1
    logger2.info('Se procede a generar el dataframe acorde a las nesesidades de negocio')
    for i in CUATRIMESTRES:
            df = obtener_datos_del_cuatrimestre(i,str(j),conn)
            if centinela  =='vacio':
                centinela = 'no vacio'
                df_row = df.copy()
            else :
                df_row = pd.concat([df, df_row])

            j = j +1

    df_row.sort_values(['department', 'job'])
    df_row = df_row.fillna(0)
    df_row = df_row.astype(CONVERT_DICT_METRICAS_1)
    result = pd.DataFrame(df_row).to_json(orient='records')
    conn.commit()
    conn.close()
    return result




def obtenerMetricas2(metricas):
    conn = getConn()
    cargarTablaMetricas(conn,metricas)

    logger2.info('Se procede a generar el dataframe acorde a las nesesidades de negocio')
    cursor = conn.cursor()
    cursor.execute('Select department_id , department , cantidad_media as hired from metricas2 where cantidad > cantidad_media order by cantidad desc')

    df = pd.DataFrame.from_records(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    print('obtube el df')
    df = df.astype(CONVERT_DICT_METRICAS_2)
    print('converti el diccionario')
    result = pd.DataFrame(df).to_json(orient='records')
    print(result)


    return result