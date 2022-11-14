

import pandas as pd
import conn.db as db
import api as rest_api


def cargar_datos_iniciales():


    csv = pd.read_csv('./archives/jobs.csv')
    tipo_de_tabla = 'jobs'
    db.insertardatos_y_bkp(csv, tipo_de_tabla)
    csv2 = pd.read_csv('./archives/departments.csv')
    tipo_de_tabla = 'departments'
    db.insertardatos_y_bkp(csv2, tipo_de_tabla)
    csv3 = pd.read_csv('./archives/hired_employees.csv')
    tipo_de_tabla = 'hired_employees'
    db.insertardatos_y_bkp(csv3, tipo_de_tabla)


if __name__ == '__main__':

    db.initialize_data_base()
    cargar_datos_iniciales()
    #rest_api.iniciar()
    db.borrar_y_restaurar_bkp()
