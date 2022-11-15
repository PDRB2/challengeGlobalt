

import pandas as pd
import conn.db as db
import api as rest_api



if __name__ == '__main__':
    opciones = ['1','2','3']
    #inicia la base y crea de nuevo todas las tablas
    #el loop no era una condicion para el desafio pero quedo como un agregado para poder hacerlo mas completa
    #la solucion , sencillamente lo que hace es forzar al funcionamiento desado
    #esta solucion solo en caso que se restaure el backup se tomaran los registros que guarde la api

    opcion = input("Eliga su opcion: 1 iniciar apy , 2 borrar_la_base , 3 restaurar backup")
    while opcion not in opciones :
        opcion = input("Opcion incorrecta: 1 iniciar apy , 2 borrar_la_base , 3 restaurar backup ")
    if opcion == '1':
        db.initialize_data_base()
        db.cargar_datos_iniciales()
        rest_api.iniciar()
    elif opcion == '2':
        db.initialize_data_base()
    elif opcion == '3':
        db.restaurar_bkp()




