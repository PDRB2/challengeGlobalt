from flask import Flask, request, jsonify, app
import pandas as pd
import logging as logging

logging.basicConfig(level="INFO", format="%(levelname)s:%(asctime)s:%(message)s", filename='api.log')
handle = "api"
logger1 = logging.getLogger(handle)
USUARIO = 'JOSE'
CREDECIALES = '7d6e3c09e9f8a1b9a729122632debd4fa5989860beb430fe0f6005ce1fecc5a8'
METRICAS_1 = 'metricas1'
METRICAS_2 = 'metricas2'

app = Flask(__name__)
import conn.db as db


def validarCredenciales(user, credentials):
    return (user == USUARIO and CREDECIALES == credentials)


@app.route('/datos', methods=["POST"])
def datos():
    content = request.get_json()
    try:

        if validarCredenciales(request.headers.get('user'), request.headers.get('credentials')):
            logging.info('el usuario ' + request.headers.get('user') + ' se logeo correctamente')
            respuesta_api = (ArmarRegistrosApi(content, db))
        else:
            logging.info(' Un usuario  es desconocido y se le a denegado acceso')
            return 'No tiene permisos para acceder.'

    except:
        logging.info('No se cargo correctamente el registro enviado')
        return 'JSON INCORRECTAMENTE FORMADO'
    logging.info('Se cargo correctamente el registro enviado')
    return respuesta_api


@app.route('/metricas1', methods=["get"])
def metricas1():
    # se crea el endpoint correspondiente para
    # retornar los datos agrupados de cada departamento , trabajo y cantidades acorde a el cuatrimestre

    if not validarCredenciales(user=request.headers.get('user'), credentials=request.headers.get('credentials')):
        logging.info(' el usuario ' + request.headers.get('user') + ' es desconocido y se le a denegado acceso')
        return 'No tiene permisos para acceder.'

    logging.info('el usuario ' + request.headers.get('user') + ' se logeo correctamente')
    try:
        logging.info('Se procede a intentar retornar el json de metricas 1')
        return (db.obtenerMetricas1(METRICAS_1))
    except:
        logging.warning('Error en la devolucion de json contactar con soporte!')
        return 'Error en la devolucion del json , intente contactar a soporte'


@app.route('/metricas2', methods=["get"])
def metricas2():
    # se crea el endpoint correspondiente para
    # retornar los datos agrupados de cada departamento , trabajo y cantidades acorde a el cuatrimestre

    if not validarCredenciales(user=request.headers.get('user'), credentials=request.headers.get('credentials')):
        logging.info(' el usuario ' + request.headers.get('user') + ' es desnocido y se le a denegado acceso')
        return 'No tiene permisos para acceder.'
    logging.info('el usuario ' + request.headers.get('user') + ' se logeo correctamente')
    try:
        logging.info('Se procede a intentar retornar el json de metricas 2')
        return (db.obtenerMetricas2(METRICAS_2))
    except:
        logging.warning('Error en la devolucion de json contactar con soporte!')
        return 'Error en la devolucion del json , intente contactar a soporte'


def ArmarRegistrosApi(content, db):
    try:

        data = pd.json_normalize(content['tables'])
        for index, row in data.iterrows():
            registros = pd.json_normalize(row['record_info'])
            mensaje = db.insertarDatosDeApi(registros, row['table'])
            return (mensaje)

    except:

        logging.warning('No se leyo correctamente el json , incumplimiento del contrato')
        return 'JSON NO LEIBLE'
    pass


def iniciar():
    app.run(host='0.0.0.0', port=8000)
