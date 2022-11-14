from flask import Flask, request, jsonify, app
import pandas as pd
import logging as logging
logging.basicConfig(level="INFO",format="%(levelname)s:%(asctime)s:%(message)s",filename='api.log')
handle = "api"
logger1 = logging.getLogger(handle)
USUARIO = 'JOSE'
CREDECIALES = '7d6e3c09e9f8a1b9a729122632debd4fa5989860beb430fe0f6005ce1fecc5a8'


app = Flask(__name__)
import conn.db as db


def validarCredenciales(user, credentials):
    return user == USUARIO and CREDECIALES == credentials
    pass


@app.route('/datos', methods=["POST"])
def datos():
    content = request.get_json()
    try:

        user = request.headers.get('user')
        credentials = request.headers.get('credentials')
        if not validarCredenciales(user,credentials):
            logging.info(' el usuario ' +user + ' es desnocido y se le a denegado acceso')
            return 'No tiene permisos para acceder.'
        logging.info('el usuario ' + user + ' se logeo correctamente')
        respuesta_api =(insertarRegistros(content, db))

    except:
        logging.info('No se cargo correctamente el registro enviado')
        return 'JSON INCORRECTAMENTE FORMADO'
    logging.info('Se cargo correctamente el registro enviado')
    return respuesta_api


def insertarRegistros(content, db):
    try:

         data = pd.json_normalize(content['tables'])
         for index, row in data.iterrows():
             registros = pd.json_normalize(row['record_info'])
             mensaje = db.insertarDatos(registros,row['table'])
             return(mensaje)

    except:

        logging.warning('No se leyo correctamente el json , incumplimiento del contrato')
        return 'JSON NO LEIBLE'
    pass


def iniciar():
    app.run(host='0.0.0.0', port=8000)
