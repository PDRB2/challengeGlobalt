from flask import Flask, request, jsonify, app
import pandas as pd
import logging as logging
logging.basicConfig(filename='api.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')

app = Flask(__name__)
import conn.db as db


@app.route('/datos', methods=["POST"])
def datos():
    print(request.is_json)
    content = request.get_json()
    try:
        respuesta_api =(insertarRegistros(content, db))
    except:
        logging.warning('No se cargo correctamente el registro enviado')
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
