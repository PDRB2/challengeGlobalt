from flask import Flask, request, jsonify, app
import pandas as pd
from pandas.io.json import json_normalize

app = Flask(__name__)
import conn.db as db


@app.route('/datos', methods=["POST"])
def datos():
    print(request.is_json)
    content = request.get_json()
    try:
        respuesta_api =(insertarRegistros(content, db))
    except:
        return 'JSON INCORRECTAMENTE FORMADO'
    return respuesta_api


def insertarRegistros(content, db):
    try:
         print('todo va bien')
         data = pd.json_normalize(content['tables'])
         for index, row in data.iterrows():
             print(row['table'])
             registros = pd.json_normalize(row['record_info'])
             print(registros)
             mensaje = db.insertarDatos(registros,row['table'])
             return(mensaje)

    except:

        return 'JSON NO LEIBLE'
    pass


def iniciar():
    app.run(host='0.0.0.0', port=8000)
