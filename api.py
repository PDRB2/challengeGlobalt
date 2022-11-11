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
        print(content['tabla'])
        print(content['registros'])
        print(insertarRegistros(content, db))
    except:
        return 'JSON INCORRECTAMENTE FORMADO'
    return 'JSON posted'


def insertarRegistros(content, db):
    try:
         print('todo va bien')
         print(content['registros'])
         data = json_normalize(content['registros'])
         db.insertarDatos(data,content['tabla'])

    except:
        return 'JSON NO LEIBLE'
    pass


def iniciar():
    app.run(host='0.0.0.0', port=8000)
