import logging

import pandas as pd
import dataframe_image as dfi

from router.api import handle

logger2 = logging.getLogger(handle)

CONVERT_DICT_METRICAS_1 = {'department': object,
                           'job': object,
                           'Q1': int,
                           'Q2': int,
                           'Q3': int,
                           'Q4': int
                           }

def mostrarMetricas1():
    df = pd.read_json("reportes/datos_reportes/metricas2.json")

    data = pd.json_normalize(df['record_info'])
    data = data.astype(CONVERT_DICT_METRICAS_1)
    data['department'] = data['department'].astype('string')
    data = data.replace(r'\r\n', ' ', regex=True)
    print(data.head())
    dfi.export(data, 'Metricas1.png',max_rows = -1)





    pass