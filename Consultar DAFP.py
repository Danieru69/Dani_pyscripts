# -*- coding: utf-8 -*-
"""
Created on Wed Dec 14 07:14:11 2022

@author: danielf.moreno
"""
import pyodbc
import os
import pandas as pd
from IPython.display import display
from pathlib import Path

import sys

print(sys.maxsize)

#Conexión con el ODBC desde python para acceder a las BD de medios y SYPAR
conn_DAFP= pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=SCSQLDIARIDC;'
                      'Database=GPIF_DAFP;'
                      'Trusted_Connection=yes;')

#Querys SQL de consulta en la BD del DAFP para la tabla [ConsultaVinculaciones]
df_ConsultaVinculaciones = pd.read_sql_query("""SELECT [ID_INSTITUCION]
      ,[INSTITUCION]
      ,[SECTOR]
      ,[CLASIFICACION_ORGANICA]
      ,[NATURALEZA]
      ,[MUNICIPIO_INSTITUCION]
      ,[DPTO_INSTITUCION]
      ,[TIPO_DOCUMENTO]
      ,[IDENTIFICACION]
      ,[PRIMER_NOMBRE]
      ,[SEGUNDO_NOMBRE]
      ,[PRIMER_APELLIDO]
      ,[SEGUNDO_APELLIDO]
      ,[TELEFONO]
      ,[DIRECCION_DOMICILIO]
      ,[CORREO_ELECTRONICO]
      ,[FECHA_INGRESO]
      ,[NOMBRAMIENTO_VINCULACION]
      ,[NATURALEZA_EMPLEO_ACTUAL]
      ,[NIVEL_JERARQUICO_EMPLEO]
      ,[DENOMINACION_EMPLEO_ACTUAL]
      ,[CODIGO_EMPLEO_ACTUAL]
      ,[GRADO_EMPLEO_ACTUAL]
      ,[DEPENDENCIA_EMPLEO_ACTUAL]
      ,[ASIGNACION_BASICA]
      ,[GASTOS_REPRESENTACION]
      ,[PRIMA_DIRECCION]
      FROM [GPIF_DAFP].[dbo].[ConsultaVinculaciones]
      """, conn_DAFP)
      
path = r'S:\2.DELEGADAS\UNCOPI\RESPUESTA A SOLICITUDES\5431 - 5450\DAFP'

# Get the files from the path provided in the OP
files = Path(path).glob('*.xlsx')  # .rglob to get subdirectories

dfs = list()
for f in files:
    data = pd.read_excel(f)
    print(data.info())
    data = data.drop(['ITEM','PRIMER NOMBRE/RAZON SOCIAL','SEGUNDO NOMBRE','PRIMER APELLIDO','SEGUNDO APELLIDO','ACTUACION'], axis=1)
    data["IDENTIFICACIÓN"] = data["IDENTIFICACIÓN"].str.replace("C,","")
    data["IDENTIFICACIÓN"] = data["IDENTIFICACIÓN"].str.replace("N,","")
    data["IDENTIFICACIÓN"] = data["IDENTIFICACIÓN"].str.replace(" ","")
    data["IDENTIFICACIÓN"] = data["IDENTIFICACIÓN"].str.replace(".","")
    data['IDENTIFICACIÓN']=data['IDENTIFICACIÓN'].astype(float)
    
    # .stem is method for pathlib objects to get the filename w/o the extension
    #data['Archivo Origen'] = f.stem
    #Se realiza un Left Join entre los dataframe
    data.rename(columns = {'IDENTIFICACIÓN': 'IDENTIFICACION'}, inplace = True)
    df_Merge = pd.merge(data, df_ConsultaVinculaciones, on = 'IDENTIFICACION', how = 'left')
    name = os.path.basename(f).split("_")[0] #.replace("C","").replace("V","").replace("D","").replace("S","")
    name = str(name)
    #print(name)
    
    df_Merge.to_excel(r'S:\2.DELEGADAS\UNCOPI\RESPUESTA A SOLICITUDES\5431 - 5450\DAFP\resultados\\'+ name +'_DAFP.xlsx' , index=False)
    #dfs.append(data)
    

