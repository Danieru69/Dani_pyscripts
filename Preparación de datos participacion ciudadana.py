# -*- coding: utf-8 -*-
"""
Created on Tue Jun  7 14:53:21 2022

@author: danielf.moreno
"""

import pyodbc 
import pandas as pd
from IPython.display import display

#Conexión con el ODBC desde python para acceder a las BD de medios y SYPAR
conn_MEDIOS= pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=SCSQLDIARIDC;'
                      'Database=REDESYMEDIOS;'
                      'Trusted_Connection=yes;')
conn_SIPAR = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=SCSQLDIARIDC;'
                      'Database=CGR_SIPAR;'
                      'Trusted_Connection=yes;')

#Querys SQL de consulta en la BD de SIPAR para la tabla SOLICITUDES
df_SOLICITUDES1 = pd.read_sql_query("""SELECT
    SOL_CODIGO,
    SOL_DESCRIPCION,
    SOL_LOG_CODIGO,
    SOL_ENT_CODIGO as ENT_CODIGO,
    SOL_FECHA_REGISTRO_SOLICITUD,
    SOL_NUMERO_RADICADO,
    SOL_FECHA_RADICADO,
    SOL_DERECHO_PETICION,
    SOL_ENT_CODIGO_PARTICIPANTE,
    SOL_DEP_CODIGO, SOL_ESTADO,
    SOL_ANONIMO,
    SOL_PAR_CODIGO,
    SOL_OTRA_ENTIDAD_DENUNCIA,
    SOL_VIA_CODIGO,
    SOL_CARGO_SOLICITANTE,
    SOL_NUMERO_UNICO_NACIONAL,
    SOL_ORIENTACION,
    SOL_VIS_CODIGO,
    SOL_REMITE_OTRA_ENTIDAD,
    SOL_RADICADO_SALIDA_COMPLETO,
    SOL_FECHA_INGRESO_CGR
    FROM [CGR_SIPAR].[dbo].[SOLICITUDES]
    WHERE (SOL_VIA_CODIGO = 1 or SOL_VIA_CODIGO = 4 or SOL_VIA_CODIGO = 11 or SOL_VIA_CODIGO = 15) and SOL_FECHA_REGISTRO_SOLICITUD >= \'2016-01-01 00:00:00\'
    """, conn_SIPAR)
    
df_SOLICITUDES2 = pd.read_sql_query("""SELECT
    SOL_CODIGO,
    SOL_DESCRIPCION,
    SOL_LOG_CODIGO,
    SOL_ENT_CODIGO,
    SOL_FECHA_REGISTRO_SOLICITUD,
    SOL_NUMERO_RADICADO,
    SOL_FECHA_RADICADO,
    SOL_DERECHO_PETICION,
    SOL_ENT_CODIGO_PARTICIPANTE,
    SOL_DEP_CODIGO,
    SOL_ESTADO,
    SOL_ANONIMO,
    SOL_PAR_CODIGO,
    SOL_OTRA_ENTIDAD_DENUNCIA,
    SOL_VIA_CODIGO,
    SOL_CARGO_SOLICITANTE,
    SOL_NUMERO_UNICO_NACIONAL,
    SOL_ORIENTACION,
    SOL_VIS_CODIGO,
    SOL_REMITE_OTRA_ENTIDAD,
    SOL_RADICADO_SALIDA_COMPLETO,
    SOL_FECHA_INGRESO_CGR
    FROM [CGR_SIPAR].[dbo].[SOLICITUDES]
    WHERE  SOL_FECHA_RADICADO >= \'2016-01-01 00:00:00\'
    """, conn_SIPAR)

#Se realiza un drop con el objetivo de descartar los registros que cumplan la condición
df_SOLICITUDES2.drop(df_SOLICITUDES2[df_SOLICITUDES2['SOL_VIA_CODIGO'] == 1].index, inplace = True)
df_SOLICITUDES2.drop(df_SOLICITUDES2[df_SOLICITUDES2['SOL_VIA_CODIGO'] == 4].index, inplace = True)
df_SOLICITUDES2.drop(df_SOLICITUDES2[df_SOLICITUDES2['SOL_VIA_CODIGO'] == 11].index, inplace = True)

#Querys SQL de consulta en la BD de SIPAR para lsa tablas ENTIDADES y DEPENDENCIAS
df_ENTIDADES1 = pd.read_sql_query("""SELECT ENT_CODIGO, ENT_NOMBRE FROM [CGR_SIPAR].[dbo].[ENTIDADES]""", conn_SIPAR)
df_DEPENDENCIAS = pd.read_sql_query('SELECT DEP_CODIGO, DEP_DEP_CODIGO, DEP_LOG_CODIGO, DEP_NOMBRE FROM [CGR_SIPAR].[dbo].[DEPENDENCIAS]', conn_SIPAR)

#Se Anexan los dos dataframe creados a partir de la tabla SOLICITUDES en uno solo y luego se filtran las columnas que nos interesan
df_SOLICITUDES = pd.concat([df_SOLICITUDES1,df_SOLICITUDES2])
df_SOLICITUDES_FILTERED = df_SOLICITUDES [["SOL_CODIGO","SOL_DESCRIPCION","ENT_CODIGO","SOL_FECHA_REGISTRO_SOLICITUD","SOL_NUMERO_RADICADO","SOL_FECHA_RADICADO","SOL_ESTADO","SOL_VIA_CODIGO","SOL_NUMERO_UNICO_NACIONAL","SOL_FECHA_INGRESO_CGR"]]

#Se realiza un Left Join entre los dataframe usando como llave "ENT_CODIGO" para saber que entidades tienen asignadas una solicitud
df_Merge_1 = pd.merge(df_SOLICITUDES_FILTERED, df_ENTIDADES1, on = 'ENT_CODIGO', how = 'left')

#Querys SQL de consulta en la BD de SIPAR para las tablas TRASLADOS y ENTIDADES
df_TRASLADOS = pd.read_sql_query("""SELECT
    TRA_CODIGO,
    TRA_SOL_CODIGO AS SOL_CODIGO,
    TRA_TIPO,
    TRA_DEP_CODIGO_DESTINO,
    TRA_ENT_CODIGO_DESTINO,
    TRA_ESTADO_REGISTRO,
    TRA_FECHA_ELIMINACION,
    TRA_FECHA_REGISTRO,
    TRA_TRA_CODIGO_RETRASLADO,
    TRA_COMISIONADO
    FROM [CGR_SIPAR].[dbo].[TRASLADOS]
    """, conn_SIPAR)
df_ENTIDADES2 = pd.read_sql_query('SELECT ENT_CODIGO AS TRA_ENT_CODIGO_DESTINO, ENT_NOMBRE AS TRA_ENT_NOMBRE_DESTINO FROM [CGR_SIPAR].[dbo].[ENTIDADES]', conn_SIPAR)

#Se realiza un Left Join entre los dataframe usando como llave "TRA_ENT_CODIGO_DESTINO"
df_Merge_2 = pd.merge(df_TRASLADOS, df_ENTIDADES2, on = 'TRA_ENT_CODIGO_DESTINO', how = 'left')

#Se seleccionan los registros que tengan estado "A" o que sean nulos
df_Merge_2_select = df_Merge_2.loc[(df_Merge_2['TRA_ESTADO_REGISTRO']== 'A')|(df_Merge_2['TRA_ESTADO_REGISTRO'].isnull())]

#Se realiza un Left Join entre los dataframe usando como llave "SOL_CODIGO" para saber si las solicitudes han tenido traslados y a que entidades se han trasladado, se renombra la columna "TRA_DEP_CODIGO_DESTINO"
df_Merge_3 = pd.merge(df_Merge_1, df_Merge_2_select, on = 'SOL_CODIGO', how = 'left')
df_Merge_3.rename(columns = {'TRA_DEP_CODIGO_DESTINO': 'DEP_CODIGO'}, inplace = True)

#Se realiza un Left Join entre los dataframe usando como llave "DEP_CODIGO" para saber que dependencia de la contraloría está tramitando o posee la solicitud
df_Merge_4 = pd.merge(df_Merge_3, df_DEPENDENCIAS, on = 'DEP_CODIGO', how = 'left')
df_Merge_4.loc[:, 'NUM_TRAS_ACT'] = 1

#Se agrupa el dataframe por las variables "SOL_CODIGO" y "TRA_CODIGO" para crear una nueva variable que nos indica cuantos traslados activos ha tenido cada solicitud
df_group_Merge_4 = df_Merge_4.groupby(by=['SOL_CODIGO','TRA_CODIGO'], dropna=False)[['NUM_TRAS_ACT']].agg('count')
df_group_Merge_4.reset_index(drop=False, inplace=True)
df_group_Merge_4.dropna(subset=['TRA_CODIGO'], how='any', inplace=True)
df_group_Merge_4 = df_group_Merge_4.groupby(by=['SOL_CODIGO'], dropna=False)[['NUM_TRAS_ACT']].agg('count')

#Se funde la nueva variable con el dataframe que se ha venido cunstruyendo previamente
df_Merge_5 = pd.merge(df_Merge_4, df_group_Merge_4, on = 'SOL_CODIGO', how = 'outer')
df_Merge_5.drop_duplicates(subset = 'SOL_CODIGO', keep = 'first')
display(df_Merge_5.info())

"""
writer = pd.ExcelWriter('converted-to-excel.xlsx')
df_group_Merge_4.to_excel(writer)
writer.save()
"""


"""
print(df_SOLICITUDES[["SOL_CODIGO","SOL_DESCRIPCION","SOL_ENT_CODIGO","SOL_FECHA_REGISTRO_SOLICITUD","SOL_NUMERO_RADICADO","SOL_FECHA_RADICADO","SOL_ESTADO","SOL_VIA_CODIGO","SOL_NUMERO_UNICO_NACIONAL","SOL_FECHA_INGRESO_CGR"]])
print(type(df_SOLICITUDES[["SOL_CODIGO","SOL_DESCRIPCION","SOL_ENT_CODIGO","SOL_FECHA_REGISTRO_SOLICITUD","SOL_NUMERO_RADICADO","SOL_FECHA_RADICADO","SOL_ESTADO","SOL_VIA_CODIGO","SOL_NUMERO_UNICO_NACIONAL","SOL_FECHA_INGRESO_CGR"]]))
"""