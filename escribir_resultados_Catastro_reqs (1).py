# =============================================================================
# Paquetes
# =============================================================================
import os, re
import pandas as pd
from glob import glob
from unidecode import unidecode
from time import time
from datetime import datetime
from sqlalchemy import create_engine
# =============================================================================
# Parametros
# =============================================================================
ruta_entrada = r'S:\Solicitudes_originales'
ruta_salida = r'S:\Solicitudes_arregladas'
ruta_escritura = r'S:\Resultados'

# solicitudes = glob(os.path.join(ruta_entrada,'*_LuzPineros'))
# solicitudes = [os.path.basename(ele) for ele in solicitudes]
solicitudes = ['prueba_consulta_2023IE0015248_MANP']
# =============================================================================
# Conexión a BD 
# =============================================================================
engine_SNC = create_engine('mssql+pyodbc://{server}:{port}/{db}?driver={dvr}&{auth}'
                          .format(server = 'sqlsrv-diari-ui-trv-ws.database.windows.net',
                                  port = '1433',
                                  db = 'db-diari-ui-trv-ws-supernotariado',
                                  dvr = 'ODBC+Driver+17+for+SQL+Server',
                                  auth = 'trusted_connectios=yes'))

engine_CAT = create_engine('mssql+pyodbc://{server}:{port}/{db}?driver={dvr}&{auth}'
                          .format(server = 'sqlsrv-diari-ui-trv-ws.database.windows.net',
                                  port = '1433',
                                  db = 'db-diari-ui-trv-ws-catastro',
                                  dvr = 'ODBC+Driver+17+for+SQL+Server',
                                  auth = 'trusted_connectios=yes'))

# =============================================================================
# Consultas catastro
# =============================================================================
for solicitud in solicitudes:
    
    ruta_solicitud = os.path.join(ruta_escritura,solicitud)
    
    if not os.path.exists(ruta_solicitud):
        os.mkdir(ruta_solicitud)
        
    consulta_peticiones = f"SELECT id,nombreArchivo FROM [dbo].[LogConsultasCatastro] WHERE asunto = '{solicitud}' AND estado = 'F'"
    peticiones = pd.read_sql(consulta_peticiones,engine_CAT)
    peticiones = peticiones.drop_duplicates(subset = 'nombreArchivo').reset_index(drop = True)
    # pet = 22
    
    for pet in peticiones['id'].tolist():
        
        consulta_cat = f'''
        select pr.idLogConsulta, pr.identifConsultada, pr.matricula, pr.direccion_real,pr.chip,
    pro.numero_identificacion, pro.nombre_propietario, pro.primer_apellido, pro.segundo_apellido,av.avaluo_ano,av.valor_avaluo
    from  (Predio pr
    left join Propietario pro on pr.id = pro.Predioid) left join Avaluo av on av.Predioid = pr.id 
    where pr.idLogConsulta = {str(pet)}
    order by pr.id,av.avaluo_ano asc
        '''
        
        catastro = pd.read_sql(consulta_cat,engine_CAT)
        catastro.to_excel(os.path.join(ruta_solicitud,f'{solicitud}_CATASTRO.xlsx'),index=False)
