# -*- coding: utf-8 -*-
"""
Created on Mon Jul 25 09:24:02 2022

@author: nicolas.osorio
"""
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
ruta_entrada = r'P:\SUPER_NOTARIADO\Solicitudes_originales'
ruta_salida = r'P:\SUPER_NOTARIADO\Solicitudes_arregladas'
ruta_escritura = r'P:\SUPER_NOTARIADO\Resultados'

# solicitudes = glob(os.path.join(ruta_entrada,'*_LuzPineros'))
# solicitudes = [os.path.basename(ele) for ele in solicitudes]
#solicitudes = ['2022IE0066520']
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


nom_solicitud = ['2022IE0138580']
a=0
for solicitud in nom_solicitud:
    
    ruta_salida = r'P:\SUPER_NOTARIADO\Resultados'+f'\{nom_solicitud[a]}'

    if not os.path.exists(ruta_salida):
            os.mkdir(ruta_salida)

    consulta_peticiones = f"SELECT id,nombreArchivo FROM [dbo].[LogConsultasSupernot] WHERE asunto = '{nom_solicitud[a]}' AND estado = 'F'"
    peticiones = pd.read_sql(consulta_peticiones,engine_SNC)
    peticiones = peticiones.drop_duplicates(subset = 'nombreArchivo',keep = 'last').reset_index(drop=True)

    peticiones_ya = glob(os.path.join(ruta_salida,'*'))
    peticiones_ya = [os.path.basename(ele) for ele in  peticiones_ya]
    peticiones_ya = [int(re.findall(r'(?!:_)\d{1,}',os.path.basename(ele))[0]) for ele in  peticiones_ya]
    peticiones_ya = list(set(peticiones_ya))
    correr = [ele for ele in peticiones['id'] if ele not in peticiones_ya]
    # pet = 22
        
    for pet in correr:
        consulta_anotaciones = f'''select 
        db.[tipodocidentcons]
		,db.[identifconsultada]
		,db.[novedad]
		,db.[idjurisdiccion]
		,db.[jurisdiccion]
		,db.[matricula]
		,db.[chip]
		,db.[cedulacatastral]
		,db.[direccion]
		,ac.[numeroanotacion]
		,ac.[fechaanotacion]
		,ac.[radicacionanotacion]
		,ac.[nomdocumentoanotacionfolio]
		,ac.[cantidadmonetaria]
		,ac.[codnaturalezajuridicafolio]
		,ac.[nomnaturalezajuridicafolio]
		,ac.[comentario]
		,ac.[grupo]
		,pc.[razonsocial] as razonsocial_persona_cede
		,pc.[numidtributaria] as numidtributaria_persona_cede
		,pr.[primerapellido] as primerapellido_persona_recibe
		,pr.[segundoapellido] as segundoapellido_persona_recibe
		,pr.[primernombre] as primernombre_persona_recibe
		,pr.[segundonombre] as segundonombre_persona_recibe
		,pr.[codtipoidentificacion] as codtipoidentificacion_persona_recibe
		,pr.[numerocedulaciudadania] as numerocedulaciudadania_persona_recibe
		,pr.[espropietario] as espropietario_persona_recibe
		,pr.[anotacioncertificadoid] as anotacioncertificadoid_persona_recibe
		,pr.[tipopersona] as tipopersona_persona_recibe
		,pr.[numidtributaria] as numidtributaria_persona_recibe
		,pr.[razonsocial] as razonsocial_persona_recibe
		,pr.[porcentajeparticipacion] as porcentajeparticipacion_persona_recibe
		,db.[consecconsestjuridicologsupernot]
		,db.[fechaconsestjuridicologsupernot]
		,db.[departamentopredio]
		,db.[municipiopredio]
		,db.[estadomatricula]
		,db.[numeropropietarios]
		,db.[persrecibeprediocodtipoidentificacion]
		,db.[persrecibepredionumeroidentificacion]
		,db.[persrecibepredioprimerapellido]
		,db.[persrecibeprediosegundoapellido]
		,db.[persrecibepredioprimernombre]
		,db.[persrecibeprediosegundonombre]
		,db.[persrecibepredionumidtributaria]
		,db.[persrecibeprediorazonsocial]
		,db.[persrecibepredioespropietario]
		,ac.[coddocumentoanotacionfolio]
		,ac.[fechadocumentoanotacion]
		,ac.[entidadanotacion]
		,ac.[comentarioespecificaanotacionfolio]
		,ac.[nombremunicipioanotacion]
		,ac.[validez]
		,ac.[codtipomoneda]
		,pc.[anotacioncertificadoid] as anotacioncertificadoid_persona_cede
		,pc.[tipopersona] as tipopersona_persona_cede
		,pc.[espropietario] as espropietario_persona_cede
		,pc.[numerocedulaciudadania] as numerocedulaciudadania_persona_cede
		,pc.[codtipoidentificacion] as codtipoidentificacion_persona_cede
		,pc.[primerapellido] as primerapellido_persona_cede
		,pc.[primernombre] as primernombre_persona_cede
		,pc.[segundoapellido] as segundoapellido_persona_cede
		,pc.[segundonombre] as segundonombre_persona_cede
		,pc.[porcentajeparticipacion] as porcentajeparticipacion_persona_cede
		from [dbo].[matriculas_datosbasicos] db
		left join matricula_anotacioncertificado ac on db.id = ac.idmatriculadatosbasicosid
		left join anotacioncertificado_persona_cede pc on ac.id = pc.anotacioncertificadoid
		left join anotacioncertificado_persona_recibe pr on ac.id = pr.anotacioncertificadoid
		where db.idLogConsulta = {str(pet)}
        '''
        
        consulta_propietarios = f'''
        SELECT db.idLogConsulta, db.id, db.IdentifConsultada, db.IdJurisdiccion ,db.Matricula, db.novedad, db.Direccion, db.Chip, db.CedulaCatastral,
        mp.TipoPersona, mp.NumeroCedulaCiudadania, mp.NumIdTributaria, mp.EsPropietario, mp.RazonSocial, mp.PrimerApellido, mp.SegundoApellido, mp.PrimerNombre, mp.SegundoNombre
        FROM [dbo].[Matriculas_DatosBasicos] db
        left join Matricula_Propietarios_Actuales mp on db.id = mp.MatriculaDatosBasicosid
        where db.idLogConsulta = {str(pet)}
        '''
        
        anotaciones = pd.read_sql(consulta_anotaciones,engine_SNC)
        propietarios = pd.read_sql(consulta_propietarios,engine_SNC)
        
        anotaciones.to_excel(os.path.join(ruta_salida,f'{nom_solicitud[a]}_anotaciones_SUPERNOTARIADO.xlsx'),index=False)
        propietarios.to_excel(os.path.join(ruta_salida,f'{nom_solicitud[a]}_propietarios_propiedades_SUPERNOTARIADO.xlsx'),index=False)
        a=a+1

