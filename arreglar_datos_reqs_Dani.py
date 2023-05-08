

#%%
# =============================================================================
# Paquetes
# =============================================================================
import os, re
import pandas as pd
from glob import glob
#from unidecode import unidecode
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
import yaml
from sqlalchemy import create_engine, text
from getpass import getpass
from datetime import date, datetime
import pyodbc
import time
print(pyodbc.drivers()) 

# =============================================================================
# Parametros
# =============================================================================

#%%

ruta_entrada = r'C:\Users\danielf.moreno\Documents\Python Scripts\Supernot Auto\solicitudes'
ruta_salida = r'C:\Users\danielf.moreno\Documents\Python Scripts\Supernot Auto'
driver = webdriver.Chrome(r'C:\Users\danielf.moreno\Documents\Python Scripts\Supernot Auto\chromedriver.exe')

engine_SNC_supernot = create_engine('mssql+pyodbc://{server}:{port}/{db}?driver={dvr}&{auth}'
                          .format(server = 'sqlsrv-diari-ui-trv-ws.database.windows.net',
                                  port = '1433',
                                  db = 'db-diari-ui-trv-ws-supernotariado',
                                  dvr = 'ODBC+Driver+17+for+SQL+Server',
                                  auth = 'trusted_connectios=yes'))
engine_SNC_catastro = create_engine('mssql+pyodbc://{server}:{port}/{db}?driver={dvr}&{auth}'
                          .format(server = 'sqlsrv-diari-ui-trv-ws.database.windows.net',
                                  port = '1433',
                                  db = 'db-diari-ui-trv-ws-catastro',
                                  dvr = 'ODBC+Driver+17+for+SQL+Server',
                                  auth = 'trusted_connectios=yes'))
engine_SNC_rues = create_engine('mssql+pyodbc://{server}:{port}/{db}?driver={dvr}&{auth}'
                          .format(server = 'sqlsrv-diari-ui-trv-ws.database.windows.net',
                                  port = '1433',
                                  db = 'db-diari-ui-trv-ws-rues',
                                  dvr = 'ODBC+Driver+17+for+SQL+Server',
                                  auth = 'trusted_connectios=yes'))

idLogConsulta_catastro = ''
idLogConsulta_rues = ''
idLogConsulta_supernot = ''


def __init__(self,ruta_entrada, ruta_salida):
    self.ruta_entrada = r'P:\SUPER_NOTARIADO\Solicitudes_originales'
    self.ruta_salida = r'P:\SUPER_NOTARIADO\Solicitudes_arregladas'
    
    



def state_driver():
    state = 0
    
    if "Login de usuario" == driver.title:
        state = 1
    if "Login de usuario" != driver.title:
        print("No está en el login o en la página del WS")
        state = 0
    if driver.find_elements(By.CLASS_NAME, "btn.btn-primary") != []:
        print("Debe loguearse")
        state = 2
    if "Index" == driver.title:
        if os.listdir(os.path.join(ruta_salida,"Archivos_planos_WS")) == []:
            state = 3
        else:
            print("Datos Preparados")
            state = 4
    #if :
        
    return state
    
    

def Procedure(state):
    
    #==========================================
    #state variable status :V
    #
    #0--> Login page
    #1--> Create or owerwrite yaml file
    #2--> try to login
    #3--> Home page (Select WS)
    #4--> Catastro Ws
    #5--> Vehiculos
    #6--> DAFP
    #7--> RUES WS
    #8--> Supernot Ws
    #9--> Check Results
    #10--> Export Results
    #==========================================
    if state == 0:
        
        driver = webdriver.Chrome(r'C:\Users\danielf.moreno\Documents\Python Scripts\Supernot Auto\chromedriver.exe')
        driver.get("https://wsdiari.contraloria.gov.co")
    if state == 1:
        
        try:
            yaml.safe_load(open(r'C:\Users\danielf.moreno\Documents\Python Scripts\Supernot Auto\data.yml'))
        except:
            print("Yaml file does not exist or is not in the path")
            create_yaml()
        conf = yaml.safe_load(open(r'C:\Users\danielf.moreno\Documents\Python Scripts\Supernot Auto\data.yml'))
        try :
            myWsUser = conf['wsdiari_user']['UserName']
            myWsPassword = conf['wsdiari_user']['Password']
        except:
            print("Archivo Yaml vacío")
            create_yaml()
        
    if state == 2:
        
        login("https://wsdiari.contraloria.gov.co/Usuarios/Login", "UserName", myWsUser, "Password", myWsPassword, "btn.btn-primary")
        WebDriverWait(driver=driver, timeout=10).until(
        lambda x: x.execute_script("return document.readyState === 'complete'")
        )
        error_message = "Inicio de sesión no válido, usuario o password no son validos o servicio autenticacion con Directorio Activo presenta novedades"
        errors = driver.find_elements(By.CLASS_NAME, "text-danger.validation-summary-errors")
        print(errors)
        if any(error_message in e.text for e in errors): 
            print("[!] Login failed, try again")
            create_yaml()
            state = 1
        else:
            print("[+] Login successful")
            
    if state == 3:
                
        preparar_datos()
                   
    if state == 4:
        
        while state == 4:
            subir_doc_catastro()        
    
    #if state == 5:
        
        
    
    #if state == 6:
        
        
    
    if state == 7:
        
        while state == 7:
            subir_doc_rues()
        
        
    if state == 8:
        
        while state == 8:
            subir_doc_supernot()
        
    if state == 9:
        
        check_results()

    if state == 10:
        
        export_results()


def check_results():
    
    #if "Consulta_RUES" in os.listdir((os.path.join(ruta_salida,"Archivos_planos_WS"))):
    for archivo in os.listdir((os.path.join(ruta_salida,"Archivos_planos_WS"))):
        if "RUES" in archivo:
            #print("revisar Ws rues")
            query_log_rues= f'''SELECT [id]
            ,[nombreArchivo]
            ,[estado]
            ,[asunto]
            FROM [dbo].[LogConsultasRues] where [nombreArchivo] = '{archivo}' order by id desc'''
            try :
                engine_SNC_rues
                consulta_Log_rues = pd.read_sql(query_log_rues,engine_SNC_rues)
            except:
                print("Error al conectarse a la BD RUES")
            if consulta_Log_rues["estado"][0] == 'F':
                print("Consulta lista para exportar")
                rues_ok = True

            else:
                print("Algo falló en la consulta")
                state = 7
        elif "Catastro" in archivo:
            print("revisar Ws Catastro")
        elif "Supernot" in archivo:
            print("revisar Ws Supernot")
            
    return state

def append_to_excel(fpath, df, sheet_name):
    with pd.ExcelWriter(fpath, mode="w", engine='openpyxl') as f:
        df.to_excel(f, sheet_name=sheet_name, index = False, startrow=0)

#append_to_excel(<your_excel_path>, <new_df>, <new_sheet_name>)

def export_results():
    
    ruta_escritura_results = os.path.join(ruta_salida,"Resultados")
       
    if not os.path.exists(ruta_escritura_results):
        os.mkdir(ruta_escritura_results)
    solicitudes = glob(os.path.join(ruta_entrada,'*.xlsx'))
    
    query_rues = f"""SELECT
    [identConsultada] AS NIT
	,[digito_verificacion] AS DIGITO_VERIFICACION
	,[razon_social] AS NOMBRE_ENTIDAD
	,[organizacion_juridica] AS ORGANIZACION_JURIDICA
	,[codigo_tipo_sociedad] AS COD_TIPO_SOCIEDAD
	,[tipo_sociedad] AS TIPO_SOCIEDAD
	,[estado_matricula] AS ESTADO_MATRICULA
	,[fecha_actualizacion_rues] AS FECHA_ACTUALIZACION_RUES
	,[fecha_cancelacion] AS FECHA_CANCELACION
	,[fecha_matricula] AS FECHA_MATRICULA
	,[fecha_renovacion] AS FECHA_RENOVACION
	,[ultimo_ano_renovado] AS [ULTIMO_AÑO_RENOVADO]
	,[cod_ciiu_act_econ_pri] AS COD_CIIU_PRINC
	,[desc_ciiu_act_econ_pri] AS DES_CIIU_PRINC
	,[cod_ciiu_act_econ_sec] AS COD_CIIU_SEC
	,[desc_ciiu_act_econ_sec] AS DES_CIIU_SEC
	,[ciiu3] AS COD_CIIU_TERC
	,[desc_ciiu3] AS DES_CIIU_TERC
	,[tipo_identificacion] AS TIPO_ID_INTEGRANTE
	,[dpto_comercial] AS DEPARTAMENTO_COM
	,[municipio_comercial] AS MUNICIPIO_COM
	,[direccion_comercial] AS DIRECCION_COM
	,[telefono_comercial_1] AS TELEFONO_COM_1
	,[telefono_comercial_2] AS TELEFONO_COM_2
	,[telefono_comercial_3] AS TELEFONO_COM_3
	,[correo_electronico_comercial] AS CORREO_ELECTRONICO_COMERCIAL
	,[dpto_fiscal] AS DEPARTAMENTO_FISCAL
	,[municipio_fiscal] AS MUNICIPIO_FISCAL
	,[zona_fiscal] AS barrio_fiscal
	,[direccion_fiscal] AS DIRECCION_FISCAL
	,[telefono_fiscal_1] AS TELEFONO_FISCAL_1
	,[telefono_fiscal_2] AS TELEFONO_FISCAL_2
	,[telefono_fiscal_3] AS TELEFONO_FISCAL_3
	,[correo_electronico_fiscal] AS CORREO_ELECTRONICO_FISCAL
	,[idLogConsulta]
    FROM [dbo].[RuesConsNitDatosBasicosBD] rdb left join LogConsultasRues lg on rdb.idLogConsulta = lg.id 
    where lg.id = '{idLogConsulta_rues}' and lg.nombreArchivo = '{os.path.basename(os.path.join(ruta_salida,'Archivos_planos_WS','consulta_RUES.txt'))}'
    """
    engine_SNC_rues
    consulta_rues = pd.read_sql(text(query_rues),engine_SNC_rues)
    consulta_rues = consulta_rues[consulta_rues.NOMBRE_ENTIDAD.notnull()]
    
    consulta_anotaciones = f"""select 
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
		where db.idLogConsulta = 
        """
        
    consulta_propietarios = f"""
        SELECT db.idLogConsulta, db.id, db.IdentifConsultada, db.IdJurisdiccion ,db.Matricula, db.novedad, db.Direccion, db.Chip, db.CedulaCatastral,
        mp.TipoPersona, mp.NumeroCedulaCiudadania, mp.NumIdTributaria, mp.EsPropietario, mp.RazonSocial, mp.PrimerApellido, mp.SegundoApellido, mp.PrimerNombre, mp.SegundoNombre
        FROM [dbo].[Matriculas_DatosBasicos] db
        left join Matricula_Propietarios_Actuales mp on db.id = mp.MatriculaDatosBasicosid
        where db.idLogConsulta = 
        """
    
    
    
    for solicitud in solicitudes:
        #print(solicitud)
        if "C" in os.path.basename(solicitud):
            print(solicitud + "Catastro")
            
        if "V" in os.path.basename(solicitud):
            print(solicitud + "Vehículos")
        if "D" in os.path.basename(solicitud):
            print(solicitud + "DAFP")
        if "R" in os.path.basename(solicitud):
            print(solicitud + "RUES")
            os.path.basename(solicitud)[:13]
            docs = pd.read_excel(solicitud)
            docs = docs.drop(columns = ['ITEM','SEGUNDO NOMBRE', 'PRIMER APELLIDO', 'SEGUNDO APELLIDO', 'ACTUACION','PRIMER NOMBRE/RAZON SOCIAL'])
            docs.columns = ['DOC']
            docs["DOC"] = docs["DOC"].str.upper()
            
            for doc_rues in consulta_rues["NIT"]:
                #print(doc_rues)
                for doc in docs["DOC"]:
                    if doc_rues in doc:
                        print("Aww Yeah!")
                        if not os.path.exists(os.path.join(ruta_escritura_results,os.path.basename(solicitud)[:13])):
                            os.mkdir(os.path.join(ruta_escritura_results,os.path.basename(solicitud)[:13]))
                        df =[] 
                        df= pd.DataFrame()
                        if not os.path.exists(os.path.join(ruta_escritura_results,os.path.basename(solicitud)[:13],doc_rues+'.xlsx')):
                            df.to_excel(os.path.join(ruta_escritura_results,os.path.basename(solicitud)[:13],doc_rues+'.xlsx'))
                        append_to_excel(os.path.join(ruta_escritura_results,os.path.basename(solicitud)[:13],doc_rues+'.xlsx'), consulta_rues[consulta_rues.NOMBRE_ENTIDAD.notnull()].where((consulta_rues["NIT"] == doc_rues) & (consulta_rues["NIT"] != '')).dropna(how = 'all'), 'RUES')   
                        #consulta_rues.where(consulta_rues["NIT"] == doc_rues).dropna().append_to_excel(os.path.join(ruta_escritura_results,os.path.basename(solicitud)[:13],doc_rues+'.xlsx'),sheet_name = "RUES",index=False,header = None)
        if "S" in os.path.basename(solicitud):
            print(solicitud + "Supernot")
            
        
        

def preparar_datos():
    solicitudes = glob(os.path.join(ruta_entrada,'*.xlsx'))
    #print(solicitudes)
    for solicitud in solicitudes:
        if "C" in os.path.basename(solicitud):
            #print(solicitud + "Catastro")
            arreglar_datos_catastro(solicitud)
        if "V" in os.path.basename(solicitud):
            print(solicitud + "Vehículos")
        if "D" in os.path.basename(solicitud):
            print(solicitud + "DAFP")
        if "R" in os.path.basename(solicitud):
            #print(solicitud + "RUES")
            arreglar_datos_rues(solicitud)
        if "S" in os.path.basename(solicitud):
            #print(solicitud + "Supernot")
            arreglar_datos_supernot(solicitud)
    
def subir_doc_catastro():
    
    driver.get("https://wsdiari.contraloria.gov.co/Catastro")
    driver.find_element(By.ID,"Asunto").clear()
    driver.find_element(By.ID,"Asunto").send_keys("Consulta_", str(datetime.now()))
    try:
        driver.find_element(By.XPATH,"//input[@type='file']").send_keys(os.path.join(ruta_salida,"Archivos_planos_WS")  +'\consulta_Catastro.txt')
    except:
        print("No hay archivo Catastro")
        state = 5
    driver.find_element(By.XPATH,"/html/body/div[@class='container']/div/div[@class='container-fluid']/form/div[2]/button").click()
    time.sleep(5)
    if "se esta procesando con el id" in driver.find_element(By.XPATH,"/html/body/div[@class='container']/div/h3").text:
        print("Consulta subida")
        idLogConsulta_catastro = "\d+" in driver.find_element(By.XPATH,"/html/body/div[@class='container']/div/h3").text
        state = 5
    else:
        driver.back()
        state = 4
        
    return state

def subir_doc_rues():
    
    driver.get("https://wsdiari.contraloria.gov.co/Rues")
    driver.find_element(By.ID,"Asunto").clear()
    driver.find_element(By.ID,"Asunto").send_keys("Consulta_", str(datetime.now()))
    try:
        driver.find_element(By.XPATH,"//input[@type='file']").send_keys(os.path.join(ruta_salida,"Archivos_planos_WS")  +'\consulta_RUES.txt')
    except:
        print("No hay archivo RUES")
        state = 8
    driver.find_element(By.XPATH,"/html/body/div[@class='container']/div/div[@class='container-fluid']/form/div[2]/button").click()
    time.sleep(5)
    if any("se esta procesando con el id" in driver.find_element(By.XPATH,"/html/body/div[@class='container']/div/h3").text):
        print("Consulta subida")
        idLogConsulta_rues = "\d+" in driver.find_element(By.XPATH,"/html/body/div[@class='container']/div/h3").text
        state = 8
    else:
        driver.back()
        state = 7
        
    return state

def subir_doc_supernot():
    
    driver.get("https://wsdiari.contraloria.gov.co/Supernot")
    driver.find_element(By.ID,"Asunto").clear()
    driver.find_element(By.ID,"Asunto").send_keys("Consulta_", str(datetime.now()))
    try:
        driver.find_element(By.XPATH,"//input[@type='file']").send_keys(os.path.join(ruta_salida,"Archivos_planos_WS")  +'\consulta_Supernot.txt')
    except:
        print("No hay archivo RUES")
        state = 9
    driver.find_element(By.XPATH,"/html/body/div[@class='container']/div/div[@class='container-fluid']/form/div[3]/button").click()
    time.sleep(5)
    if any("se esta procesando con el id" in driver.find_element(By.XPATH,"/html/body/div[@class='container']/div/h3").text):
        print("Consulta subida")
        idLogConsulta_supernot = "\d+" in driver.find_element(By.XPATH,"/html/body/div[@class='container']/div/h3").text
        state = 9
    else:
        driver.back()
        state = 8
        
    return state

def arreglar_datos_rues(solicitudes):
    
    ruta_escritura = os.path.join(ruta_salida,"Archivos_planos_WS")
       
    if not os.path.exists(ruta_escritura):
        os.mkdir(ruta_escritura)
        
    datos = pd.read_excel(solicitudes)
    datos = pd.DataFrame(datos)
    datos = datos.drop(columns = ['ITEM','SEGUNDO NOMBRE', 'PRIMER APELLIDO', 'SEGUNDO APELLIDO', 'ACTUACION','PRIMER NOMBRE/RAZON SOCIAL'])
    datos.columns = ['DOC']
    datos["DOC"] = datos["DOC"].str.upper()
    datos["DOC"] = datos["DOC"].replace(" ","")
    for dato in datos["DOC"].index.values.tolist():
        print(dato)
        if datos["DOC"][dato].isnumeric() != True:
            if datos["DOC"][dato][datos["DOC"][dato].find('-') + 1].isnumeric() != True:
                datos["DOC"][dato] = datos["DOC"][dato][:len(datos["DOC"][dato])-1]
            else:
                datos["DOC"][dato] = datos["DOC"][dato][:len(datos["DOC"][dato])-2]
        try:
            [(m.start(0), m.end(0)) for m in re.finditer('\D',datos["DOC"][dato])][2]
            datos["DOC"][dato] = datos["DOC"][dato][:[(m.start(0), m.end(0)) for m in re.finditer('\D',datos["DOC"][dato])][2][0]]
        finally:
            continue
            
            
    datos["DOC"] = datos["DOC"].str.replace("\D","", regex = True)
    #datos = datos.duplicated("DOC")
    
    datos['DOC'].to_csv(os.path.join(ruta_escritura,'consulta_RUES.txt'),mode = 'a',index=False,header = None, sep = ';')#f_{nom_solicitud} , quoting=csv.QUOTE_NONE

def arreglar_datos_supernot(solicitudes):
            
    #nom_solicitud = os.path.basename(solicitud)
    #solicitud = solicitud.replace('\\\\','\\')
    #print(nom_solicitud)
    ruta_escritura = os.path.join(ruta_salida,"Archivos_planos_WS")
    #print(ruta_escritura)        
    if not os.path.exists(ruta_escritura):
        os.mkdir(ruta_escritura)
        
    #archivos = glob(os.path.join(solicitud))
    #print(archivos)
    datos = pd.read_excel(solicitudes)
    #print(datos.info())
    #datos = datos[~datos[datos.columns[0]].isna()].reset_index(drop=True)
    #print(datos)
    datos = pd.DataFrame(datos)
    datos = datos.drop(columns = ['ITEM','SEGUNDO NOMBRE', 'PRIMER APELLIDO', 'SEGUNDO APELLIDO', 'ACTUACION','PRIMER NOMBRE/RAZON SOCIAL'])
    datos.columns = ['DOC']
    datos["DOC"] = datos["DOC"].str.upper()
    datos["DOC"] = datos["DOC"].replace(" ","")
    for dato in datos["DOC"].index.values.tolist():
        print(dato)
        if ((not "N," in datos["DOC"][dato]) and (not "C," in datos["DOC"][dato])) == True:
            #print("FLAG 1")
            if len(datos["DOC"][dato]) == 9 and (datos["DOC"][dato] == 8 or datos["DOC"][dato] == 9):
                datos["DOC"][dato] = "N," + datos["DOC"][dato]
            elif datos["DOC"][dato][1] != ",":
                datos["DOC"][dato][1] = ","
                datos["DOC"][dato] = "N," + datos["DOC"][dato]
            elif "-" in datos["DOC"][dato]:
                if datos["DOC"][dato][datos["DOC"][dato].find('-') + 1].isnumeric() != True:
                    datos["DOC"][dato] = datos["DOC"][dato][:len(datos["DOC"][dato])-1]
                else:
                    datos["DOC"][dato] = datos["DOC"][dato][:len(datos["DOC"][dato])-2]
                datos["DOC"][dato] = "N," + datos["DOC"][dato]
        elif "-" in datos["DOC"][dato]:
            if datos["DOC"][dato][datos["DOC"][dato].find('-') + 1].isnumeric() != True:
                datos["DOC"][dato] = datos["DOC"][dato][:len(datos["DOC"][dato])-1]
            else:
                datos["DOC"][dato] = datos["DOC"][dato][:len(datos["DOC"][dato])-2]
        elif datos["DOC"][dato][1] != ",":
            datos["DOC"][dato][1] = ","
        try:
            [(m.start(0), m.end(0)) for m in re.finditer('\D',datos["DOC"][dato])][2]
            datos["DOC"][dato] = datos["DOC"][dato][:[(m.start(0), m.end(0)) for m in re.finditer('\D',datos["DOC"][dato])][2][0]]
        finally:
            continue
                        
    datos['DOC'].to_csv(os.path.join(ruta_escritura,'consulta_Supernot.txt'),mode = 'a',index=False,header = None, sep = ';')#f_{nom_solicitud} , quoting=csv.QUOTE_NONE

def arreglar_datos_catastro(solicitudes):
            
    ruta_escritura = os.path.join(ruta_salida,"Archivos_planos_WS")        
    if not os.path.exists(ruta_escritura):
        os.mkdir(ruta_escritura)

    datos = pd.read_excel(solicitudes)
    datos = pd.DataFrame(datos)
    datos = datos.drop(columns = ['ITEM','SEGUNDO NOMBRE', 'PRIMER APELLIDO', 'SEGUNDO APELLIDO', 'ACTUACION','PRIMER NOMBRE/RAZON SOCIAL'])
    datos.columns = ['DOC']
    datos["DOC"] = datos["DOC"].str.upper()
    datos["DOC"] = datos["DOC"].replace(" ","")
    for dato in datos["DOC"].index.values.tolist():
        #print(dato)
        if ((not "N," in datos["DOC"][dato]) and (not "C," in datos["DOC"][dato])) == True:
            if len(datos["DOC"][dato]) == 9 and (datos["DOC"][dato] == 8 or datos["DOC"][dato] == 9):
                datos["DOC"][dato] = "N," + datos["DOC"][dato]
            elif datos["DOC"][dato][1] != ",":
                datos["DOC"][dato][1] = ","
                datos["DOC"][dato] = "N," + datos["DOC"][dato]
            elif "-" in datos["DOC"][dato]:
                if datos["DOC"][dato][datos["DOC"][dato].find('-') + 1].isnumeric() != True:
                    datos["DOC"][dato] = datos["DOC"][dato][:len(datos["DOC"][dato])-1]
                else:
                    datos["DOC"][dato] = datos["DOC"][dato][:len(datos["DOC"][dato])-2]
                datos["DOC"][dato] = "N," + datos["DOC"][dato]
        elif "-" in datos["DOC"][dato]:
            if datos["DOC"][dato][datos["DOC"][dato].find('-') + 1].isnumeric() != True:
                datos["DOC"][dato] = datos["DOC"][dato][:len(datos["DOC"][dato])-1]
            else:
                datos["DOC"][dato] = datos["DOC"][dato][:len(datos["DOC"][dato])-2]
        elif datos["DOC"][dato][1] != ",":
            datos["DOC"][dato][1] = ","
        try:
            [(m.start(0), m.end(0)) for m in re.finditer('\D',datos["DOC"][dato])][2]
            datos["DOC"][dato] = datos["DOC"][dato][:[(m.start(0), m.end(0)) for m in re.finditer('\D',datos["DOC"][dato])][2][0]]
        finally:
            continue
            
        
    datos['DOC'].to_csv(os.path.join(ruta_escritura,'consulta_Catastro.txt'),mode = 'a',index=False,header = None, sep = ';')#f_{nom_solicitud} , quoting=csv.QUOTE_NONE
    return ruta_escritura

def create_yaml():
    #Crea el archivo con las credenciales para el Web Service
    data = dict(
        wsdiari_user = dict(
            UserName = getpass("User: "),
            Password = getpass("Password: "),
        )
    )
    with open(r'C:\Users\danielf.moreno\Documents\Python Scripts\Supernot Auto\data.yml', 'w') as outfile:
        yaml.dump(data, outfile, default_flow_style=False)

def login(url,usernameId, username, passwordId, password, submit_buttonId):
   driver.get(url)
   driver.find_element(By.ID,usernameId).send_keys(username)
   driver.find_element(By.ID,passwordId).send_keys(password)
   driver.find_element(By.CLASS_NAME,submit_buttonId).click()


Procedure(state_driver())



driver = webdriver.Chrome(r'C:\Users\danielf.moreno\Documents\Python Scripts\Supernot Auto\chromedriver.exe')





success_message = "Su consulta en WS Supernotariado Consulta Identificaciones, se esta procesando con el ID"
succes =  driver.find_elements(By.CLASS_NAME, "container")
print(succes)
if any(success_message in e.text for e in succes):
    print("Archivo subido")
else:
    driver.back()