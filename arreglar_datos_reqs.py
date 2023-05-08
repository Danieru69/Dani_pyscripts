# =============================================================================
# Paquetes
# =============================================================================
import os, re
import pandas as pd
from glob import glob
from unidecode import unidecode


# =============================================================================
# Parametros
# =============================================================================
ruta_entrada = r'P:\SUPER_NOTARIADO\Solicitudes_originales'
ruta_salida = r'P:\SUPER_NOTARIADO\Solicitudes_arregladas'

solicitudes = glob(os.path.join(ruta_entrada,'*DFML'))

solicitudes_ya = glob(os.path.join(ruta_salida,'*DFML'))
solicitudes_ya = [os.path.basename(ele) for ele in solicitudes_ya]

solicitudes = [ele for ele in solicitudes if os.path.basename(ele) not in solicitudes_ya ]
#solicitudes_ = [os.path.basename(ele) for ele in solicitudes]


solicitud_1 = [glob(os.path.join(solicitud,'*.xlsx')) for solicitud in solicitudes]
# correr =  [ele for ele in solicitudes if os.path.base]
# solicitud = solicitudes[0]





for solicitud in solicitud_1[0]:
    

    nom_solicitud = os.path.basename(solicitud)
    nom_solicitud = nom_solicitud.replace('C','')
    nom_solicitud = nom_solicitud.replace('V','')
    nom_solicitud = nom_solicitud.replace('D','')
    nom_solicitud = nom_solicitud.replace('S','')
    nom_solicitud = nom_solicitud.replace(' ','')
    ruta_escritura = os.path.join(ruta_salida,nom_solicitud[:len(nom_solicitud)-5]+'DFML')
      
    if not os.path.exists(ruta_escritura):
        os.mkdir(ruta_escritura)
        
    datos = pd.read_excel(solicitud)
    datos = pd.DataFrame(datos)
    datos = datos[["IDENTIFICACIÓN"]]#datos.drop(columns = ['ITEM','SEGUNDO NOMBRE', 'PRIMER APELLIDO', 'SEGUNDO APELLIDO', 'ACTUACION','PRIMER NOMBRE/RAZON SOCIAL'])
    datos.columns = ['DOC']
    datos["DOC"] = datos["DOC"].str.upper()
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
       
    datos['DOC'].to_csv(os.path.join(ruta_escritura,nom_solicitud[:len(nom_solicitud)-6]+'.txt'),mode = 'a',index=False,header = None, sep = ';')#f_{nom_solicitud} , quoting=csv.QUOTE_NONE
