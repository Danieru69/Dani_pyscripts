# -*- coding: utf-8 -*-
"""
Created on Mon Dec 12 10:12:54 2022

@author: danielf.moreno
"""

from pathlib import Path
import pandas as pd

path = r'S:\2.DELEGADAS\UNCOPI\5103 - 5124\Consolidado\Desagregar\Consolidado_RUNT_07122022.xlsx'  # or unix / linux / mac path

# Get the files from the path provided in the OP
#files = Path(path).glob('*.xlsx')  # .rglob to get subdirectories

df = pd.read_excel(path, sheet_name=1)
"""
dfs = list()
for f in files:
    data = pd.read_excel(f)
    # .stem is method for pathlib objects to get the filename w/o the extension
    data['Archivo Origen'] = f.stem
    dfs.append(data)

df = pd.DataFrame.from_records([data.__dict__ for f in data])
"""

dfs = df["ARCHIVO ORIGEN"].unique()
series = pd.Series(dfs)
#print(df["ARCHIVO ORIGEN"].to_string)#.str.lower().str.replace('s/+',""))
#series = series.str.replace('s/+', "")
#df["ARCHIVO ORIGEN"] = df["ARCHIVO ORIGEN"].str.replace("c","").str.replace("v","").str.replace("d","").str.replace("s","")

#print(series)

for a in series:
    #df["ARCHIVO ORIGEN"] = df["ARCHIVO ORIGEN"].str.replace('s/+', "")
    #series = series.str.replace('s/+', "")
    if  df["ARCHIVO ORIGEN"].sort_index(inplace=True) == series.sort_index(inplace=True):
        temp_df = df.loc[(df["ARCHIVO ORIGEN"] == a)]
        temp_df["ARCHIVO ORIGEN"] = temp_df["ARCHIVO ORIGEN"].str.replace("C","").str.replace("V","").str.replace("D","").str.replace("S","")
        name = str(temp_df['ARCHIVO ORIGEN'].iloc[0])
        print(name)
        temp_df.to_excel(r'S:\2.DELEGADAS\UNCOPI\RESPUESTAS RUNT\Información respuesta\SOLRUNT 24 AL 27\\'+ name +'RUNT.xlsx' , index=False)
        

#for d in series:
    
        #if df["ARCHIVO ORIGEN"].to_string == d:
            #archivo = pd.DataFrame(df)
            #print(d)


#dfs.head()
#print(dfs)
#print(df.columns)
#print (df.head())
#print(df.index.get_level_values(0))




#df.index.get_level_values(0).to_excel(r'S:\2.DELEGADAS\UNCOPI\5103 - 5124\Consolidado\Desagregar\Prueba.xlsx', index=False)
