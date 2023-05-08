# -*- coding: utf-8 -*-
"""
Created on Thu Dec 15 07:27:27 2022

@author: danielf.moreno
"""
from pathlib import Path
import pandas as pd
import os

path = r'S:\2.DELEGADAS\UNCOPI\RESPUESTAS RUNTt\Originales\SOLRUNT040_01032023'  # or unix / linux / mac path

# Get the files from the path provided in the OP
files = Path(path).glob('*.xlsx')  # .rglob to get subdirectories

dfs = list()
for f in files:
    data = pd.read_excel(f)
    data["IDENTIFICACIÓN"] = data["IDENTIFICACIÓN"].str.replace("C,","")
    data["IDENTIFICACIÓN"] = data["IDENTIFICACIÓN"].str.replace("N,","")
    data['IDENTIFICACIÓN']=data['IDENTIFICACIÓN'].astype(float)
    # .stem is method for pathlib objects to get the filename w/o the extension
    #data['Archivo Origen'] = f.stem.split("_")[0]
    #dfs.append(data)
    data = data.drop(['ITEM'], axis=1)
    print (os.path.basename(f).split("_")[0])
    print (data)
    with pd.ExcelWriter(r'S:\2.DELEGADAS\UNCOPI\RESPUESTAS RUNTt\Originales\01032023_DET-01-PR-009- FR-001.xlsx', engine='openpyxl', mode='a') as writer:  
        data.to_excel(writer, sheet_name= os.path.basename(f).split("_")[0], index=False)
    
    

#df = pd.concat(dfs, ignore_index=True)


"""
df.head()
print(df.columns)
print(df)
"""

