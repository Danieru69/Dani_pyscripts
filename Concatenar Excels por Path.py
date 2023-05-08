# -*- coding: utf-8 -*-
"""
Created on Fri Dec  2 10:21:15 2022

@author: danielf.moreno
"""

from pathlib import Path
import pandas as pd

path = r'S:\3.GERENCIAS_DEP\RISARALDA\6464_2023IE0027681\ArchivosOrigen\2022'  # or unix / linux / mac path

# Get the files from the path provided in the OP
files = Path(path).glob('*.txt')  # .rglob to get subdirectories

dfs = list()
for f in files:
    data = pd.read_csv(f, delimiter=',',header=None, encoding='latin-1')
    data.columns
    data = data[[1,2,3,4,5,6]]
    data.columns = ['TIPO_DOC','DOC','1APELLIDO','2APELLIDO','1NOMBRE','2NOMBRE']
    # .stem is method for pathlib objects to get the filename w/o the extension
    data['Archivo Origen'] = f.stem
    dfs.append(data)

df = pd.concat(dfs, ignore_index=True)
#df["IDENTIFICACIÓN"] = df["IDENTIFICACIÓN"].str.replace("C,","")
#df["IDENTIFICACIÓN"] = df["IDENTIFICACIÓN"].str.replace("N,","")
#df = df.drop(['ITEM'], axis=1)

"""
df.head()
print(df.columns)
print(df)
"""

df.to_csv(r'S:\3.GERENCIAS_DEP\RISARALDA\6464_2023IE0027681\ArchivosOrigen\2022\consolidado.csv', index=False)