# -*- coding: utf-8 -*-
"""
Created on Tue Jan 24 11:34:27 2023

@author: danielf.moreno
"""

import re

def get_conrtact(texto):
    patron = re.findall(r'(CO1.BDOS.\d{,7}|CO1.PCCNTR.\d{,7}|CO1.REQ.\d{,7})',texto)
    #print(patron)
    return patron
   
#get_conrtact('Los contratos CO1.PCCNTR.1369718, CO1.BDOS.1107643 y CO1.REQ.3154543 fueron alertados por la contraloría')
