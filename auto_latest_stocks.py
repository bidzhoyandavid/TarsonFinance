#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 26 18:07:46 2023

@author: david
"""

from config.config import *
from alpha_vantage.download import *
from alpha_vantage.transform import *
from alpha_vantage.upload import *
import time
from tqdm import tqdm
from datetime import datetime, date
import sys

today = date.today()

if datetime.now().isoweekday() in [7, 1]:
    sys.exit("The date {} is sunday or monday".format(today))


symbols = pd.read_sql(
        """
            select id, symbol from public.investment_company
            where
                1=1
                and status = 'Active'
        """
        , con = engine
    )

non_200, not_uploaded, non_downloaded, empty_data = uploadLatestStockPrice(symbols = symbols)

    
k = 0
while len(non_200) != 0 and k <= 3:
    k +=1
    symbols_non_200_status = symbols.merge(pd.DataFrame(non_200, columns = ['symbol']), on = 'symbol', how = 'inner')
    non_200, not_uploaded_1, non_downloaded_1, empty_data_1 = uploadLatestStockPrice(symbols = symbols_non_200_status)

k = 0
while len(not_uploaded) != 0 and k <= 3:
    k += 1
    symbols_not_uploaded = symbols.merge(pd.DataFrame(not_uploaded, columns = ['symbol']), on = 'symbol', how = 'inner')
    non_200_2, not_uploaded, non_downloaded_2, empty_data_2 = uploadLatestStockPrice(symbols = symbols_not_uploaded)


k = 0
while len(non_downloaded) !=0 and k <= 3:
    k += 1
    symbols_non_downloaded = symbols.merge(pd.DataFrame(non_downloaded, columns = ['symbol']), on = 'symbol', how = 'inner')
    non_200_3, nontuploaded_3, non_downloaded, empty_data_3 = uploadLatestStockPrice(symbols = symbols_non_downloaded)
        
       
k = 0
while len(empty_data) !=0 and k <=3:    
    k += 1
    symbols_empty_data = symbols.merge(pd.DataFrame(empty_data, columns = ['symbol']), on = 'symbol', how = 'inner')
    non_200_4, nontuploaded_4, non_downloaded_4, empty_data = uploadLatestStockPrice(symbols = symbols_empty_data)


print("""
        """)
print('The Data for {} was uploaded'.format(today))
