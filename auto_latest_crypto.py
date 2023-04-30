#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 26 23:11:58 2023

@author: david
"""


from config.config import *
from alpha_vantage.download import *
from alpha_vantage.transform import *
from alpha_vantage.upload import *
from tqdm import tqdm
from datetime import datetime


cryptos = pd.read_sql(
        """
            select id,  currency_code from public.investment_digitalcurrency
        """
        , con = engine
    )

non_200_status = []
empty_data = []   
non_downloaded = []
complete = 'latest'

for i in tqdm(cryptos.index):
    crypto = cryptos.loc[i, 'currency_code']
    crypto_id = cryptos.loc[i, 'id']
    
    try:
        data, data_code = alphaCrypto(symbol = crypto, period = 'daily', outputsize='full', datatype='csv', market = 'CNY')
    except:
        non_downloaded.append(crypto)
        continue
        
    
    if data_code != 200:
        non_200_status.append(crypto)
        continue
    
    if len(data) <=1:
        empty_data.append(crypto)
        continue

 
    data_transformed = transformCrypto(data, crypto_id = crypto_id) 
    
    if complete == 'full':
        data_transformed = data_transformed
    else:
        data_transformed = data_transformed[:1]
        
    data_transformed.to_sql('investment_cryptoprice', schema = 'public', if_exists='append', index = False, con = engine)


today = datetime.today().strftime('%Y-%m-%d')
print('Data for {} was uploaded'.format(today))

# non_200, empty_data = uploadCryptoPrice(cryptos = cryptos, complete='full')

# while len(non_200) != 0:
#     cryptos = cryptos.merge(pd.DataFrame(non_200, columns = {'currency_doe'}), on = 'currency_code', how = 'inner')
#     non_200, empty_data = uploadCryptoPrice(cryptos = cryptos)
    
    
    
    
    
    
    
    

            