#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 29 17:49:28 2023

@author: david
"""

from config.config import *
from alpha_vantage.download import *
from risk_analytics.var_metrics import *
import pandas as pd
from tqdm import tqdm

symbols = pd.read_sql(
        """
            select id, symbol from public.investment_company
            where
                1=1
                and status = 'Active'
                and date_upload = '2023-05-11'
        """
        , con = engine
    )

non_252_days = []

for i in tqdm(symbols.index):
    symbol = symbols.loc[i, 'symbol']
    symbol_id = symbols.loc[i, 'id']
    
    data = pd.read_sql(
            """
            select 
                date_at
                , stock_id
                , close
            from public.investment_stockprices
            where
                1=1
                and period_id = 1
                and stock_id = {}
            order by date_at 
            """.format(symbol_id)
            , con = engine
        )
    
    if len(data) <=253:
        non_252_days.append(symbol)
        continue
    else:
        data = data.tail(757)
    
    try:
        beta = pd.read_sql(
                """
                    select beta from public.investment_companyratios
                    where
                        1=1
                        and name_id = {}
                """.format(symbol_id)
                , con = engine
            ).loc[0, 'beta']
    except:
        beta = None
    


    
    data['log_return'] = np.log(data['close']) - np.log(data['close'].shift(1))
    
    riskData = FillVaR(Data = data, Beta = beta)
    riskData['stock_id'] = symbol_id
    riskData.to_sql('investment_stockrisk', schema = 'public', if_exists = 'append', index = False, con = engine)
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
