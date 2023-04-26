# -*- coding: utf-8 -*-
"""
Created on Mon Apr 10 17:46:43 2023

@author: bidzh
"""

from config.config import *
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetClass
from alpaca.data.timeframe import TimeFrame
import pandas as pd
from alpaca.data.enums import Adjustment, DataFeed
from alpaca.data.requests import StockBarsRequest, StockQuotesRequest, StockQuotesRequest
from typing import Optional, Union
from uuid import UUID
from alpaca.data.historical.stock import StockHistoricalDataClient
import requests as re
import numpy as np
from datetime import datetime 
from tqdm import tqdm


alpaca_search = GetAssetsRequest(asset_class=AssetClass.US_EQUITY)
alpaca_assets = trading_client.get_all_assets(alpaca_search)

alpaca_symbols = pd.DataFrame()

for asset in alpaca_assets:
    if asset.status.name == 'ACTIVE':
        data = pd.DataFrame({
            'symbol': [asset.symbol]
            , 'status': [asset.status.name]
            , 'name': [asset.name]  
            , 'easy_to_borrow': [asset.easy_to_borrow]
            , 'exchange': [asset.exchange]
            , 'fractionable': [asset.fractionable]
            , 'uuid': [str(asset.id)]
            , 'maintenance_margin_requirement': [asset.maintenance_margin_requirement]
            , 'marginable': [asset.marginable]
            , 'min_size_order': [asset.min_order_size]
            , 'min_trade_increment': [asset.min_trade_increment]
            , 'price_increment': [asset.price_increment]
            , 'shortable': [asset.shortable]
            , 'tradable': [asset.tradable]
            })    
        alpaca_symbols = pd.concat([alpaca_symbols, data])

alpaca_symbols['cik'] = np.nan
alpaca_symbols['sector'] = np.nan
alpaca_symbols['industry'] = np.nan
alpaca_symbols['date_upload'] = datetime.today().strftime(format = '%Y-%m-%d')
alpaca_symbols['date_update'] = datetime.today().strftime(format = '%Y-%m-%d')

alpaca_symbols.to_sql('investment_company', schema = 'public', if_exists='append', index = False, con = engine)


db_alpaca_symbols = pd.read_sql("select id, symbol from public.investment_company where status = 'ACTIVE'", con = engine)

def getStock(symbol, date_start, date_end):
    request = StockBarsRequest(symbol_or_symbols = symbol
                               , timeframe = TimeFrame.Day
                               , start = date_start #pd.to_datetime('2015-01-01')
                               , end = date_end # pd.to_datetime('2023-04-10')
                               , limit = None
                               , adjustment = None
                               , feed = None)
    
    data = stock_client.get_stock_bars(request)
    data1 = data.df.reset_index()
    
    return data1

error = []
for ind in tqdm(db_alpaca_symbols.index):    
    try:    
        data = getStock(db_alpaca_symbols.loc[ind, 'symbol']
                        , date_start = pd.to_datetime('2015-01-01')
                        , date_end = pd.to_datetime('2023-04-10'))
        data = data.drop(columns = 'symbol')
        data['stock_id'] = db_alpaca_symbols.loc[ind, 'id']
        data = data.rename(columns = {'timestamp': 'date_at'})
        data.to_sql('investment_stockprices', schema = 'public', if_exists='append', index = False, con = engine)
    except:
        error.append(db_alpaca_symbols.loc[ind, 'symbol'])





