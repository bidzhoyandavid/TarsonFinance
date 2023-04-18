# -*- coding: utf-8 -*-
"""
Created on Thu Apr  6 15:00:12 2023

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


stock_client = StockHistoricalDataClient(api_key = AlpacaMarket.API_KEY
                                         , secret_key= AlpacaMarket.SECRET_KEY)


# =============================================================================
# Alpaca part
# =============================================================================

alpaca_search = GetAssetsRequest(asset_class=AssetClass.US_EQUITY)
alpaca_assets = trading_client.get_all_assets(alpaca_search)

alpaca_symbols = pd.DataFrame()

for asset in alpaca_assets:
    data = pd.DataFrame({
        'symbol': [asset.symbol]
        , 'status': [asset.status.name]
        , 'name': [asset.name]        
        })    
    alpaca_symbols = pd.concat([alpaca_symbols, data])
    
    


def getStockBars(symbol_or_symbols: Union[str, list[str]]                 
                 , timeframe: TimeFrame = TimeFrame.Day
                 , start: Optional[Union[pd.to_datetime, str]] = None
                 , end: Optional[Union[pd.to_datetime, str]] = None
                 , limit: int = None
                 , adjustment: Adjustment = None
                 , feed: DataFeed = None):
    
    request = StockBarsRequest(symbol_or_symbols = symbol_or_symbols
                               , timeframe = timeframe
                               , start = start
                               , end = end
                               , limit = limit
                               , adjustment = adjustment
                               , feed = feed)
    
    data = stock_client.get_stock_bars(request)
    data = data.df.reset_index()
    
    final = pd.DataFrame()
    final = pd.concat([final, data[data['symbol'] == symbol_or_symbols[0]]], axis = 1)
    final = final[['timestamp', 'close']]
    final.columns = ['date', symbol_or_symbols[0]]
    
    if len(symbol_or_symbols) != 1:  
        for item in range(1, len(symbol_or_symbols)):
            temp = data[data['symbol'] == symbol_or_symbols[item]]
            temp = temp[['timestamp', 'close']]
            temp.columns  = ['date', symbol_or_symbols[item]]
            final = pd.merge(final, temp, how ='inner', left_on = 'date', right_on='date')    
    
    return final




request = StockBarsRequest(symbol_or_symbols=['FSBC'], timeframe = TimeFrame.Day)
data = stock_client.get_stock_bars(request).df.reset_index()






    
# =============================================================================
# alpha vantage part
# =============================================================================


session = re.Session()

def alpha_companies(status = 'active'):
    import csv
    query = {
        'function': 'LISTING_STATUS'
        , 'state': status
        , 'apikey': AlphaVantage.API_KEY
        }
    
    request = session.get(AlphaVantage.BASE_URL, params = query)
    
    if request.status_code != 200:
        return print('The code is {}'.format(request.status_code))
    else:
        with re.Session() as s:
            download = s.get(request.url)
            decoded_content = download.content.decode('utf-8')
            cr = csv.reader(decoded_content.splitlines(), delimiter=',')
            my_list = list(cr)
        
        data = pd.DataFrame(my_list, columns = my_list[0]).drop(index=[0])
        return data


def replace_str_index(text,index=0,replacement=''):
    return '%s%s%s'%(text[:index],replacement,text[index+1:])

def alpha_symbol_convert(symbol):
    index = 0
    dash_count = 0
    for element in symbol:        
        if element == '-':
            dash_count += 1
            if dash_count == 2:
                symbol = replace_str_index(symbol, index, 'R')
            else:
                symbol = replace_str_index(symbol, index, '.')
        index += 1
    return symbol




alpha_active = alpha_companies('active')
alpha_active['symbol_convert'] = alpha_active['symbol'].apply(lambda x: alpha_symbol_convert(x))

alpha_delist = alpha_companies('delisted')
alpha_delist['symbol_convert'] = alpha_delist['symbol'].apply(lambda x: alpha_symbol_convert(x))



alpha_all = pd.concat([alpha_active, alpha_delist])


# =============================================================================
# compare
# =============================================================================


cm_coincided = pd.merge(alpaca_symbols, alpha_all, left_on = 'symbol', right_on='symbol', how = 'inner')
cm_coincided.to_excel('coincided.xlsx')


t_norw = alpha_all[alpha_all['symbol'] == 'NORW']

cm_coincided_active = pd.merge(alpaca_symbols[alpaca_symbols['status'] == 'ACTIVE']
                               , alpha_active
                               , left_on='symbol'
                               , right_on='symbol_convert'
                               )

cm_coincided_active.to_excel('cm_coinc_active.xlsx')


cm_coincided_delist = pd.merge(alpaca_symbols[alpaca_symbols['status'] == 'INACTIVE']
                               , alpha_delist
                               , left_on='symbol'
                               , right_on='symbol_convert'
                               )
cm_coincided_delist.to_excel('cm_coinc_delist.xlsx')


cm_didnt_match =  alpaca_symbols[alpaca_symbols['status'] == 'ACTIVE'].merge(alpha_active
                                                                             , how = 'outer'
                                                                             , indicator=True).query('_merge == "left_only"').drop('_merge', 1)



# =============================================================================
# alphavantage company overview
# =============================================================================

def alphaCompanyOverview(symbol):
    query = {
            'function': 'OVERVIEW'
            , 'symbol': symbol       
            , 'datatype': 'json'
            , 'apikey': AlphaVantage.API_KEY
        }
    s = re.Session()

    response = s.get(AlphaVantage.BASE_URL, params = query)
    
    if response.status_code != 200:
        return print('The status code is {}'.format(response.status_code))
    else:
        df_json = response.json()
        data = pd.DataFrame.from_dict([df_json]).T   
        return data
    
alpha_comp = alphaCompanyOverview('BABA')



def alphaIncomeStatement(symbol):
    query = {
            'function': 'INCOME_STATEMENT'
            , 'symbol': symbol
            , 'api_key': AlphaVantage.API_KEY        
        }
    s = re.Session()
    
    response = s.get(AlphaVantage.BASE_URL, params = query)
    
    if response.status_code != 200:
        return print('The status code is {}'.format(response.status_code))
    else:
        df_json = response.json()
        data = pd.DataFrame.from_dict([df_json]).T   
        return data


alpha_income = alphaIncomeStatement('MSFT')

def alphaBalanceSheet(symbol):
    query = {
            'function': 'BALANCE_SHEET'
            , 'symbol': symbol
            , 'api_key': AlphaVantage.API_KEY        
        }
    s = re.Session()
    
    response = s.get(AlphaVantage.BASE_URL, params = query)
    
    if response.status_code != 200:
        return print('The status code is {}'.format(response.status_code))
    else:
        df_json = response.json()
        data = pd.DataFrame.from_dict([df_json]).T   
        return data    


def alphaCashFlow(symbol):
    query = {
            'function': 'CASH_FLOW'
            , 'symbol': symbol
            , 'api_key': AlphaVantage.API_KEY        
        }
    s = re.Session()
    
    response = s.get(AlphaVantage.BASE_URL, params = query)
    
    if response.status_code != 200:
        return print('The status code is {}'.format(response.status_code))
    else:
        df_json = response.json()
        data = pd.DataFrame.from_dict([df_json]).T   
        return data  


def alphaEarnings(symbol):
    query = {
            'function': 'EARNINGS'
            , 'symbol': symbol
            , 'api_key': AlphaVantage.API_KEY        
        }
    s = re.Session()
    
    response = s.get(AlphaVantage.BASE_URL, params = query)
    
    if response.status_code != 200:
        return print('The status code is {}'.format(response.status_code))
    else:
        df_json = response.json()
        data = pd.DataFrame.from_dict([df_json]).T   
        return data  



earnings = alphaEarnings('AAPL')

def alphaNewsSentiment(symbols, topics, time_from, time_to, limit):
    query = {
            'function': 'NEWS_SENTIMENT'
            , 'tickers': symbols
            , 'topics': topics
            , 'time_from': time_from
            , 'time_to': time_to
            , 'api_key': AlphaVantage.API_KEY
        }

    s = re.Session()
    
    response = s.get(AlphaVantage.BASE_URL, params = query)
    
    data = response.json()
    





