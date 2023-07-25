# -*- coding: utf-8 -*-
"""
Created on Thu Apr 13 10:55:58 2023

@author: bidzh
"""

from config.config import *
import pandas as pd
import numpy as np
import csv
import requests as re
from pandas import json_normalize
from alpha_vantage import transform

session = re.Session()

def outputData(inputs, request):
    """
        Converts the input data format into pandas dataframe
        Input:
            inputs: type of data - either csv or json
            request: request to the server
        output:
            data: pandas dataframe
    """
    if inputs == 'csv':
        with re.Session() as s:
            download = s.get(request.url)
            decoded_content = download.content.decode('utf-8')
            cr = csv.reader(decoded_content.splitlines(), delimiter=',')
            my_list = list(cr)
        
        data = pd.DataFrame(my_list, columns = my_list[0]).drop(index=[0])
        return data
    else:
        df_json = request.json()
        data = pd.DataFrame.from_dict([df_json]).T   
        return data

    
def uploadData(df, table_name, schema = 'public', connection = engine):
    """
        Uploads data into database
        Input:
            df: pandas dataframe to be uploaded
            table_name: table name in the database
            schema: name of schema in database
            connection: SQLAlchemy engine
        Output:
            data was uploaded or error message
    """
    try:
        return df.to_sql(table_name, schema = schema, if_exists = 'append', index = False, con = connection)
    except:
        return print('The data was not uploaded')
        
        
def alphaSelectCatalogue(table_name, connection = engine):
    try:
        df = pd.read_sql("select * from public.{}".format(table_name), con = connection)
        return  df
    except:
        return None
        
# =============================================================================
# Fundamental Data
# =============================================================================

def alphaListing(status = 'active'):
    """
        Returns the list of active symbols
    """
    query = {
        'function': 'LISTING_STATUS'
        , 'state': status
        , 'apikey': AlphaVantage.API_KEY
        }
    
    request = session.get(AlphaVantage.BASE_URL, params = query)
    
    if request.status_code != 200:
        return print('The code is {}'.format(request.status_code))
    else:
        return outputData(inputs='csv', request = request)

def alphaCompanyOverview(symbol):
    """
        Returns company overview by symbol
    """
    query = {
            'function': 'OVERVIEW'
            , 'symbol': symbol       
            , 'datatype': 'json'
            , 'apikey': AlphaVantage.API_KEY
        }
    
    request = session.get(AlphaVantage.BASE_URL, params = query)
    
    if request.status_code != 200:
        return print('The status code is {}'.format(request.status_code))
    else:
       return outputData(inputs = 'json', request = request)
        
def alphaIncomeStatement(symbol):
    """
        Returns Income statement for a chosen symbol
    """
    query = {
            'function': 'INCOME_STATEMENT'
            , 'symbol': symbol
            , 'apikey': AlphaVantage.API_KEY        
        }
    
    request = session.get(AlphaVantage.BASE_URL, params = query)

    if request.status_code != 200:
        data = {}
    else:
        data = request.json()
    
    return data, request.status_code

def alphaBalanceSheet(symbol):
    """
        Returns a balance sheet for a chosen symbol
    """
    query = {
            'function': 'BALANCE_SHEET'
            , 'symbol': symbol
            , 'apikey': AlphaVantage.API_KEY        
        }
    
    request = session.get(AlphaVantage.BASE_URL, params = query)
    
    if request.status_code != 200:
        data = {}
    else:
        data = request.json()
    
    return data, request.status_code

def alphaCashFlow(symbol):
    """
        Returns a CashFlow for a chosen symbol
    """
    query = {
            'function': 'CASH_FLOW'
            , 'symbol': symbol
            , 'apikey': AlphaVantage.API_KEY        
        }
    
    request = session.get(AlphaVantage.BASE_URL, params = query)
    
    if request.status_code != 200:
        data = {}
    else:
        data = request.json()
    
    return data, request.status_code

def alphaIPOCalendar():
    """
        Returns an IPO calenfar
    """
    query= {
            'funcion': 'IPO_CALENDAR'
            , 'apikey': AlphaVantage().API_KEY
        }
    
    request = session.get(AlphaVantage.BASE_URL, params = query)
    
    if request.status_code != 200:
        return print('The code is {}'.format(request.status_code))
    else:
        return outputData(inputs = 'csv', request = request) 
    
    
# =============================================================================
# Stock prices
# =============================================================================
    
    
def alphaStockPrices(symbol, period, outputsize, datatype, interval = None):
    """
        Returns stock prices by symbol
        Input:
            symbol: the stock ticker
            period: the priodic data, daily, monthly, intraday
            outputsize: either compact or full
            datatype: either json or csv
            interval: by default None, set some value only when period is intraday
        Outputs:
            pandas dataframe with stock prices
    """
    
    if period == 'daily':
        query = {
                'function': 'TIME_SERIES_DAILY'
                , 'outputsize': outputsize
                , 'symbol': symbol
                , 'datatype': datatype
                , 'apikey': AlphaVantage().API_KEY            
            }
    elif period == 'intraday':
        query = {
                'function': 'TIME_SERIES_INTRADAY'
                , 'symbol': symbol
                , 'interval': interval
                , 'adjusted': 'false'
                , 'datatype': datatype
                , 'apikey': AlphaVantage().API_KEY            
            }
    elif period == 'weekly':
        query = {
                'function': 'TIME_SERIES_WEEKLY'
                , 'symbol': symbol
                , 'datatype': datatype
                , 'apikey': AlphaVantage().API_KEY            
            }
    elif period == 'monthly':
        query = {
                'function': 'TIME_SERIES_MONTHLY'
                , 'symbol': symbol
                , 'datatype': datatype
                , 'apikey': AlphaVantage().API_KEY            
            }
    else:
        return None
    
    request = session.get(AlphaVantage().BASE_URL, params = query)
    
    if request.status_code != 200:
        data = {}
    else:
        data = outputData(inputs = 'csv', request = request)
    return data, request.status_code 
        

def alphaLatest(symbol, datatype):
    """
        Returns the latest qoutes for a chosen symbol
        Inputs:
            symbol: the schosen symbol
            datatype: either json or csv
        Output:
            pandas DataFrame with latest quotes
    """
    
    query = {
            'function': 'GLOBAL_QUOTE'
            , 'symbol': symbol
            , 'datatype': datatype
            , 'apikey': AlphaVantage().API_KEY
        }
    
    request = session.get(AlphaVantage().BASE_URL, params = query)
    
    if request.status_code != 200:
        data = {}
    else:
        data = outputData(inputs = 'csv', request = request)
    
    return data, request.status_code


# =============================================================================
# news and sentiment
# =============================================================================
    
def alphaNews(symbol
              , topics = None
              , time_from = None
              , time_to = None
              , sort = None
              , limit = 200):
    query = {
            'function': 'NEWS_SENTIMENT'
            , 'tickers': symbol
            , 'topics': topics
            , 'time_from': time_from
            , 'time_to': time_to
            , 'sort': sort
            , 'limit': limit
            , 'apikey': AlphaVantage().API_KEY
        }
    
    request = session.get(AlphaVantage().BASE_URL, params = query)
    
    if request.status_code != 200:
        data = {}
    else:
        response = outputData(inputs = 'json', request = request)
        data = json_normalize(response.loc['feed', 0])
        return data, request.status_code
# =============================================================================
# crypto
# =============================================================================
    
def alphaCrypto(symbol, period, outputsize, datatype, interval = None, market = 'USD'):
    """
        Returns a crypto time series to USD (by default)
        Inputs:
            symbol: cryptocurrency 
            period: intraday, daily, weekly, monthly
            outputsize: either full or compact
            datatype: either json or csv
            interval: set once period is intraday
            market: by default USD
        Output:
            pandas data frame with crypto time series to usd
    """
    if period == 'intraday':
        query = {
                'function': 'function'
                , 'symbol': symbol
                , 'market': market
                , 'interval': interval
                , 'outputsize': outputsize
                , 'datatype': datatype
                , 'apikey': AlphaVantage().API_KEY
            }
    elif period == 'daily':
        query = {
                'function': 'DIGITAL_CURRENCY_DAILY'
                , 'symbol': symbol
                , 'market': market
                , 'apikey': AlphaVantage().API_KEY
            }
    elif period == 'weekly':
        query = {
                'function': 'DIGITAL_CURRENCY_WEEKLY'
                , 'symbol': symbol
                , 'market': market
                , 'apikey': AlphaVantage().API_KEY
            }
    elif period == 'monthly':
        query = {
                'function': 'DIGITAL_CURRENCY_MONTHLY'
                , 'symbol': symbol
                , 'market': market
                , 'apikey': AlphaVantage().API_KEY
            }
    else:
        return None
    
    request = session.get(AlphaVantage().BASE_URL, params = query)
    
    if request.status_code != 200:
        data = {}
    else:
        response = request.json()['Time Series (Digital Currency Daily)']
        data = pd.DataFrame.from_dict(response).T     
        
    return data, request.status_code
   
    
# =============================================================================
# commodities
# =============================================================================

def alphaCommodities(function, datatype, interval = 'daily'):
    """
        Returns commodities qoutes
        Input:
            function: name of commodity (oil, gas, etc)
            datatype: either csv or json
            inteval: by default daily
        Output:
            time series of commodity qoutes
    """
    
    query = {
            'function': function
            , 'datatype': datatype
            , 'interval': interval
            , 'apikey': AlphaVantage().API_KEY
        }

    request = session.get(AlphaVantage().BASE_URL, params = query)

    if request.status_code != 200:
        return print('The status code is {}'.format(request.status_code))
    else:
        return outputData(inputs = 'csv', request = request)
    

# =============================================================================
# Economic indicators
# =============================================================================
    

def alphaEconomicIndicators(function, interval, datatype, maturity = '10year'):
    """
        Returns economic indicators
        Inputs:
            function: name of economic indicator
            interval: interval for the economic indicator
            datatype: either csv or json
            maturity: set once the function is TREASURY_YIELD
        Output:
            pandas data frame with economic indicator
    """
    
    if function == 'REAL_GDP':
        query = {
                'function': 'REAL_GDP'
                , 'interval': interval
                , 'datatype': datatype
                , 'apikey': AlphaVantage().API_KEY            
            }
        
    elif function == 'REAL_GDP_PER_CAPITA':
        query = {
                'function': 'REAL_GDP_PER_CAPITA'
                , 'datatype': datatype
                , 'apikey': AlphaVantage().API_KEY
            }
        
    elif function == 'TREASURY_YIELD':
        query = {
                'function': 'TREASURY_YIELD'
                , 'interval': interval
                , 'maturity': maturity
                , 'datatype': datatype
                , 'apikey': AlphaVantage().API_KEY
            }
  
    elif function == 'FEDERAL_FUNDS_RATE':
        query = {
                'function': 'FEDERAL_FUNDS_RATE'
                , 'interval': interval
                , 'datatype': datatype
                , 'apikey': AlphaVantage().API_KEY
            }   
        
    elif function == 'CPI':
       query = {
               'function': 'CPI'
               , 'interval': interval
               , 'datatype': datatype
               , 'apikey': AlphaVantage().API_KEY
           } 
        
    elif function == 'INFLATION':
         query = {
                 'function': 'INFLATION'
                 , 'datatype': datatype
                 , 'apikey': AlphaVantage().API_KEY
             } 
         
    elif function == 'RETAIL_SALES':
        query = {
                'function': 'RETAIL_SALES'
                , 'datatype': datatype
                , 'apikey': AlphaVantage().API_KEY
            }
        
    elif function == 'DURABLES':
        query = {
                'function': 'DURABLES'
                , 'datatype': datatype
                , 'apikey': AlphaVantage().API_KEY
            }
        
    elif function == 'UNEMPLOYMENT':
        query = {
                'function': 'UNEMPLOYMENT'
                , 'datatype': datatype
                , 'apikey': AlphaVantage().API_KEY
            }
        
    elif function == 'NONFARM_PAYROLL':
        query = {
                'function': 'NONFARM_PAYROLL'
                , 'datatype': datatype
                , 'apikey': AlphaVantage().API_KEY
            }
    else:
        return None
    
    request = session.get(AlphaVantage().BASE_URL, params = query)
    
    if request.status_code != 200:
        return print('The status code is {}'.format(request.status_code))
    else:
        return outputData(inputs = 'csv', request = request)    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
