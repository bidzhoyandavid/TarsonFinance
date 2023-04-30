# -*- coding: utf-8 -*-
"""
Created on Sat Apr 15 12:44:29 2023

@author: bidzh
"""
import pandas as pd
import requests as re
import csv



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




def transformCrypto(data, crypto_id):
    """
        Transform crypto to the format to be uploaded
        Input:
            data: dataframe downloaded from Alpha Vantage
            crypto_id: id of crypto from the db
        Output:
            data_transformed: dataframe with prepared format to be uploaded
    
    """
    data = data.reset_index().rename(columns={'index':'date_at'})
    
    data_transformed = data[[ 'date_at'
                             , '1b. open (USD)'
                             , '2b. high (USD)'
                             , '3b. low (USD)'
                             , '4b. close (USD)'
                             , '5. volume'
                             , '6. market cap (USD)']].rename(columns = {'1b. open (USD)': 'open'
                                                                    , '2b. high (USD)': 'high'
                                                                    , '3b. low (USD)': 'low'
                                                                    , '4b. close (USD)': 'close'
                                                                    , '6. market cap (USD)': 'marketcap'
                                                                    , '5. volume': 'volume'})  
                                                                      
    for col in ['low', 'close', 'high', 'open', 'volume', 'marketcap']:
        data_transformed[col] = pd.to_numeric(data_transformed[col])
    
    data_transformed['date_at'] = pd.to_datetime(data_transformed['date_at'])
    data_transformed['digital_id'] = crypto_id
    data_transformed['market_id'] = 143
    data_transformed['period_id'] = 1
    
    return data_transformed


def transformLatest(data, symbol_id):
    """
        Transforms the latest data into format to be uploaded to database
        Input:
            data: a dataframe with data of latest day
            symbol_id: the ID of selected symbol
        Output:
            data: transformed data to be uploaded to database
    """
    
    
    data = data[['open', 'high', 'low', 'price', 'volume', 'latestDay', 'symbol']]
    data = data.rename(columns = {'price': 'close'
                                  , 'latestDay': 'date_at'
                                  , 'symbol': 'stock_id'})
    data['stock_id'] = symbol_id
    data['period_id'] = 1
    data['date_at'] = pd.to_datetime(data['date_at'])
    
    for i in ['open', 'high', 'low', 'close', 'volume']:
        data[i] = pd.to_numeric(data[i])

    return data    


def alphaTransformStockPrices(data, stock_id, period_id = 1, date = None):
    
    data = data.rename(columns = {'timestamp': 'date_at'})
    for col in ['open', 'close', 'high', 'low', 'volume']:
        data[col] = pd.to_numeric(data[col])
        
    
    data['date_at'] = pd.to_datetime(data['date_at'])
    if date is not None:
        data = data[data['date_at'] > date]
    data['symbol_id'] = symbol_id
    data['period_id'] = period_id
    
    
    return data
