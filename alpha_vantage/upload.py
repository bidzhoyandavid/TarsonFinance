#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 25 11:16:06 2023

@author: david
"""

from config.config import *
from alpha_vantage.download import *
from alpha_vantage.transform import *
from alpha_vantage.get_functions import *
import pandas as pd
from datetime import datetime, timedelta, date
import time



def uploadListingCompanies(merged):

    # common info
    investment_company = merged[['Name', 'symbol', 'CIK', 'Sector', 'status_x', 'Industry', 'ipoDate', 'exchange', 'AssetType']]
    investment_company['date_upload'] = date.today()
    investment_company['date_update'] = date.today()
    investment_company['sector_id'] = investment_company['Sector'].apply(lambda x: getIDSector(sector_name=x))
    investment_company['industry_id'] = investment_company['Industry'].apply(lambda x: getIDIndustry(industry_name=x))
    investment_company['exchange_id'] = investment_company['exchange'].apply(lambda x: getIDExchange(x))
    investment_company['security_type_id'] = investment_company['AssetType'].apply(lambda x: getIDSecurityType(x))
    investment_company = investment_company[['Name', 'symbol', 'status_x', 'CIK', 'sector_id', 'industry_id', 'date_upload', 'date_update', 'ipoDate', 'exchange_id', 'security_type_id']]
    investment_company = investment_company.rename(columns = {'Name': 'name'
                                                              , 'CIK': 'cik'
                                                              , 'ipoDate': 'ipo_date'
                                                              , 'status_x': 'status'})
    investment_company['ipo_date'] = pd.to_datetime(investment_company['ipo_date'])
    
    
    return investment_company

def uploadNewCompanyOverview(merged):        
    # companyratios
    investment_companyratios = merged[['LatestQuarter', 'MarketCapitalization', 'EBITDA', 'PERatio', 'PEGRatio', 'BookValue', 'DividendPerShare', 'DividendYield'
                                       , 'EPS', 'RevenuePerShareTTM', 'ProfitMargin', 'OperatingMarginTTM', 'ReturnOnAssetsTTM', 'ReturnOnEquityTTM', 'RevenueTTM'
                                       , 'GrossProfitTTM', 'DilutedEPSTTM', 'TrailingPE', 'ForwardPE', 'PriceToSalesRatioTTM', 'PriceToBookRatio', 'EVToRevenue'
                                       , 'EVToEBITDA', 'Beta', 'SharesOutstanding', 'symbol']]
    
    
    investment_companyratios['name_id'] = investment_companyratios['symbol'].apply(lambda x: getIDSymbol(x))
    
    investment_companyratios = investment_companyratios.sort_index(axis = 1)
    
    investment_companyratios = investment_companyratios.rename(columns = {
            'Beta': 'beta'
            , 'BookValue': 'book_value'
            , 'DilutedEPSTTM': 'diluted_eps_ttm'
            , 'DividendPerShare': 'dividend_per_share'
            , 'DividendYield': 'dividend_yield'
            , 'EBITDA': 'ebitda'
            , 'EPS': 'eps'
            , 'EVToEBITDA': 'ev2ebitda'
            , 'EVToRevenue': 'ev2revenue'
            , 'ForwardPE': 'forward_pe'
            , 'GrossProfitTTM': 'gross_profit_ttm'
            , 'LatestQuarter': 'date_at_findata'
            , 'MarketCapitalization': 'market_cap'
            , 'OperatingMarginTTM': 'operating_margin_ttm'
            , 'PERatio': 'pe_ratio'
            , 'PEGRatio': 'peg_ratio'
            , 'PriceToBookRatio': 'price2book_ratio'
            , 'PriceToSalesRatioTTM': 'price2sales_ratio'
            , 'ProfitMargin': 'profit_margin_ttm'
            , 'ReturnOnAssetsTTM': 'roa_ttm'
            , 'ReturnOnEquityTTM': 'roe_ttm'
            , 'RevenuePerShareTTM': 'revenue_per_share'
            , 'RevenueTTM': 'revenue_ttm'
            , 'SharesOutstanding': 'shares_outstanding'
            , 'TrailingPE': 'trailing_pe'                                                                          
        })
    
    investment_companyratios = investment_companyratios.drop(columns = ['symbol'])
    investment_companyratios = investment_companyratios.replace('-', None, inplace = False)
    investment_companyratios = investment_companyratios.replace('None', None, inplace = False)
    
    
    return investment_companyratios


    
def uploadCryptoPrice(cryptos, complete):
    """
        Uploades crypto prices to db
        Input:
            cryptos: dataframe from db with the list of cryptocurrency
            complete: either 'full' or 'latest'
        Output:
            non_200_status: list of cryptos with non 200 status
            empty_data: list of cryptos with empty data
    """
        
    non_200_status = []
    empty_data = []   

    for i in cryptos.index:
        crypto = cryptos.loc[i, 'currency_code']
        crypto_id = cryptos.loc[i, 'id']
        
        data, data_code = alphaCrypto(symbol = crypto, period = 'daily', outputsize='full', datatype='csv', market = 'CNY')
            
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

    return non_200_status, empty_data
    
def uploadLatestStockPrice(symbols):
    """
        Uploades data of latest day into db
        Input:
            symbols: a dataframe with symbols and their IDs
        Output:
            non_200_status: symbols that returned non 200 status code
            not_uploaded: dict of dataframes that were not uploaded
    """
    non_200_status = []
    not_uploaded = {}
    non_downloaded = []
    empty_data = []
    
    for i in symbols.index:
        symbol = symbols.loc[i, 'symbol']
        symbol_id = symbols.loc[i, 'id']
        time.sleep(1)
        try:
            latest, code = alphaLatest(symbol=symbol, datatype = 'csv')
        except:
            non_downloaded.append(symbol)
            
        
        if len(latest) == 0:
            empty_data.append(symbol)
            continue
        
        if code != 200:
            non_200_status.append(symbol)
        else:
            transformedLatest = transformLatest(data = latest, symbol_id = symbol_id)
            
            if (transformedLatest.loc[1, 'date_at']).strftime('%Y-%m-%d') == (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d'):
                try:
                    transformedLatest.to_sql('investment_stockprices', schema = 'public', if_exists = 'append', index = False, con = engine)
                except:
                    not_uploaded[symbol_id] = transformedLatest
            else:
                pass
        
    return non_200_status, not_uploaded, non_downloaded, empty_data
        

    
