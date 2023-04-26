#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 25 11:16:06 2023

@author: david
"""

from config.config import *
from alpha_vantage.download import *
import pandas as pd


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



def uploadListingCompanies(merged):

    # common info
    investment_company = merged[['Name', 'symbol', 'CIK', 'Sector', 'status', 'Industry', 'ipoDate', 'Exchange', 'AssetType']]
    investment_company['date_upload'] = date.today()
    investment_company['date_update'] = date.today()
    investment_company['sector_id'] = investment_company['Sector'].apply(lambda x: getIDSector(sector_name=x))
    investment_company['industry_id'] = investment_company['Industry'].apply(lambda x: getIDIndustry(industry_name=x))
    investment_company['exchange_id'] = investment_company['Exchange'].apply(lambda x: getIDExchange(x))
    investment_company['security_type_id'] = investment_company['AssetType'].apply(lambda x: getIDSecurityType(x))
    investment_company = investment_company[['Name', 'symbol', 'status', 'CIK', 'sector_id', 'industry_id', 'date_upload', 'date_update', 'ipoDate', 'exchange_id', 'security_type_id']]
    investment_company = investment_company.rename(columns = {'Name': 'name'
                                                              , 'CIK': 'cik'
                                                              , 'ipoDate': 'ipo_date'})
    
    
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
    
    return investment_companyratios




    
