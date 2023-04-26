# -*- coding: utf-8 -*-
"""
Created on Mon Apr 17 11:43:05 2023

@author: bidzh
"""

from config.config import *
from alpha_vantage.download import *
from alpha_vantage.get_functions import getID
from tqdm import tqdm
import numpy as np
from datetime import datetime, date

companies = alphaListing()

df_companies = pd.DataFrame()
error = []
nan = []

for i in tqdm(companies.index):
    try:
        data = alphaCompanyOverview(companies.loc[i, 'symbol']).T
        if len(data) != 0:
            df_companies = pd.concat([df_companies, data])
        else:
            nan.append(companies.loc[i, 'symbol'])
    except:
        error.append(companies.loc[i, 'symbol'])


df_companies = df_companies[df_companies['Symbol'].notna()]

df_companies.replace('None', np.nan, inplace = True)

df_compound = df_companies.merge(companies, how = 'inner', left_on = 'Symbol', right_on = 'symbol')



# =============================================================================
# Basic data upload
 # =============================================================================

cat_sector = alphaSelectCatalogue('investment_sector')
cat_industry = alphaSelectCatalogue('investment_industry')
cat_exchange = alphaSelectCatalogue('investment_exchange')
cat_security_type = alphaSelectCatalogue('investment_securitytype')




company_basic = df_compound[['Name', 'Symbol', 'CIK', 'Sector', 'Industry',
                             'Exchange', 'AssetType', 'ipoDate', 'status']]

company_basic = company_basic.merge(cat_sector, how = 'inner', left_on = 'Sector', right_on = 'sector_name')
company_basic = company_basic.merge(cat_industry, how = 'inner', left_on = 'Industry', right_on = 'industry_name')
company_basic = company_basic.rename(columns = {'id_x': 'sector_id', 'id_y': 'industry_id'})


company_basic = company_basic.merge(cat_exchange, how = 'inner', left_on = 'Exchange', right_on = 'exchange_name')
company_basic = company_basic.merge(cat_security_type, how = 'inner', left_on = 'AssetType', right_on = 'security_type')
company_basic = company_basic.rename(columns = {'id_x': 'exchange_id', 'id_y': 'security_type_id'})
company_basic['date_upload'] = date.today()
company_basic['date_update'] = date.today()


invest_company = pd.read_sql('select * from public.investment_company', con = engine)

company_basic = company_basic[['Name', 'Symbol', 'status', 'CIK',  'sector_id', 'industry_id'
                               , 'date_upload', 'date_update', 'ipoDate', 'exchange_id', 'security_type_id']]

company_basic.columns = invest_company.columns

company_basic.to_sql('investment_company', schema = 'public', if_exists = 'append', index = False, con = engine)




company_ratios = df_compound[['Symbol', 'LatestQuarter', 'MarketCapitalization', 'EBITDA', 'PERatio', 'PEGRatio'
                             , 'BookValue', 'DividendPerShare', 'DividendYield', 'EPS'
                             , 'RevenuePerShareTTM', 'ProfitMargin', 'OperatingMarginTTM', 'ReturnOnAssetsTTM'
                             , 'ReturnOnEquityTTM', 'RevenueTTM', 'GrossProfitTTM', 'DilutedEPSTTM'
                             , 'TrailingPE', 'ForwardPE', 'PriceToSalesRatioTTM', 'PriceToBookRatio'
                             , 'EVToRevenue', 'EVToEBITDA', 'Beta', 'SharesOutstanding']]

company_ratios_id = company_ratios.merge(invest_company[['id', 'symbol']], how = 'inner', left_on = 'Symbol', right_on = 'symbol')

company_ratios_new = company_ratios_id[['LatestQuarter', 'MarketCapitalization', 'EBITDA', 'PERatio', 'PEGRatio', 'BookValue'
                                        , 'DividendPerShare', 'DividendYield', 'EPS', 'RevenuePerShareTTM', 'ProfitMargin', 'OperatingMarginTTM'
                                        , 'ReturnOnAssetsTTM', 'ReturnOnEquityTTM', 'RevenueTTM', 'GrossProfitTTM', 'DilutedEPSTTM'
                                        , 'TrailingPE', 'ForwardPE', 'PriceToSalesRatioTTM', 'PriceToBookRatio', 'EVToRevenue'
                                        , 'EVToEBITDA', 'Beta', 'SharesOutstanding', 'id']]

invest_companyratios = pd.read_sql('select * from public.investment_companyratios', con = engine).drop(columns = {'id'})

company_ratios_new.columns = invest_companyratios.columns
company_ratios_new = company_ratios_new.replace('0000-00-00', None, inplace = False)
company_ratios_new = company_ratios_new.where(pd.notnull(company_ratios_new), None)


company_ratios_new.to_sql('investment_companyratios',  schema = 'public', if_exists = 'append', index = False, con = engine)

# =============================================================================
# fundamental data
# =============================================================================


test = alphaIncomeStatement('IBM')

annual = test['annualReports']
annual_1 = pd.DataFrame.from_dict([annual[0]] )
t = annual[0]
df_t = pd.Da
