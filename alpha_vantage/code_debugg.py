# -*- coding: utf-8 -*-
"""
Created on Thu Apr 13 17:14:43 2023

@author: bidzh
"""

from config.config import *
from alpha_vantage.download import *


# =============================================================================
# fundamental data
# =============================================================================


f_listing = alphaListing()
f_company_overview = alphaCompanyOverview('BABA')
f_income_stmt = alphaIncomeStatement('BABA')
f_balance_sheet = alphaBalanceSheet('BABA')
f_cash_flow = alphaCashFlow('BABA')
f_ipo_calendar = alphaIPOCalendar() ## check the function

# =============================================================================
# stock prices
# =============================================================================


# passed
s_ibm_week = alphaStockPrices(symbol='IBM'
                         , period = 'weekly'
                         , outputsize = 'compact'
                         , datatype = 'csv') 


# passesd
s_ibm_montly = alphaStockPrices(symbol='IBM'
                         , period = 'monthly'
                         , outputsize = 'compact'
                         , datatype = 'csv')


s_ibm_intraday = alphaStockPrices(symbol='IBM'
                         , period = 'intraday'
                         , interval = '15min'
                         , outputsize = 'compact'
                         , datatype = 'csv') 


# ---------------------- daily - check once premium key recieved ----------
s_ibm_daily = alphaStockPrices(symbol='IBM'
                         , period = 'daily'
                         , outputsize = 'compact'
                         , datatype = 'csv')

s_ibm_daily = alphaStockPrices(symbol='IBM'
                         , period = 'daily'
                         , outputsize = 'compact'
                         , datatype = 'json')

# --------------------- 

# passed
s_latest_csv = alphaLatest('IBM', datatype = 'csv')

# =============================================================================
# news 
# =============================================================================

# passed
news_ibm = alphaNews('IBM') 

# =============================================================================
# crypto
# =============================================================================

# passed
c_eth_csv = alphaCrypto(symbol='ETH'
                        , period = 'daily'
                        , outputsize = 'compact'
                        , datatype = 'csv')



# passed
c_eth_csv_weekly = alphaCrypto(symbol='ETH'
                        , period = 'weekly'
                        , outputsize = 'compact'
                        , datatype = 'csv')

# passed
c_eth_csv_monthly = alphaCrypto(symbol='ETH'
                        , period = 'monthly'
                        , outputsize = 'compact'
                        , datatype = 'csv')


# =============================================================================
# commodities
# =============================================================================


# passed
com_wti_csv = alphaCommodities(function = 'WTI'
                               , datatype = 'csv')

# passed
com_brent_csv = alphaCommodities(function = 'BRENT'
                               , datatype = 'csv')

# passed
com_gas_csv = alphaCommodities(function = 'NATURAL_GAS'
                               , datatype = 'csv')

# passed
com_copper_csv = alphaCommodities(function = 'COPPER'
                               , datatype = 'csv')

com_alum_csv = alphaCommodities(function = 'ALUMINIUM'
                               , datatype = 'csv') # check for correctness

# passed
com_wheat_csv = alphaCommodities(function = 'WHEAT'
                               , datatype = 'csv')
# passed
com_corn_csv = alphaCommodities(function = 'CORN'
                               , datatype = 'csv')

# passed
com_cotton_csv = alphaCommodities(function = 'COTTON'
                               , datatype = 'csv')

# passed
com_sugar_csv = alphaCommodities(function = 'SUGAR'
                               , datatype = 'csv')

# passed
com_coffe_csv = alphaCommodities(function = 'COFFEE'
                               , datatype = 'csv')


# passed
com_all_csv = alphaCommodities(function = 'ALL_COMMODITIES'
                               , datatype = 'csv')

# =============================================================================
# economic indicator
# =============================================================================

# passed
e_real_gdp_quar = alphaEconomicIndicators(function='REAL_GDP'
                                          , interval = 'quarterly'
                                          , datatype = 'csv')

e_real_gdp_quar = alphaEconomicIndicators(function='REAL_GDP'
                                          , interval = 'quarterly'
                                          , datatype = 'json') # configure json output

# passed
e_real_gdp_annual = alphaEconomicIndicators(function='REAL_GDP'
                                          , interval = 'annually'
                                          , datatype = 'csv')


# passed
e_gdp_cap_quar = alphaEconomicIndicators(function='REAL_GDP_PER_CAPITA'
                                          , interval = 'quarterly'
                                          , datatype = 'csv')
# passed
e_treasury_csv = alphaEconomicIndicators(function='TREASURY_YIELD'
                                          , interval = 'annually'
                                          , maturity='3month'
                                          , datatype = 'csv')

# passed
e_funds = alphaEconomicIndicators(function='FEDERAL_FUNDS_RATE'
                                          , interval = 'monthly'                                          
                                          , datatype = 'csv')

# passed
e_cpi = alphaEconomicIndicators(function='CPI'
                                          , interval = 'monthly'                                          
                                          , datatype = 'csv')

# passed
e_inflation = alphaEconomicIndicators(function='INFLATION'
                                          , interval = 'monthly'                                          
                                          , datatype = 'csv')
# passed
e_retail = alphaEconomicIndicators(function='RETAIL_SALES'
                                          , interval = 'monthly'                                          
                                          , datatype = 'csv')
# passed
e_durable = alphaEconomicIndicators(function='DURABLES'
                                          , interval = 'monthly'                                          
                                          , datatype = 'csv')
# passed
e_unemp = alphaEconomicIndicators(function='UNEMPLOYMENT'
                                          , interval = 'monthly'                                          
                                          , datatype = 'csv')

# passed
e_nonfarm = alphaEconomicIndicators(function='NONFARM_PAYROLL'
                                          , interval = 'monthly'                                          
                                          , datatype = 'csv')


physic = pd.read_csv(r'C:\Users\bidzh\Downloads\physical_currency_list (3).csv')
physic.columns = ['currency_code', 'currency_name']


uploadData(physic, 'investment_physicalcurrency')
digital = pd.read_csv(r'C:\Users\bidzh\Downloads\digital_currency_list.csv')
digital.columns = ['currency_code', 'currency_name']

uploadData(digital, 'investment_digitalcurrency')




