#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 25 17:22:15 2023

@author: david
"""

from config.config import *
from alpha_vantage.download import *
from alpha_vantage.get_functions import *
from alpha_vantage.upload import *
from tqdm import tqdm
from datetime import datetime, date, timedelta
import sys

# =============================================================================
# downloading data
# =============================================================================

current_date = datetime.today().strftime('%Y-%m-%d')
previous_day = (datetime.today() - timedelta(days = 1)).strftime('%Y-%m-%d')

listing_alpha = alphaListing(status = 'active')
listing_alpha_new = listing_alpha[listing_alpha['assetType'] == 'Stock']


listing_db = pd.read_sql(
        """
            select * from public.investment_company
            where
                1=1
                and status = 'Active'
        """
    , con = engine
    ) 



# =============================================================================
# Inactive status
# =============================================================================

print("Current date is:", current_date)

difference_inactive = listing_alpha_new.merge(listing_db, indicator=True, on = 'symbol', how = 'left').loc[lambda x: x['_merge'] == 'right_only']
if len(difference_inactive) != 0:
    for t in difference_inactive.index:
        symbol = difference_inactive.loc[t, 'symbol']
        sql = """
                  update public.investment_company
                  set status = 'Inactive'
                      , date_update = '{}'
                  where symbol = '{}'        
              """.format(current_date, symbol)
        with engine.begin() as conn:
            conn.execute(sql)
    print("Inactive companies status as od {} were updated".format(current_date))
else:
    print("No inactive companies")

   

    
# =============================================================================
# new active companies
# =============================================================================

difference = listing_alpha_new.merge(listing_db, indicator = True, how = 'left', on = 'symbol').loc[lambda x: x['_merge'] == 'left_only']
difference_new = difference[difference['ipoDate'] == previous_day ][['symbol', 'name_x', 'ipoDate', 'status_x', 'exchange', 'assetType']]


if len(difference_new) == 0:
    sys.exit('No companies as of {}'.format(previous_day))


companies = pd.DataFrame()
nan = []
error = []

for i in tqdm(difference_new.index):
    symbol = difference_new.loc[i, 'symbol']
    try:
        temp = alphaCompanyOverview(symbol = symbol).T
        if len(data) != 0:
            companies = pd.concat([companies, temp])
        else:
            nan.append(symbol)
    except:
        error.append(symbol)
        

companies = companies.rename(columns = {'Symbol': 'symbol'})
        
if len(companies.columns) != 0:
    merged = companies.merge(difference_new, on = 'symbol',  indicator=False, how = 'inner')

    # common info
    investment_company = uploadListingCompanies(merged = merged)
    investment_company.to_sql('investment_company', schema = 'public', if_exists = 'append', index = False, con = engine)

    
    # companyratios
    investment_companyratios = uploadNewCompanyOverview(merged)
    investment_companyratios.to_sql('investment_companyratios', schema = 'public', if_exists = 'append', index = False, con = engine)
    print("Full company data as of {} was uploaded".format(current_date))

else:
    difference_new['exchange_id'] = difference_new['exchange'].apply(lambda x: getIDExchange(x))
    difference_new['security_type_id'] = difference_new['assetType'].apply(lambda x: getIDSecurityType(x))
    difference_new['sector_id'] = None
    difference_new['industry_id'] = None
    difference_new['cik'] = None
    difference_new = difference_new.rename(columns = {'name_x': 'name'
                                                      , 'ipoDate': 'ipo_date'
                                                      , 'status_x': 'status'})
    difference_new = difference_new[['symbol', 'name', 'ipo_date', 'status', 'exchange_id'
                                     , 'cik', 'security_type_id', 'sector_id', 'industry_id']]
    difference_new['date_upload'] = current_date
    difference_new['date_update'] = current_date
    
    difference_new.to_sql('investment_company', schema = 'public', if_exists = 'append', index= False, con = engine)
    print("Restricted company data as of {} was uploaded".format(current_date))

if len(error) !=0:
    print('Error symbols {} as of {}'.format(error, current_date))
    


      
        
        
        
 
        
        
