# -*- coding: utf-8 -*-
"""
Created on Sat Apr 15 12:52:55 2023

@author: bidzh
"""

from config.config import *
import pandas as pd


def getIDSymbol(symbol, connection = engine):
    df = pd.read_sql("""select id from public.investment_company where symbol = '{}'""".format(symbol), con = connection)
    return df.iloc[0]['id']

def getIDCompanyCommon(company_id, date, period, connection = engine):
    df = pd.read_sql("""
                         select id from public.investment_companycommon 
                         where 
                             1=1
                             and company_id = {}
                             and fiscal_date = {}
                             and period = {}
                     """.format(company_id, date, period), con = connection)
                     
    return df.iloc[0]['id']


 
def getIDPhysicalCurrency(currency_code, connection = engine):
    df = pd.read_sql(
            """
                select id from public.investment_physicalcurrency
                where
                    1=1
                    and currency_code = '{}'
            """.format(currency_code)
        )
    
    return df.iloc[0]['id']



def getIDDigitalCurrency(currency_code, connection = engine):
    df = pd.read_sql(
            """
                select id from public.investment_digitalcurrency
                where
                    1=1
                    and currency_code = {}
            """.format(currency_code)
            , con = connection
        )
    
    return df.iloc[0]['id']

def getIDIndicator(indicatorType, connection = engine):
    df = pd.read_sql(
            """
                select id from public.investment_indicator
                where
                    1=1
                    and indicatorType = {}
            """.format(indicatorType)
            , con = connection
        )
    return df.iloc[0]['id']

def getIDSector(sector_name, connection = engine):
    df = pd.read_sql(
            """
                select id from public.investment_sector
                where 
                    1=1
                    and sector_name = '{}'
            """.format(sector_name)
            , con = connection
        )

    return df.iloc[0]['id']

def getIDIndustry(industry_name, connection = engine):
    df = pd.read_sql(
            """
                select id from public.investment_industry
                where 
                    1=1
                    and industry_name = '{}'
            """.format(industry_name)
            , con = connection
        )
    return df.iloc[0]['id']

def getIDCommodityType(commodityType, connection = engine):
    df = pd.read_sql(
            """
                select id from public.investment_commodity
                where
                    1=1
                    and commoditytype = {}
            """.format(commodityType)
            , con = connection
        )
    return df.iloc[0]['id']


def getIDMaturity(maturityType, connection = engine):
    df = pd.read_sql(
            """
                select id from public.investment_maturity
                where
                    1=1
                    and maturitytype = {}
            """.format(maturityType)
            , con = connection
        )
    
    return df.iloc[0]['id']



def getIDNewsCategory(category, connection = engine):
    df = pd.read_sql(
            """
                select id from public.investment_newscategory
                where
                    1=1
                    and category = {}
            """.format(category)
            , con = connection
        )
    return df.iloc[0]['id']


def getIDNewsSentiment(sentiment, connection = engine):
    df = pd.read_sql(
            """
                select id from public.investment_newssentiment
                where
                    1=1
                    and sentiment = {}
            """.format(sentiment)
            , con = connection        
        )
    return df.iloc[0]['id']

def getIDNewsTopics(topics, connection = engine):
    df = pd.read_sql(
            """
                select id from public.investment_newstopics
                where
                    1=1
                    and topics = {}
            """.format(topics)
            , con = connection
        )
    return df.iloc[0]['id']


def getIDPricePeriod(period, connection = engine):
    df = pd.read_sql(
            """
                select id from public.investment_priceperiod
                where 
                    1=1
                    and period = {}
            """.format(period)
            , con = connection
        )
    return df.iloc[0]['id']


def getIDReportPeriod(period, connection = engine):
    df = pd.read_sql(
            """
                select id from public.investment_reportperiod
                where 
                    1=1
                    and period = {}
            """.format(period)
            , con = connection
        )
    return df.iloc[0]['id']


def getIDSource(source, connection = engine):
    df =pd.read_sql(
            """
                select id from public.investment_sourcedomain
                where
                    1=1
                    and source = {}
            """.format(source)
            , con = connection
        )
    return df.iloc[0]['id']

def getID(table_name, attribute_name, value, connection = engine):
    """
        Returns id of catalogues tables
        Input:
            table_name: the name of table to extract id from
            attribute_name: the name of attribute of selected table
            value: the value of attribute to get the id
        Output:
            the ID of selected value
        
    """
    try:        
        df = pd.read_sql(
                """
                    select id from public.{}
                    where
                        1=1
                        and {} = {}
                """.format(table_name, attribute_name, value)
                , con = connection
            )
        return df.iloc[0]['id']
    except:
        return None


def getIDExchange(exchange_name, connection = engine):
    try:
        df = pd.read_sql(
                """
                    select id from public.investment_exchange
                    where
                        1=1
                        and exchange_name = '{}'
                """.format(exchange_name)
                , con = connection
            )
        return df.iloc[0]['id']
    except:
        return None
    

def getIDSecurityType(security_name, connection = engine):
    try:
        df = pd.read_sql(
                """
                    select id from public.investment_securitytype
                    where
                        1=1
                        and security_type = '{}'
                """.format(security_name)
                , con = connection
            )
        return df.iloc[0]['id']
    except:
        return None











