# -*- coding: utf-8 -*-
"""
Created on Tue Apr 18 11:43:13 2023

@author: bidzh
"""

from config.config import *
from alpha_vantage.download import *
from alpha_vantage.get_functions import getID
from tqdm import tqdm
import numpy as np
from datetime import datetime, date


symbols = pd.read_sql("""select ic.id, ic.symbol from public.investment_company ic
                          where
                              1=1
                              and status = 'Active'
                          order by symbol desc                         
                      """, con = engine)

def convertToFloat(value):
    if value == 'None':
        return value
    else:
        return float(value)


def IncomeStatemntUpload(report, period, symbol_id, currency):
    df_temp = pd.DataFrame.from_dict([report])
    fiscal_date = df_temp.loc[0, 'fiscalDateEnding']
    company_common = pd.DataFrame({
            'fiscal_date': [fiscal_date]
            , 'currency': [currency]
            , 'company_id': [symbol_id]
            , 'period_id': [period]        
        })
    company_common.to_sql('investment_companycommon', schema = 'public', if_exists = 'append', index = False, con = engine)
    
    common_id = pd.read_sql("""
                              select max(id) from public.investment_companycommon
                            """, con = engine).loc[0, 'max']
                          
    income_st = pd.DataFrame({
            'companycommon_ptr_id': [common_id]
            , 'gross_profit': [convertToFloat(df_temp.loc[0, 'grossProfit'])]
            , 'total_revenue': [convertToFloat(df_temp.loc[0, 'totalRevenue'])]
            , 'cost_of_revenue': [convertToFloat(df_temp.loc[0, 'costOfRevenue'])]
            , 'cost_of_goos_sold': [convertToFloat(df_temp.loc[0, 'costofGoodsAndServicesSold'])]
            , 'operating_income': [convertToFloat(df_temp.loc[0, 'operatingIncome'])]
            , 'selling_general_and_admin': [convertToFloat(df_temp.loc[0, 'sellingGeneralAndAdministrative'])]
            , 'rnd': [convertToFloat(df_temp.loc[0, 'researchAndDevelopment'])]
            , 'operating_expenses': [convertToFloat(df_temp.loc[0, 'operatingExpenses'])]
            , 'investment_income_net': [convertToFloat(df_temp.loc[0, 'investmentIncomeNet'])]
            , 'net_interest_income': [convertToFloat(df_temp.loc[0, 'netInterestIncome'])]
            , 'interest_income': [convertToFloat(df_temp.loc[0, 'interestIncome'])]
            , 'interest_expense': [convertToFloat(df_temp.loc[0, 'interestExpense'])]
            , 'non_interest_income': [convertToFloat(df_temp.loc[0, 'nonInterestIncome'])]
            , 'other_nonoper_income': [convertToFloat(df_temp.loc[0, 'otherNonOperatingIncome'])]
            , 'depreciation': [convertToFloat(df_temp.loc[0, 'depreciation'])]
            , 'depreciation_and_amortization': [convertToFloat(df_temp.loc[0, 'depreciationAndAmortization'])]
            , 'income_before_tax': [convertToFloat(df_temp.loc[0, 'incomeBeforeTax'])]
            , 'income_tax_expense': [convertToFloat(df_temp.loc[0, 'incomeTaxExpense'])]
            , 'interest_debt_expense': [convertToFloat(df_temp.loc[0, 'interestAndDebtExpense'])]
            , 'net_income_from_contin_operations': [convertToFloat(df_temp.loc[0, 'netIncomeFromContinuingOperations'])]
            , 'comprehensive_income_net_of_tax': [convertToFloat(df_temp.loc[0, 'comprehensiveIncomeNetOfTax'])]
            , 'ebit': [convertToFloat(df_temp.loc[0, 'ebit'])]
            , 'ebitda': [convertToFloat(df_temp.loc[0, 'ebitda'])]
            , 'net_income': [convertToFloat(df_temp.loc[0, 'netIncome'])]
        })
    
    income_st = income_st.replace('None', None, inplace = False)
    try:
        income_st.to_sql('investment_companyincomestatement', schema = 'public', if_exists='append', index = False, con = engine)
    except:
        if period == 1:
            txt = 'Annual'
        else:
            txt = 'Quarter'
        return print("""{} Income Statement report as of {} for {} symbol was not uploaded""".format(txt, df_temp.loc[0,'fiscalDateEnding'], symbols.loc[i, 'symbol'] ))
  

def BalanceSheetUpload(report, symbol_id, period, currency):
    df_temp = pd.DataFrame.from_dict([report])
    fiscal_date = df_temp.loc[0, 'fiscalDateEnding']
    try:
        common_id = pd.read_sql(
                """
                    select id from public.investment_companycommon
                    where
                        1=1
                        and company_id = {}
                        and period_id = {}
                        and currency = '{}'
                        and fiscal_date = '{}'
                """.format(symbol_id, period, currency, fiscal_date)
                , con = engine
            ).loc[0, 'id']
    except:
        company_common = pd.DataFrame({
                'fiscal_date': [fiscal_date]
                , 'currency': [currency]
                , 'company_id': [symbol_id]
                , 'period_id': [period]        
            })
        company_common.to_sql('investment_companycommon', schema = 'public', if_exists = 'append', index = False, con = engine)
        
        common_id = pd.read_sql("""
                                  select max(id) from public.investment_companycommon
                              """, con = engine).loc[0, 'max']

            
        
    balance_sheet = pd.DataFrame({
            'companycommon_ptr_id': [common_id]
            , 'total_assets': [convertToFloat(df_temp.loc[0, 'totalAssets'])]
            , 'total_current_assets': [convertToFloat(df_temp.loc[0, 'totalCurrentAssets'])]
            , 'cash_and_equiv': [convertToFloat(df_temp.loc[0, 'cashAndCashEquivalentsAtCarryingValue'])]
            , 'cash_and_shortinvestment': [convertToFloat(df_temp.loc[0, 'cashAndShortTermInvestments'])]
            , 'inventory': [convertToFloat(df_temp.loc[0, 'inventory'])]
            , 'current_net_recievables': [convertToFloat(df_temp.loc[0, 'currentNetReceivables'])]
            , 'total_non_current_assets': [convertToFloat(df_temp.loc[0, 'totalNonCurrentAssets'])]
            , 'proprety_plan_equipment': [convertToFloat(df_temp.loc[0, 'propertyPlantEquipment'])]
            , 'accumulated_deprec_amort': [convertToFloat(df_temp.loc[0, 'accumulatedDepreciationAmortizationPPE'])]
            , 'intagible_assets': [convertToFloat(df_temp.loc[0, 'intangibleAssets'])]
            , 'intagible_assets_exc_goodwill': [convertToFloat(df_temp.loc[0, 'intangibleAssetsExcludingGoodwill'])]
            , 'goodwill': [convertToFloat(df_temp.loc[0, 'goodwill'])]
            , 'investments': [convertToFloat(df_temp.loc[0, 'investments'])]
            , 'long_term_investment': [convertToFloat(df_temp.loc[0, 'longTermInvestments'])]
            , 'short_term_investment': [convertToFloat(df_temp.loc[0, 'shortTermInvestments'])]
            , 'other_current_assets': [convertToFloat(df_temp.loc[0, 'otherCurrentAssets'])]
            , 'other_noncurrent_assets': [convertToFloat(df_temp.loc[0, 'otherNonCurrentAssets'])]
            , 'total_liabilities': [convertToFloat(df_temp.loc[0, 'totalLiabilities'])]
            , 'total_current_liabilities': [convertToFloat(df_temp.loc[0, 'totalCurrentLiabilities'])]
            , 'current_accounts_payable': [convertToFloat(df_temp.loc[0, 'currentAccountsPayable'])]
            , 'deferred_revenue': [convertToFloat(df_temp.loc[0, 'deferredRevenue'])]
            , 'current_debt': [convertToFloat(df_temp.loc[0, 'currentDebt'])]
            , 'short_term_debt': [convertToFloat(df_temp.loc[0, 'shortTermDebt'])]
            , 'total_noncurrent_liab': [convertToFloat(df_temp.loc[0, 'totalNonCurrentLiabilities'])]
            , 'capital_lease_obligations': [convertToFloat(df_temp.loc[0, 'capitalLeaseObligations'])]
            , 'long_term_debt': [convertToFloat(df_temp.loc[0, 'longTermDebt'])]
            , 'current_long_term_debt': [convertToFloat(df_temp.loc[0, 'currentLongTermDebt'])]
            , 'long_term_noncurrent': [convertToFloat(df_temp.loc[0, 'longTermDebtNoncurrent'])]
            , 'short_long_term_debt_total': [convertToFloat(df_temp.loc[0, 'shortLongTermDebtTotal'])]
            , 'other_current_liab': [convertToFloat(df_temp.loc[0, 'otherCurrentLiabilities'])]
            , 'other_noncurrent_liab': [convertToFloat(df_temp.loc[0, 'otherNonCurrentLiabilities'])]
            , 'total_shareholder_equity': [convertToFloat(df_temp.loc[0, 'totalShareholderEquity'])]
            , 'treasury_stock': [convertToFloat(df_temp.loc[0, 'treasuryStock'])]
            , 'retained_earnings': [convertToFloat(df_temp.loc[0, 'retainedEarnings'])]
            , 'common_stock': [convertToFloat(df_temp.loc[0, 'commonStock'])]
            , 'common_stock_shares_outstand': [convertToFloat(df_temp.loc[0, 'commonStockSharesOutstanding'])]
        })
    
    balance_sheet = balance_sheet.replace('None', None, inplace = False)
    try:
        balance_sheet.to_sql('investment_companybalancesheet', schema = 'public', if_exists='append', index = False, con = engine)
    except:
        if period == 1:
            txt = 'Annual'
        else:
            txt = 'Quarter'
            print("""{} Balance sheet report as of {} for {} symbol was not uploaded""".format(txt, df_temp.loc[0,'fiscalDateEnding'], symbol ))

    
def CashFlowUpload(report, period, symbol_id, currency):
    df_temp = pd.DataFrame.from_dict([report])
    fiscal_date = df_temp.loc[0, 'fiscalDateEnding']

    try:
        common_id = pd.read_sql(
                """
                    select id from public.investment_companycommon
                    where
                        1=1
                        and company_id = {}
                        and period_id = {}
                        and currency = '{}'
                        and fiscal_date = '{}'
                """.format(symbol_id, period, currency, fiscal_date)
                , con = engine
            ).loc[0, 'id']
    except:
        company_common = pd.DataFrame({
                'fiscal_date': [fiscal_date]
                , 'currency': [currency]
                , 'company_id': [symbol_id]
                , 'period_id': [period]        
            })
        company_common.to_sql('investment_companycommon', schema = 'public', if_exists = 'append', index = False, con = engine)
        
        common_id = pd.read_sql("""
                                  select max(id) from public.investment_companycommon
                              """, con = engine).loc[0, 'max']

    cash_flow = pd.DataFrame({
            'companycommon_ptr_id': [common_id]
            , 'operating_cashflow': [convertToFloat(df_temp.loc[0, 'operatingCashflow'])]
            , 'payment_from_oper_activities': [convertToFloat(df_temp.loc[0, 'paymentsForOperatingActivities'])]
            , 'proceeds_from_oper_activities': [convertToFloat(df_temp.loc[0, 'proceedsFromOperatingActivities'])]
            , 'change_in_oper_liab': [convertToFloat(df_temp.loc[0, 'changeInOperatingLiabilities'])]
            , 'change_in_oper_assets': [convertToFloat(df_temp.loc[0, 'changeInOperatingAssets'])]
            , 'deprec_depletion_amort': [convertToFloat(df_temp.loc[0, 'depreciationDepletionAndAmortization'])]
            , 'capital_expenditures': [convertToFloat(df_temp.loc[0, 'capitalExpenditures'])]
            , 'change_in_recievables': [convertToFloat(df_temp.loc[0, 'changeInReceivables'])]
            , 'change_in_inventory': [convertToFloat(df_temp.loc[0, 'changeInInventory'])]
            , 'profit_loss': [convertToFloat(df_temp.loc[0, 'profitLoss'])]
            , 'cashflow_from_investment': [convertToFloat(df_temp.loc[0, 'cashflowFromInvestment'])]
            , 'cashflow_from_financing': [convertToFloat(df_temp.loc[0, 'cashflowFromFinancing'])]
            , 'proceeds_from_repayment_of_short_term_debt': [convertToFloat(df_temp.loc[0, 'proceedsFromRepaymentsOfShortTermDebt'])]
            , 'payments_for_repurchase_of_common_stocks': [convertToFloat(df_temp.loc[0, 'paymentsForRepurchaseOfCommonStock'])]
            , 'payment_for_repurchase_of_equity': [convertToFloat(df_temp.loc[0, 'paymentsForRepurchaseOfEquity'])]
            , 'payment_for_repurchase_of_preferred_stock': [convertToFloat(df_temp.loc[0, 'paymentsForRepurchaseOfPreferredStock'])]
            , 'dividend_payout': [convertToFloat(df_temp.loc[0, 'dividendPayout'])]
            , 'dividend_payout_common_stock': [convertToFloat(df_temp.loc[0, 'dividendPayoutCommonStock'])]
            , 'dividend_payout_preferred_stock': [convertToFloat(df_temp.loc[0, 'dividendPayoutPreferredStock'])]
            , 'proceeds_from_issuance_of_common_stocks': [convertToFloat(df_temp.loc[0, 'proceedsFromIssuanceOfCommonStock'])]
            , 'proceeds_from_issuance_of_long_term_debt': [convertToFloat(df_temp.loc[0, 'proceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet'])]
            , 'proceeds_from_issuance_of_preferres_stocks': [convertToFloat(df_temp.loc[0, 'proceedsFromIssuanceOfPreferredStock'])]
            , 'proceeds_from_repurchase_of_equity': [convertToFloat(df_temp.loc[0, 'proceedsFromRepurchaseOfEquity'])]
            , 'proceeds_from_sale_of_treasury_stock': [convertToFloat(df_temp.loc[0, 'proceedsFromSaleOfTreasuryStock'])]
            , 'change_in_cash_and_equiv': [convertToFloat(df_temp.loc[0, 'changeInCashAndCashEquivalents'])]
            , 'change_in_exchange_rate': [convertToFloat(df_temp.loc[0, 'changeInExchangeRate'])]
            , 'net_income': [convertToFloat(df_temp.loc[0, 'netIncome'])]        
        })
    cash_flow = cash_flow.replace('None', None, inplace=False)
    try:
        cash_flow.to_sql('investment_companycashflow', schema = 'public', if_exists = 'append', index = False, con = engine)
    except:
        if period == 1:
            txt = 'Annual'
        else:
            txt = 'Quarter'
        return print("""{} Cash Flow report as of {} for {} symbol was not uploaded""".format(txt, df_temp.loc[0,'fiscalDateEnding'], symbol ))


for i in tqdm(symbols.index):
    symbol_id = symbols.loc[i, 'id']
    symbol = symbols.loc[i, 'symbol']
    
    if '-' in symbol:
        print (symbol)
        continue
    
    income = alphaIncomeStatement(symbol)
    
    if len(income['annualReports']) == 0:
        print(symbol)
        continue

    currency = pd.DataFrame.from_dict([income['annualReports'][0]]).loc[0, 'reportedCurrency']
    fiscal_date = pd.DataFrame.from_dict([income['annualReports'][0]]).loc[0, 'fiscalDateEnding']
    
    
    balance= alphaBalanceSheet(symbol)
    cash = alphaCashFlow(symbol)  
      
    # income stattement
    annualReports_Income = income['annualReports']
    for report in annualReports_Income:
        IncomeStatemntUpload(report = report, period = 1, symbol_id=symbol_id, currency = currency)
        
    quarterReports_Income = income['quarterlyReports']
    for report in quarterReports_Income:
        IncomeStatemntUpload(report = report, period = 2, symbol_id = symbol_id, currency = currency)
 
    # balance sheet
    annualReports_balance = balance['annualReports']
    for report in annualReports_balance:
        BalanceSheetUpload(report = report, period = 1, symbol_id=symbol_id, currency = currency)
        
    quarterReports_balance = balance['quarterlyReports']
    for report in quarterReports_balance:
        BalanceSheetUpload(report=report, period = 2, symbol_id = symbol_id, currency = currency)
         

    # cash flow
    annualReports_cash = cash['annualReports']
    for report in annualReports_cash:
        CashFlowUpload(report = report, period = 1, symbol_id=symbol_id, currency = currency)
        
    quarterReports_cash = cash['quarterlyReports']
    for report in quarterReports_cash:
        CashFlowUpload(report=report, period = 2, symbol_id = symbol_id, currency = currency)
    

