#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 29 18:02:04 2023

@author: david
"""

from config.config import *
from alpha_vantage.download import *
import pandas as pd
import numpy as np
from scipy.stats import norm
from multiprocess import Pool
pd.options.mode.chained_assignment = None



def VaRCalculation(Data, Formula, Period_Interval,  Confidence_Interval = 0.99, EWMA_lambda = 0.94):
    """ Calculates Value-at-Risk using three methods
        1. Historical simulation
        2. Parametric VaR
        3. Parametric EWMA
        Inputs:
            Data: dataframe with the stock returns
            Formula: either 'Historical simulation' or 'Parametric EWMA' or 'Parametric Normal'
            Period_Interval: number of days for VaR calculation
            Confidence_Interval: confidence interval for VaR calculation, by default 0.99
            EWMA_lambda: lambda coefficient for EWMA VaR calculation
        Output:
            VaR: value-at-risk for the stock prices in percents
            
    """
    
    # ===================================================
    # Historical simulation
    # ===================================================
    if Formula == 'Historical simulation':
        VaR = np.quantile(Data, 1 - Confidence_Interval)
        return(float('{:.5f}'.format(VaR)))
    
    # ===================================================
    # Parametric Normal
    # ===================================================
    if Formula == 'Parametric Normal':
        VaR = Data.mean() - Data.std() * norm.ppf(Confidence_Interval)
        return(float('{:.5f}'.format(VaR)))
   
    
    # ===================================================
    # Parametric EWMA
    # ===================================================
    if Formula == 'Parametric EWMA':
        Degree_of_Freedom = np.empty([Period_Interval, ])
        Weights =  np.empty([Period_Interval, ])
        Degree_of_Freedom[0] = 1
        Degree_of_Freedom[1] = EWMA_lambda
        Range = range(Period_Interval)
        for i in range(2,Period_Interval):
            Degree_of_Freedom[i]=Degree_of_Freedom[1]**Range[i]
        for i in range(Period_Interval):
            Weights[i]=Degree_of_Freedom[i]/sum(Degree_of_Freedom)           
        
        sqrdData = Data**2
        EWMAstd = np.sqrt(sum(Weights * sqrdData))
        
        VaR = Data.mean() - EWMAstd * norm.ppf(Confidence_Interval)
        return(float('{:.5f}'.format(VaR)))


def MarginVaR(Ticker, Portfolio, Shares):
    """
    Calculate marginal VaR for each stock in the portfolio
    Inputs:
        ticker: ticker of the share to be analysed
        Portfolio: dataframe of return rates
        Shares: dataframe of shares in the portfolio
    output:
        margVaR: marginalVaR for ticker of analysis
    """
    
    i = (Portfolio[Ticker])
    data_return = np.matmul(Portfolio, Shares)
    corr = i.corr(data_return)
    std_i = i.std()
    std_p = data_return.std()
    margVaR = corr *std_i/std_p
    
    return margVaR 


def DelNa(Data):
    """ drops rows of df where all values are any
    """
    return Data.dropna(how='all')

def ComponentVaR(margVar: list[str]
                 , shares: list[float]
                 , VaR_amount: float):
    
    """
        Calculates a component VaR for the stock in the portfolio
        Inputs:
            margVar: list of marginal VaR for the stock in the portfolio
            shares: list of shares of stocks in the portfolio
            VaR_amount: total VaR amount
        Output:
            comp_var: component VaR for the selected stock
    """
    
    comp_perc = [(x, y) for x in margVar for y in shares]
    comp_perc = [x*y for x,y in zip(margVar, shares)]
    
    comp_var = [x*VaR_amount for x in comp_perc]
    
    return comp_var


def riskRatios(df, ratio_type, Beta):
    """
        Calculates Sharp, Sortino, Treynor ratios
        Input:
            df: dataframe with the log_returns
            ratio_type: either 'sharp' or 'sortino' or 'treynor'
        Output:
            ratio: calculated ratio
    """
    if ratio_type == 'sharp':
        try:
            ratio = df.mean()/df.std()*252**0.5
        except:
            ratio = None
    
    if ratio_type == 'sortino':
        try:
            ratio = df.mean()/df[df<0].std()*252**0.5
        except:
            ratio = None

    if ratio_type == 'treynor':
        if Beta is None or Beta == 0:
            ratio = None
        else:
            ratio = df.mean()/Beta*252**0.5
            
    return ratio






def FillVaR(Data, Beta, Period_Interval = 252):
    """
    calculates Vale-at-Risk for the wghole range of data
    Input:
        Data: dataframe with portfolio log-returns
        Beta: float, beta of stock from CAPM model
        Period_interval: integer, days of analysis
    Output:
        Data with portfolio returns and VaRs
    """
    Data = Data.sort_index(ascending = True)
    Data = Data[1:]
    
    Data['historical_var'] = 0
    Data['parametric_var'] = 0
    Data['ewma_var'] = 0
    Data['sharp_ratio'] = 0
    Data['sortino_ratio'] = 0
    Data['treynor_ratio'] = 0
    p = Pool(3)
   
    # NewData = Data[50:].reset_index().drop(['index'], axis = 1)
    NewData = Data[Period_Interval:]
    for count, i in enumerate(NewData.index):
        df = Data[count :count + Period_Interval ][Data.columns[3]]
        df = df.sort_index(ascending=False)
        
        
        ratios = p.starmap(riskRatios, [(df, 'sharp', None)
                                        , (df, 'sortino', None)
                                        , (df, 'treynor', Beta)])
        
 
        var = p.starmap(VaRCalculation, [  (df, 'Historical simulation', Period_Interval)
                                          , (df, 'Parametric EWMA', Period_Interval)
                                          , (df, 'Parametric Normal', Period_Interval)])
        
        NewData.loc[i, 'historical_var'] = var[0]
        NewData.loc[i, 'parametric_var'] = var[2]
        NewData.loc[i, 'ewma_var'] = var[1]
        NewData.loc[i, 'sharp_ratio'] = ratios[0]
        NewData.loc[i, 'sortino_ratio'] = ratios[1]
        NewData.loc[i, 'treynor_ratio'] = ratios[2]
            
        
        
    # return NewData[[Data.columns[0], 'Historical VaR', 'Parametric VaR', 'Parametric EWMA']]
    final = NewData[['ewma_var', 'historical_var', 'parametric_var', 'log_return', 'sharp_ratio', 'sortino_ratio', 'treynor_ratio', 'date_at']]
    # final.columns = Data.columns[0]
    # final = final.rename(columns = {'Parametric EWMA' : Data.columns[0]})
    
    return final



