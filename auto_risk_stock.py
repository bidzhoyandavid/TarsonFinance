#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May  2 11:34:13 2023

@author: david
"""

from config.config import *
from alpha_vantage.download import *
from risk_analytics.var_metrics import *
import pandas as pd
from tqdm import tqdm
from datetime import datetime, date, timedelta
import sys


today = date.today()
yesterday = (today - timedelta(days=1)).strftime("%Y-%m-%d")

if today.isoweekday() in [7, 1]:
    sys.exit("The date {} is sunday or monday".format(today))


symbols = pd.read_sql(
    """
            select id, symbol from public.investment_company
            where
                1=1
                and status = 'Active'
        """,
    con=engine,
)

non_252_days = []

for i in symbols.index:
    symbol = symbols.loc[i, "symbol"]
    symbol_id = symbols.loc[i, "id"]

    data = pd.read_sql(
        """
            select
                date_at
                , stock_id
                , close
            from public.investment_stockprices
            where
                1=1
                and period_id = 1
                and stock_id = {}
            order by date_at desc
            limit 254
            """.format(
            symbol_id
        ),
        con=engine,
    )

    if len(data) < 254:
        non_252_days.append(symbol)
        continue

    try:
        beta = pd.read_sql(
            """
                    select beta from public.investment_companyratios
                    where
                        1=1
                        and name_id = {}
                """.format(
                symbol_id
            ),
            con=engine,
        ).loc[0, "beta"]
    except:
        beta = None

    data = data.sort_values("date_at").reset_index().drop(columns=["index"])
    data["log_return"] = np.log(data["close"]) - np.log(data["close"].shift(1))

    riskData = FillVaR(Data=data, Beta=beta).reset_index().drop(columns=["index"])
    riskData["stock_id"] = symbol_id
    if str(riskData.loc[0, "date_at"]) == yesterday:
        riskData.to_sql(
            "investment_stockrisk",
            schema="public",
            if_exists="append",
            index=False,
            con=engine,
        )


print(
    """
        """
)
print("The RiskData for {} was uploaded".format(today))
