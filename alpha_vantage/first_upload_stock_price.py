# -*- coding: utf-8 -*-
"""
Created on Thu Apr 20 19:07:23 2023

@author: bidzh
"""

from config.config import *
from alpha_vantage.download import *
from alpha_vantage.get_functions import getID
from tqdm import tqdm
import numpy as np
from datetime import datetime, date
import time


symbols = pd.read_sql(
    """select id, symbol from public.investment_company
                          where
                              1=1
                              and status = 'Active'
                              and date_upload = '2023-05-11'
                          order by symbol                          
                      """,
    con=engine,
)


price_non_200_status = []
price_empty_dict = []
non_uploaded = []
non_downloaded = []

for i in tqdm(symbols.index):
    symbol = symbols.loc[i, "symbol"]
    symbol_id = symbols.loc[i, "id"]

    try:
        data, data_code = alphaStockPrices(
            symbol=symbol, period="daily", outputsize="full", datatype="csv"
        )
    except:
        non_downloaded.append(symbol)
        continue

    time.sleep(0.2)
    if len(data) <= 2:
        price_empty_dict.append(symbol)
        continue

    if data_code != 200:
        price_non_200_status.append(symbol)
        continue

    data = data.rename(columns={"timestamp": "date_at"})
    # data = data[data['date_at'] > '2023-04-19']
    for col in ["open", "close", "high", "low", "volume"]:
        data[col] = pd.to_numeric(data[col])
    data["stock_id"] = symbol_id
    data["period_id"] = 1
    data["date_at"] = pd.to_datetime(data["date_at"])
    try:
        data.to_sql(
            "investment_stockprices",
            schema="public",
            if_exists="append",
            index=False,
            con=engine,
        )
        symbols.loc[i, "status"] = "upload"
    except:
        non_uploaded.append(symbol)
        symbols.loc[i, "status"] = "did not upload"


symbols = symbols.merge(pd.DataFrame(price_empty_dict, columns=["symbol"]), on="symbol")
