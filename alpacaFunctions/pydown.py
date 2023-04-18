# -*- coding: utf-8 -*-
"""
Created on Mon Apr 10 17:45:36 2023

@author: bidzh
"""

from config.config import *
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetClass
from alpaca.data.timeframe import TimeFrame
import pandas as pd
from alpaca.data.enums import Adjustment, DataFeed
from alpaca.data.requests import StockBarsRequest, StockQuotesRequest, StockQuotesRequest
from typing import Optional, Union
from uuid import UUID
from alpaca.data.historical.stock import StockHistoricalDataClient
import requests as re

