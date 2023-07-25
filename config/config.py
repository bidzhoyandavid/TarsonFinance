# -*- coding: utf-8 -*-
"""
Created on Thu Apr  6 14:48:33 2023

@author: bidzh
"""

from sqlalchemy import create_engine
import psycopg2
from sshtunnel import SSHTunnelForwarder
# from alpaca.trading.client import TradingClient
# from alpaca.data.historical.stock import StockHistoricalDataClient


class AlphaVantage:
    API_KEY = 'UJBXF1Y11FJN58T0'
    BASE_URL = 'https://www.alphavantage.co/query'
    
# class AlpacaMarket:
#     API_KEY = 'PK6WNMH9CC4RTN464C5H'
#     SECRET_KEY = 'GSIVqqNlWOo7bYpvJTIlGWl34Z6WBfcuGgVLp6p4'
#     BASE_URL = 'https://api.alpaca.markets'
    
# class AlpacaSandbox:
#     API_KEY = 'CKVW5TK1MK7HW519I859'
#     SECRET_KEY = 'l2P59bVmDT3cA1UgC4PQ3jAygMVLmsxtTrABkmEZ'
#     BASE_URL = 'https://broker-api.sandbox.alpaca.markets'
    
    
# class PolygonMarket:
#     BASE_URL = 'https://api.polygon.io'
#     API_KEY = 'U_vXiK2uvih9qG3fXawV_835wpqVW_lj'
    
    
# trading_client = TradingClient(AlpacaMarket.API_KEY, AlpacaMarket.SECRET_KEY)
# stock_client = StockHistoricalDataClient(api_key = AlpacaMarket.API_KEY
#                                          , secret_key= AlpacaMarket.SECRET_KEY)




# server = SSHTunnelForwarder(
#     ('185.241.53.46', 22),
#     ssh_username="root",
#     ssh_password="Samarina$$321",
#     remote_bind_address=('127.0.0.1', 5432)
#     )

# server.start()
# local_port = str(server.local_bind_port)
engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format("postgres", "sql", "127.0.0.1", 5432, "tarsondb"))
