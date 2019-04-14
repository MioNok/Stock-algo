#When the database is populated we can analyse the stocks and send trade orders.
#The script also fetches new data on a daily basis

import alpaca_trade_api as tradeapi
import pandas as pd
import time
import sqlalchemy

from populate_database import read_data_daily
from populate_database import write_data_to_sql
from populate_database import read_snp_tickers

#Database details
from populate_database import serverSite


#Incoming..
#import talib

#Alpaca tradeApi
api = tradeapi.REST(
    key_id="Api_key",
    secret_key="Secret_key",
    base_url="https://paper-api.alpaca.markets"
)

sleepBetweenCalls = 10

def getLastData():
    query = """SELECT timestamp FROM dailydata ORDER BY timestamp  DESC limit 1;"""
    engine = sqlalchemy.create_engine(serverSite)
    lastData = pd.read_sql(query, engine)
    return lastData.iloc[0,0]
    

def main():
    
    while True: 
        
        clock = api.get_clock()
        now = clock.timestamp
        
        if api.get_clock == False and "12:00" in str(now):
            #Read the latest data saved in the database:
            lastData = getLastData()
            #Read the latest possible data to get. Just testing with aapl, any ticker will do.
            latestData = read_data_daily(tickers = ["AAPL"], outputsize = "compact").iloc[0:0]
                        
            #If the lastdata and latestdata is not the same and the time is 12:00 and market is closed we can search for new data!
            if lastData != latestData:  
                tickers = read_snp_tickers()
                newStockData = read_data_daily(tickers, outputsize = "compact", saveLatestOnly = True)
                write_data_to_sql(newStockData, "dailydata", if_exists = "append")
                
    time.sleep(sleepBetweenCalls)