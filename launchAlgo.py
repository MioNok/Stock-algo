#When the database is populated we can analyse the stocks and send trade orders.

import alpaca_trade_api as tradeapi
import pandas as pd
import time
import talib

NY = 'America/New_York'
api = tradeapi.REST(
    key_id='InsertKey',
    secret_key='InserKey',
    base_url='https://paper-api.alpaca.markets'
)



#This is from the alpaca site..work in progress.
def _get_prices(symbols, end_dt, max_workers=5):
    '''Get the map of DataFrame price data from Alpaca's data API.'''

    start_dt = end_dt - pd.Timedelta('50 days')
    start = start_dt.strftime('%Y-%m-%d')
    end = end_dt.strftime('%Y-%m-%d')

    def get_barset(symbols):
        return api.get_barset(
            symbols,
            'day',
            limit = 50,
            start=start,
            end=end
        )

    # The maximum number of symbols we can request at once is 200.
    barset = None
    idx = 0
    while idx <= len(symbols) - 1:
        if barset is None:
            barset = get_barset(symbols[idx:idx+200])
        else:
            barset.update(get_barset(symbols[idx:idx+200]))
        idx += 200

    return barset.df

def prices(symbols):
    '''Get the map of prices in DataFrame with the symbol name key.'''
    now = pd.Timestamp.now(tz=NY)
    end_dt = now
    if now.time() >= pd.Timestamp('09:30', tz=NY).time():
        end_dt = now - \
            pd.Timedelta(now.strftime('%H:%M:%S')) - pd.Timedelta('1 minute')
    return _get_prices(symbols, end_dt)






def getLatestData():
    def read_data_daily_and_ti(tickers = dowTickers):
    rawStockDataDaily = pd.DataFrame()

    for ticker in tickers:
        retry = True
        retry_counter = 0
        
        while retry:
            tempRawStockDataDaily = pd.read_csv("https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED"+
                                                "&symbol="+ ticker +
                                                "&outputsize=compact"+
                                                "&datatype=csv"+
                                                "&apikey="+ apikey[key_counter])
            
            #Check that the data is correct
            if tempRawStockDataDaily.shape[1] == 9:
                 #Write the ticker name to the df to keep track of what data belong where.
                tempRawStockDataDaily["ticker"] = ticker
                print("Fetched " + ticker + " daily data" )
                keyCounter()
                
                #Saving the reults for this ticker and moving on to the following ticker.
                rawStockDataDaily = rawStockDataDaily.append(tempRawStockDataDaily)
                
                #Free API key only gets you so far, as of writing this alphavantage is limiting the amount of API calls you can make in a minute..
                time.sleep(sleeptime)
                retry = False
                
            else: 
                #Fetch has failed. Think about what you have done and try again.
                retry_counter += 1 # counting the retrys, if above 10, stop pining the server, its not happening.
                print("Retrying to fetch ", ticker)
                
                if retry_counter == 5:
                    print("The fetch has failed 5 times in a row, something is wrong with the server, your api call or your key. Jumping over this one.")
                    retry = False
                #Lets try again.    
                else: 
                    retry = True
                time.sleep(sleeptime)
            
      
    return rawStockDataDaily

def main():
    
    while True:
        
        price_df = prices(["AAPL"])
        applsma = talib.SMA(price_df.iloc(3),200)