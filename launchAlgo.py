#When the database is populated we can analyse the stocks and send trade orders.
#The script also fetches new data on a daily basis

import alpaca_trade_api as tradeapi
import pandas as pd
import time
import sqlalchemy

from populate_database import read_data_daily
from populate_database import write_data_to_sql
from populate_database import read_from_database

#Database details
from populate_database import serverSite


#Incoming..
import talib

#Alpaca tradeApi
api = tradeapi.REST(
    key_id="",
    secret_key="",
    base_url="https://paper-api.alpaca.markets"
)

sleepBetweenCalls = 10

def getLastData():
    query = """SELECT timestamp FROM dailydata ORDER BY timestamp  DESC limit 1;"""
    engine = sqlalchemy.create_engine(serverSite)
    lastData = pd.read_sql(query, engine)
    return lastData.iloc[0,0]

    
class Trade:
    def __init__(self, ticker, posSize, orderSide, timeStamp):
        print("An trade has been created")
        self.ticker = ticker
        self.posSize = posSize
        self.orderSide = orderSide
        self.timeStamp = timeStamp
        self.orderID = 0
        self.entryPrice = 0
        self.costBasis = 0
        self.unrealPL = 0
        self.unrealPLprocent = 0
        
    def submitOrder(self):
        order = api.submit_order(symbol = self.ticker,
                         qty = self.posSize,
                         side = self.orderSide,
                         type = "market",
                         time_in_force = "day")
        print("An order has been submitted for ", self.ticker, " qty: ", self.posSize)
        self.orderID = order.id
        
        order = api.get_order(self.orderID)
        self.entryPrice = order.filled_avg_price
        
        
        
        
    def cancelOrder(orderID):
        api.cancel_orderorder(orderID)
        
    def flattenOrder(self):
        flattenSide = ""
        if (self.orderSide == "buy"):
            flattenSide = "sell"
        else: flattenSide == "buy"
        
        api.submit_order(symbol = self.ticker,
                         qty = self.posSize,
                         side = flattenSide,
                         type = "market",
                         time_in_force = "day")
        print("An flatten order has been submitted for ", self.ticker, " qty: ", self.posSize)

    
    def getPosition(self):
        order = api.get_position(self.ticker)
        self.costBasis = order.cost_basis
        self.unrealPL = order.unrealized_pl
        self.unrealPLprocent = order.unrealized_plpc

def search_new_data(now):
    
        if api.get_clock == False and "12:00" in str(now):
            #Read the latest data saved in the database:
            lastData = getLastData()
            #Read the latest possible data to get. Just testing with aapl, any ticker will do.
            latestData = read_data_daily(tickers = ["AAPL"], outputsize = "compact").iloc[0:0]
                        
            #If the lastdata and latestdata is not the same and the time is 12:00 and market is closed we can search for new data!
            if lastData != latestData:  
                tickers = read_from_database("""SELECT Symbol  
                                           FROM fiscdata""")
                newStockData = read_data_daily(tickers, outputsize = "compact", saveLatestOnly = True)
                write_data_to_sql(newStockData, "dailydata", if_exists = "append")
                

def ma_crossing(ma, time_period):
    #This function finds stocks to trade.
    #More accurately it finds stocks that have closed yesterday closed above/below 20EMA
    #If they pass here they will go on a watch list that from where the orders will be place if they make a new high.
    #It is written so that the EMA/SMA and time period can be changed on the fly.
    watchlist = []
        
    tickers = read_from_database("""SELECT Symbol  
                                    FROM fiscdata""")["Symbol"].tolist()
    for ticker in tickers:
                
        try:
            #Only taking the time_period + 10 entries. No need to fetch all of the data.
            data = read_from_database("Select timestamp, ticker, high, low, close from dailydata where ticker ='"+ ticker+"' ORDER BY timestamp DESC limit "+str(time_period+10)+";")
            
            #Talib need the oldest data to be first     
            data = data.iloc[::-1]
        
            if (ma == "SMA"): 
                data["SMA"+str(time_period)] = talib.SMA(data.close, timeperiod = time_period)
            elif (ma == "EMA"):
                data["EMA"+str(time_period)] = talib.EMA(data.close, timeperiod = time_period)
        
            
            #Has the stock crossed above?Limiting to stocks over 10 USD
            if (data["EMA"+str(time_period)][0] < data.close[0] and data["EMA"+str(time_period)][1] > data.close[1]) and data.high[0] > 10:
                watchlist.append([ticker,"buy", data.high[0]])
                print("Found crossings for ", ticker)
                
            #Has the stock crossed below? Limiting to stocks over 10 USD
            if (data["EMA"+str(time_period)][0] > data.close[0] and data["EMA"+str(time_period)][1] < data.close[1]) and data.high[0] > 10:
                watchlist.append([ticker,"sell",data.low[0]])
                print("Found crossings for ", ticker)
                
            print("Found no crossings for ", ticker)
        except:
            print("Database fetch has failed for ticker ", ticker)
                
    return watchlist
                
def get_watchlist_price(watchlist_df):
     
    watchlist_bars = api.get_barset(watchlist_df.ticker,'minute',limit = 1).df
    
    #The API that returns real time data is not perfect in my opinion. 
    #It returns the last minute candle that the given tickes has traded, not neccecarliy the latest
    #This has led me to do this wierd contraption where i fill the df's all NANs with the mean, and then I can just transpose it and take the first column
    #Since all the rows contain the same values after the fill.na with mean. 
    #You can set the start and end dates on the API call but I'm not sure if it supports minutes, since that is what I would be interested in.
    #This should not have been a problem but it it was

    watchlist_bars = watchlist_bars.fillna(watchlist_bars.mean())
    
    #We only want the "close" values, this is the first way I could come up with. Surely there is better.
    close_columns = [col for col in watchlist_bars.columns if "close" in col]
     
    close_values = watchlist_bars[close_columns].transpose().iloc[:,0]
    
    watchlist_df["current_price"] = list(close_values)
    watchlist_df["price_difference"] = watchlist_df["price"]- watchlist_df["current_price"]
    
    found_trades_long = []
    found_trades_short = []
    
    longs = watchlist_df[watchlist_df["side"].str.match("buy")]
    shorts = watchlist_df[watchlist_df["side"].str.match("sell")]
    
    for index, stock in longs.iterrows():
        if (stock["price_difference"] < 0 ):
            found_trades_long.append(stock["ticker"])
            

    for index, stock in shorts.iterrows():
        if (stock["price_difference"] > 0):
            found_trades_short.append(stock["ticker"])

    return found_trades_long, found_trades_short


#Define number of shares here
def fire_orders_ema_cross(trades, side, now, time_period, ema_time_period):
    #stop_prices = []
    #Find stop prices for the trades.
    #for trade in trades:
    #    data = read_from_database("Select timestamp, ticker, high, low, close from dailydata where ticker ='"+ trade+"' ORDER BY timestamp DESC limit "+str(ema_time_period+100)+";")
            
        #Talib need the oldest data to be first     
    #    data = data.iloc[::-1]
        
        #Setting the stop price to the 20EMA
   #     data["stop_price"] = talib.EMA(data.close, timeperiod = ema_time_period)
   #     stop_prices.append(data.stop_price[0])
    
    
    for trade in trades:
        try:
            new_trade = Trade(trade, 20, side, now)
            new_trade.submitOrder()
        except:
            print("Trade failed for ",trade)
    
    
def main():
    ema_time_period = 20
    
    while True: 
        
        clock = api.get_clock()
        now = clock.timestamp
        search_new_data(now)

        
        #Create watchlist before market opens.
        #if (api.get_clock == False and "9:00" in str(now)):
        col_lables = ["ticker","side","price"]
        watchlist = pd.DataFrame(ma_crossing("EMA", ema_time_period),columns = col_lables).sort_values("ticker")
        write_data_to_sql(pd.DataFrame(watchlist),"watchlist")
        
        #Trade!
        while api.get_clock().is_open:
            #Loop trough watchlist and check if the value has been crossed. 
            #Can I fetch all of them together?
            found_trades_long, found_trades_short = get_watchlist_price(watchlist)
            fire_orders_ema_cross(found_trades_long, "buy", str(now),ema_time_period)
            fire_orders_ema_cross(found_trades_short, "sell", str(now),ema_time_period)
            
            remove_from_watchlist = found_trades_long + found_trades_short
            
            watchlist = watchlist[~watchlist.ticker.str.contains('|'.join(remove_from_watchlist))]
            
            
            
            
            print("Waiting for orders")
                  
            time.sleep(sleepBetweenCalls)
            
        time.sleep(sleepBetweenCalls)
    

if __name__ == "__main__":
    main()   

