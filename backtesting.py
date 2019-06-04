
##########################################
# Unfinished!
# This is a very crude script to test different cross over strategies. This is as good as you data is. 
# I'm going to test it just out whit daily data, however if you get your hands on more granular data, kudos.
# The way the scripts finds crossovers leaves a lot to be desired.

######################################

import pandas as pd
import time
import talib
import argparse

from populate_database import write_data_to_sql
from populate_database import read_from_database
#from populate_database import read_data_alpaca



sleeptime = 10

#Legacy, but if needed the dow tickers are still here.
#dowTickers = ["AXP","AAPL","BA","CAT","CSCO","CVX","DIS","DOW","GS",
#              "HD","IBM","INTC","JNJ","JPM","KO","MCD","MMM", "MRK",
#              "MSFT","NKE","PFE","PG","TRV","UNH","UTX","V","VZ","WBA",
#             "WMT","XOM"]



#Fetch data for you backtest.
def read_data_daily(apikey,tickers = dowTickers, outputsize = "full", saveLatestOnly = False):
    rawStockDataDaily = pd.DataFrame()

    for ticker in tickers:
        retry = True
        retry_counter = 0
        
        while retry:
            tempRawStockDataDaily = pd.read_csv("https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED"+
                                                "&symbol="+ ticker+
                                                "&outputsize="+outputsize+
                                                "&datatype=csv"+
                                                "&apikey="+ apikey)
            
            #Check that the data is correct
            if tempRawStockDataDaily.shape[1] == 9:
                 #Write the ticker name to the df to keep track of what data belong where.
                tempRawStockDataDaily["ticker"] = ticker
                print("Fetched " + ticker + " daily data" )
                
                #Saving the reults for this ticker and moving on to the following ticker.
                if saveLatestOnly:
                    rawStockDataDaily = rawStockDataDaily.append(tempRawStockDataDaily.iloc[0:])
                else:
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



def find_ma_crossovers(serverSite):
    crossovers = []
    
    bt_tickers = read_from_database("SELECT DISTINCT ticker FROM backtestdata;",serverSite).ticker.tolist()
    
    for ticker in bt_tickers:
        bt_data = read_from_database("SELECT ticker, timestamp, open, high ,low, close, volume FROM backtestdata WHERE ticker ='"+ticker+"';",serverSite)
        
        #Talib need the oldest data to be first 
        bt_data = bt_data.iloc[::-1]
        bt_data["EMA"+str(9)] = talib.EMA(bt_data.close, timeperiod = 9)
        bt_data["EMA"+str(20)] = talib.EMA(bt_data.close, timeperiod = 20)
        bt_data["EMA"+str(50)] = talib.SMA(bt_data.close, timeperiod = 50)
        bt_data["EMA"+str(100)] = talib.SMA(bt_data.close, timeperiod = 100)
        bt_data["EMA"+str(200)] = talib.SMA(bt_data.close, timeperiod = 200)
        bt_data["MACD"],bt_data["MACDsignal"], bt_data["MACDhist"] = talib.MACD(bt_data.close) #Using default params
        bt_data["RSI"] = talib.RSI(bt_data.close) #Using default params
        bt_data["ATR"] = talib.ATR(bt_data.high, bt_data.low,bt_data.close)
        bt_data["TATR"] = talib.TRANGE(bt_data.high, bt_data.low,bt_data.close)
        bt_data["BBUpper"],bt_data["BBmiddle"], bt_data["BBlower"] = talib.MACD(bt_data.close) #Using default params
        bt_data["dojiDH"] = talib.CDLDRAGONFLYDOJI(bt_data.open, bt_data.high, bt_data.low, bt_data.close)
        bt_data["hammer"] = talib.CDLHAMMER(bt_data.open, bt_data.high, bt_data.low, bt_data.close)
        bt_data["CDLDOJI"] = talib.CDLDOJI(bt_data.open, bt_data.high, bt_data.low, bt_data.close)
        
        #Dropping NAs
        bt_data.dropna(inplace = True)
        
        #Find the crossovers
        #Extremely bad solution. Im sure there are better ways
        crossAbove = []
        crossBelow = []
        for key,  row in enumerate(bt_data.iterrows()):
            
            if(bt_data.iloc[key,5] > bt_data.iloc[key,8] and bt_data.iloc[key-1,5] < bt_data.iloc[key-1,8]):
                crossAbove.append(True)
            else:
                crossAbove.append(False)
        
        
        
        for key,  row in enumerate(bt_data.iterrows()):                
            
            if(bt_data.iloc[key,5] < bt_data.iloc[key,8] and bt_data.iloc[key-1,5] > bt_data.iloc[key-1,8]):
                crossBelow.append(True)

            else:
                crossBelow.append(False)
                
        bt_data["crossBelow"] = crossBelow
        bt_data["crossAbove"] = crossAbove
        
        
        class Trade:
            def __init__(self,ticker, targetEntry, stoploss, crossrow, active = True):
                self.ticker = ticker
                self.targetEntry = targetEntry                
                self.actualEntry = 0
                self.stoploss = stoploss
                self.target = 0
                self.crossrow = crossrow
                self.searching_for_entry = True
                self.active = active
                
                
            
            def tradeFoundLong(self,actualEntry):
                self.actualEntry = actualEntry
                self.target = self.actualEntry + (self.actualEntry- self.stoploss) * 2
                self.searching_for_entry = False
                
                
                #print(self.entry, self.target, self.stoploss)
            
            def tradeFoundShort(self,actualEntry):
                self.actualEntry = actualEntry
                self.target = self.actualEntry - (self.stoploss - self.actualEntry) * 2
                self.searching_for_entry = False
            
            def deactivateTrade(self):
                self.active = False
                
            
        #Looking at the longs!
        #Check if hit stoploss or target
        count_target = 0
        count_stop = 0
        count_started_trade = 0
        active_trade = Trade("Ticker",1,1,1, active = False) #Dummy trade, not important for result but excecution needs once to start with.
        
        for key, row in enumerate(bt_data.iterrows()):

            if (row[1].crossAbove):
                active_trade = Trade(ticker, targetEntry = row[1].high, stoploss = row[1].EMA20 , crossrow = row[0])
                print("Started search at row",row[0])
              
            #Enter at open if there has been a big gap durin pre and post market, othervise assume we enter at the highs
            if (active_trade.targetEntry < row[1].high and active_trade.searching_for_entry and active_trade.active):
                if (row[1].open > active_trade.targetEntry):      
                    active_trade.tradeFoundLong(actualEntry = row[1].open)  
                else:
                    (active_trade.tradeFoundLong(actualEntry = active_trade.targetEntry))
                    
                print("Started trade at row", row[0], "from", active_trade.crossrow)
                count_started_trade += 1 
            
            #Assume if it the candle hits out target we get out
            if (active_trade.target < row[1].high and active_trade.searching_for_entry == False and active_trade.active):
                active_trade.deactivateTrade()
                crossovers.append([active_trade.ticker, active_trade.crossrow,"Target"])
                print("Target met at row", row[0], " from row ", active_trade.crossrow)
                count_target += 1
            
            #If the candle closes above our stop, stop out
            if (active_trade.stoploss > row[1].close and active_trade.searching_for_entry == False and active_trade.active):
                active_trade.deactivateTrade()
                crossovers.append([active_trade.ticker, active_trade.crossrow,"Stoploss"])
                print("Stoploss met at row", row[0], " from row ", active_trade.crossrow)
                count_stop += 1
            
            
            
        
        print("Trades started:",count_started_trade ,"Stops:", count_stop, "Targets:",count_target,"from total", sum(bt_data.crossAbove))
        
        
        #Shorts!!        
        count_target = 0
        count_stop = 0
        count_started_trade = 0
        active_trade = Trade("Ticker",1,1,1, active = False) #Dummy trade, not important for result but excecution needs once to start with.
        
        for key, row in enumerate(bt_data.iterrows()):

            if (row[1].crossBelow):
                active_trade = Trade(ticker,targetEntry = row[1].low, stoploss = row[1].EMA20 , crossrow = row[0])
                print("Started search at row",row[0])
              
            #Enter at open if there has been a big gap durin pre and post market, othervise assume we enter at the lows        
            if (active_trade.targetEntry < row[1].low and active_trade.searching_for_entry and active_trade.active):
                if (row[1].open < active_trade.targetEntry):      
                    active_trade.tradeFoundShort(actualEntry = row[1].open)  
                else:
                    (active_trade.tradeFoundShort(actualEntry = active_trade.targetEntry))
                    
                print("Started trade at row", row[0], "from", active_trade.crossrow)
                count_started_trade += 1 
            
            #Assume if it the candle hits out target we get out
            if (active_trade.target < row[1].low and active_trade.searching_for_entry == False and active_trade.active):
                active_trade.deactivateTrade()
                crossovers.append([active_trade.ticker, active_trade.crossrow,"Target"])
                print("Target met at row", row[0], " from row ", active_trade.crossrow)
                count_target += 1
            
            #If the candle closes above our stop, stop out
            if (active_trade.stoploss < row[1].close and active_trade.searching_for_entry == False and active_trade.active):
                active_trade.deactivateTrade()
                crossovers.append([active_trade.ticker, active_trade.crossrow,"Stoploss"])
                print("Stoploss met at row", row[0], " from row ", active_trade.crossrow)
                count_stop += 1
            
            
            
        
        print("Trades started:",count_started_trade ,"Stops:", count_stop, "Targets:",count_target,"from total", sum(bt_data.crossAbove))
          
    return crossovers

def main(serverSite,apikey):
    stockdata = read_data_daily(apikey,tickers =  ["AAPL"])
    write_data_to_sql(stockdata, "backtestdata", if_exists = "replace", serverSite = serverSite)
    
    #stockdata15min = read_data_alpaca(pd.Series(dowTickers), "15Min")
    #write_data_to_sql(stockdata, "backtestdata15min", if_exists = "append")
    
    
    #Do stuff with this!
    crossovers = find_ma_crossovers(server.serverSite)

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-su","--serverUser", help= "server username", required = True, type = str)
    parser.add_argument("-sp","--serverPass", help= "server password", required = True, type = str)
    parser.add_argument("-sa","--serverAddress", help= "server addres",required = True, type = str)
    parser.add_argument("-db","--database", help= "the name of your database",required = True, type = str)
    parser.add_argument("-ak","--alphaKey", help= "the free alphavantge apikey",required = True, type = str)
    
    args = parser.parse_args()
    serverSite = str("mysql+pymysql://"+args.serverUser+":"+args.serverPassword+args.serverAddress+":3306/"+args.database)
    apikey = args.alphaKey
    
    main(serverSite,apikey)
    

