# This script contains the different strategies
# Currently here are two examples which are both used in the main algo.

import talib
import populate_database as db


def ma_crossover(ma, time_period, server):
    #This function finds stocks to trade.
    #More accurately it finds stocks that have closed yesterday closed above/below 20EMA
    #If they pass here they will go on a watch list that from where the orders will be place if they make a new high.
    #It is written so that the EMA/SMA and time period can be changed on the fly.
    watchlist = []
        
    tickers = db.read_snp_tickers(server.serverSite).Symbol.tolist()
    
    for ticker in tickers:
                
        try:
            data = db.read_from_database("Select date, ticker, uHigh, uLow, uClose from dailydata where ticker ='"+ ticker+"' ORDER BY date DESC limit "+str(time_period+100)+";", server.serverSite)
            
            #Talib need the oldest data to be first     
            data = data.iloc[::-1]
        
            if (ma == "SMA"): 
                data["SMA"+str(time_period)] = talib.SMA(data.uClose, timeperiod = time_period)
            elif (ma == "EMA"):
                data["EMA"+str(time_period)] = talib.EMA(data.uClose, timeperiod = time_period)
                #data["EMA"+str(time_period)] = data["uClose"].ewm(span=20, adjust = False).mean() # alternative method to count EMA
        
            
            #Has the stock crossed above?Limiting to stocks over 10 USD
            if (data["EMA"+str(time_period)][0] < data.uClose[0] and data["EMA"+str(time_period)][1] > data.uClose[1]) and data.uHigh[0] > 10:
                watchlist.append([ticker,"buy", data.uHigh[0],str(time_period)+"EMA"])
                print("Found ema crossings for ", ticker)
                
            #Has the stock crossed below? Limiting to stocks over 10 USD
            if (data["EMA"+str(time_period)][0] > data.uClose[0] and data["EMA"+str(time_period)][1] < data.uClose[1]) and data.uHigh[0] > 10:
                watchlist.append([ticker,"sell",data.uLow[0],str(time_period)+"EMA"])
                print("Found crossings for ", ticker)
                
        except:
            print("Database fetch has failed for ticker ", ticker)
            
    #Returns an list of lists with ticker, enrty price and strategy          
    return watchlist

def hammer_doji(server):
    #The idea is to look at yesterdays candles, find hammes/dragonfly dojis/dojix and then initiate trade if we get a new high.
    watchlist = []
    
    tickers = db.read_snp_tickers(server.serverSite).Symbol.tolist()
    
    for ticker in tickers:
        
        try:
            #Get the latest data only
            data = db.read_from_database("Select date, ticker,uOpen, uHigh, uLow, uClose from dailydata where ticker ='"+ ticker+"' ORDER BY date DESC limit 1;",server.serverSite)
            
            data["dojidf"] = talib.CDLDRAGONFLYDOJI(data.uOpen, data.uHigh, data.uLow, data.uClose)
            data["hammer"] = talib.CDLHAMMER(data.uOpen, data.uHigh, data.uLow, data.uClose)
            data["doji"] = talib.CDLDOJI(data.uOpen, data.uHigh, data.uLow, data.uClose)
            
            if (data.dojidf[0] == 100 | data.hammer[0] == 100 | data.doji[0] == 100):
                watchlist.append([ticker,"buy",data.uHigh[0],"H/D"])
                print("Hd found" , ticker)
                
        except: 
            print("Database fetch has failed for ticker", ticker)
    #Returns an list of lists with ticker, enrty price and strategy  
    return watchlist
            
            