# This script contains the different strategies
# Currently here are two examples which are both used in the main algo.

import talib
import populate_database as db
import pandas as pd
from datetime import date


def ma_crossover(ma, time_period, server):
    #This function finds stocks to trade.
    #More accurately it finds stocks that have closed yesterday closed above/below 20EMA
    #If they pass here they will go on a watch list that from where the orders will be place if they make a new high.
    #It is written so that the EMA/SMA and time period can be changed on the fly.
    watchlist = []
        
    tickers = db.read_snp_tickers(server.serverSite).Symbol.tolist()
    
    for ticker in tickers:
                
        try:
            data = db.read_from_database("Select distinct date, ticker, uHigh, uLow, uClose from dailydata where ticker ='"+ ticker+"' ORDER BY date DESC limit "+str(time_period+100)+";", server.serverSite)
            
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
                logging.info("Found crossings for ", ticker)
                
            #Has the stock crossed below? Limiting to stocks over 10 USD
            if (data["EMA"+str(time_period)][0] > data.uClose[0] and data["EMA"+str(time_period)][1] < data.uClose[1]) and data.uHigh[0] > 10:
                watchlist.append([ticker,"sell",data.uLow[0],str(time_period)+"EMA"])
                logging.info("Found crossings for ", ticker)
                
        except:
            logging.info("Database fetch has failed for ticker ", ticker)
            
    #Returns an list of lists with ticker, enrty price and strategy          
    return watchlist

def hammer_doji(server):
    #The idea is to look at yesterdays candles, find hammes/dragonfly dojis/dojis and then initiate trade if we get a new high.
    watchlist = []
    
    tickers = db.read_snp_tickers(server.serverSite).Symbol.tolist()
    
    for ticker in tickers:
        
        try:
            #Get the latest data only
            data = db.read_from_database("Select distinct date, ticker,uOpen, uHigh, uLow, uClose from dailydata where ticker ='"+ ticker+"' ORDER BY date DESC limit 1;",server.serverSite)
            
            
            data["dojidf"] = talib.CDLDRAGONFLYDOJI(data.uOpen, data.uHigh, data.uLow, data.uClose)
            data["hammer"] = talib.CDLHAMMER(data.uOpen, data.uHigh, data.uLow, data.uClose)
            data["doji"] = talib.CDLDOJI(data.uOpen, data.uHigh, data.uLow, data.uClose)
            
            if (int(data.dojidf) == 100 | int(data.hammer) == 100 | int(data.doji) == 100):
                watchlist.append([ticker,"buy",data.uHigh[0],"H/D"])
                logging.info("Hd found" , ticker)
                
                
        except: 
            logging.info("Database fetch has failed for ticker", ticker)
    #Returns an list of lists with ticker, enrty price and strategy  
    return watchlist


def bb_cross(server):
    #This function looks at the bollinger bands. If a stock closes below or above the upper /lower bollinger band we will execute a trade if the trend is in our favour
    #We are using the 50SMA to look at the current trend of the stock.
    #I belive it would be pretty rare for this to find a buy.
    watchlist = []
        

    tickers = db.read_snp_tickers(server.serverSite).Symbol.tolist()
    
    for ticker in tickers:
                
        try:
            data = db.read_from_database("Select date, ticker,uOpen, uHigh, uLow, uClose from dailydata where ticker ='"+ ticker+"' ORDER BY date DESC limit 100;",server.serverSite)
            
            #Talib need the oldest data to be first     
            data = data.iloc[::-1]
        
            data["SMA50"] = talib.SMA(data.uClose, timeperiod = 50)
            data["BBupper"],data["BBmiddle"], data["BBlower"] = talib.BBANDS(data.uClose, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0) #Using default params

            
            #Has the stock peaked above Upper BB and we are downtrending? -> Short
            if data.BBupper[0] < data.uHigh[0] and data.SMA50[0] < data.SMA50[2]:
                watchlist.append([ticker,"sell", data.Low[0],"BB"])
                logging.info("Found BB crossings for ", ticker)
                
            #Has the stock peaked below lower BB and we are uptrending? -> Buy
            if data.BBlower[0] > data.uLow[0] and data.SMA50[0] > data.SMA50[2]:
                watchlist.append([ticker,"buy", data.uHigh[0],"BB"])
                logging.info("Found BB crossings for ", ticker)
                
        except:
            logging.info("Database fetch has failed for ticker ", ticker)
            
    #Returns an list of lists with ticker, enrty price and strategy          
    return watchlist


def week_cross(server, apis_delta, active_trades_delta):
    #This function looks for stocks that have made a 52 week high or low and compiles them to a watchlist.
    #If the stock has closed below of above the 52week mark on the daily, we initiate a trade the next day at a new high/low.
    watchlist = []
    
    #Latest quotes is fetched in the morning. We compare the 52 week high low from between it and what it is at close.
    highlowdata = db.read_from_database("Select ticker, week52High, week52Low from latestquotes;", server.serverSite)
    highlowdata.week52High =  highlowdata.week52High.astype("float64")
    highlowdata.week52Low =  highlowdata.week52Low.astype("float64")
    
    
    symbols = ""
    for ticker in list(highlowdata.ticker):
        symbols = symbols+","+ticker
    
    str_tickers = symbols[1:]
    
    latestClose = pd.read_csv("https://cloud.iexapis.com/stable/tops/last?symbols="+str_tickers+"%2b&format=csv&token="+apis_delta.iexKey)
    latestClose.columns = ["ticker","price","size","time","seq"]
    #Mergethe dfs. We make sute that the right close goes on the right ticker.
    
    highlowdata = pd.merge(highlowdata, latestClose ,how='outer', on = ["ticker"])
        
    highlowdata["highdiff"]= highlowdata.week52High - highlowdata.price
    highlowdata["lowdiff"]= highlowdata.week52Low - highlowdata.price
    
    
    for ticker in highlowdata.iterrows():
        
        #Looking for stocks above 52week high
        if ticker[1].highdiff < 0:
            watchlist.append([ticker[1].ticker,"buy", ticker[1].price,"Week"])
            logging.info("Found week52 crossings for ", ticker[1].ticker)
            
        #Looking for stocks below 52week low    
        if ticker[1].lowdiff > 0:
            watchlist.append([ticker[1].ticker,"sell", ticker[1].price,"Week"])
            logging.info("Found week52 crossings for ", ticker[1].ticker)
    
    #We dont want to add to an existing trade, thus we remove it from watch if we have already initiated a trade on it.
    
    if len(watchlist)>0:
        
        trade_tickers = [trade.ticker for trade in active_trades_delta]
        wl_tickers = [trade[0] for trade in watchlist]
        
        for ticker in wl_tickers:
            if ticker in trade_tickers:
                wl_tickers.remove(ticker)
                logging.info("Removed", ticker,"from week watchlist")
        
        

    #Returns an list of lists with ticker, enrty price and strategy          
    return watchlist



#techical analysis of the eft data
def etf_ta(etf_data):
    universe = ["SPY","EEM","GDX","XLF","QQQ","FXI","EFA","IWM","IAU","XLI","XLV","RSX","IYR","INDA","VGK"]
    
    #points contains the final results from this simple techical analysis. This will be used to reevaluate the portfolio position sizes.
    points = []
    
    for item in universe:
        symboldata = etf_data[etf_data.ticker == item]
        #Talib needs the oldest data to be first    
        symboldata = symboldata.iloc[::-1]
        
        #Using some technical indicators to determine its strength/weekness.
        symboldata["SMA5"] = talib.SMA(symboldata.uClose, timeperiod=5)
        symboldata["SMA10"] = talib.SMA(symboldata.uClose, timeperiod=10)
        symboldata["SMA20"] = talib.SMA(symboldata.uClose, timeperiod=20)
        symboldata["SMA50"] = talib.SMA(symboldata.uClose, timeperiod=50)
        symboldata["SMA100"] = talib.SMA(symboldata.uClose, timeperiod=100)
        symboldata["SMA200"] = talib.SMA(symboldata.uClose, timeperiod=200)
        
        symboldata["EMA5"] = talib.EMA(symboldata.uClose, timeperiod=5)
        symboldata["EMA10"] = talib.EMA(symboldata.uClose, timeperiod=10)
        symboldata["EMA20"] = talib.EMA(symboldata.uClose, timeperiod=20)
        symboldata["EMA50"] = talib.EMA(symboldata.uClose, timeperiod=50)
        symboldata["EMA100"] = talib.EMA(symboldata.uClose, timeperiod=100)
        symboldata["EMA200"] = talib.EMA(symboldata.uClose, timeperiod=200)
        
        symboldata["MACD"], symboldata["macdsignal"], symboldata["macdhist"] = talib.MACD(symboldata.uClose, fastperiod=12, slowperiod=26, signalperiod=9)
        symboldata["RSI"] = talib.RSI(symboldata.uClose, timeperiod = 14)
        
        #Comparing the movingaverages to the last close.
        movingaverages = pd.DataFrame(columns= ["madata","last_close","result"])
        movingaverages.madata = symboldata.iloc[-1,6:18]
        movingaverages.last_close = symboldata.iloc[-1,4]
        movingaverages.result= movingaverages.madata < movingaverages.last_close
        ma_points = sum(movingaverages.result) # out of 12
        
        #Comparing the momentum indicators to the last close.
        #RSI
        #Buying when its weak. Reducing size if its really strong.
        if symboldata.iloc[-1,21] < 30:
            mom_points = +2
        elif symboldata.iloc[-1,21] > 70:
            mom_points = -2
        else:
            mom_points= 0
            
        #MACD
        if symboldata.iloc[-1, 18] > 0:
            mom_points += 1
        else:
            mom_points -= 1
            
        sum_points = ma_points + mom_points
        points.append([item,sum_points, movingaverages.last_close[0]])
        points_df = pd.DataFrame(points, columns = ["symbol","points", "last_close"]).sort_values(by ="points", ascending = False)
        
    
    return points_df


def rebalance_index_positions(apis, server):
    #Rebalance index portfolio accoring to some metrics.. Used in echo
    #I have created an diverse universe of different ETFs that should cover a wide range of markets and commodities.
    #This is a better way of looking up the data, all at once instead of multiple SQL calls. TODO: update charlies datafetch strategies.

    today = date.today()
    #only fetching data that is one year old or newer.
    last_date = date(today.year -1 , today.month, today.day)
    #today_date = today.strftime("%Y-%m-%d")
    string_date  = last_date.strftime("%Y-%m-%d")

    #Get all the data
    etf_data = db.read_from_database("Select distinct date, ticker, uHigh, uLow, uClose, uVolume from etfdata where date >'"+ string_date+"' ORDER BY date DESC;", server.serverSite)

    points_df = etf_ta(etf_data)
    current_port_value = float(apis.alpacaApi.get_account().portfolio_value)

    #Calculate how much of the portfolio should be weighted for the etf in question.
    points_df["etfweight"] = points_df.points / sum(points_df.points)
    #Calculate how many shares I should have at the start of the day.
    points_df["sumshares"] = ((current_port_value * points_df.etfweight) / points_df.last_close).astype(int)

    current_port_balance = apis.alpacaApi.list_positions()

    #If there are current positions, compare the difference is weighting if not buy the whole lot.
    if len(current_port_balance) >0:
        
        current_bal = []
        for pos in current_port_balance:
            current_bal.append([pos.symbol,pos.qty, pos.side])
            
        current_bal_df = pd.DataFrame(current_bal, columns = ["symbol","qty","side"])
        
        merged_df = current_bal_df.merge(points_df, left_on = "symbol", right_on = "symbol")
        merged_df["posdifference"] = merged_df.sumshares - merged_df.qty.astype(int)
        
         
    else:
        merged_df = points_df[["symbol","sumshares"]]
        merged_df.columns = ["symbol","posdifference"]

    merged_df = merged_df[["symbol","posdifference"]]
        
    return merged_df




            
            
