#When the database is populated we can analyse the stocks and send trade orders.

import pandas as pd
import time
import talib
import argparse
import alpaca_trade_api as tradeapi

#Currently unused, but as backup.
#from populate_database import read_data_alpaca
from populate_database import get_iex_data
from populate_database import write_data_to_sql
from populate_database import read_snp_tickers
from populate_database import read_from_database

#Database details
from populate_database import db_main



#Arguments
#Must haves
parser = argparse.ArgumentParser()
parser.add_argument("serverUser", help= "server username", type = str)
parser.add_argument("serverPass", help= "server password", type = str)
parser.add_argument("serverAddress", help= "server addres", type = str)
parser.add_argument("database", help= "the name of your database", type = str)
parser.add_argument("alpacaKey", help= "alpaca api key", type = str)
parser.add_argument("alpacaSKey", help= "alpaca secret api key", type = str)
parser.add_argument("iexKey", help= "Iex api key", type = str)
   
#Optional 
parser.add_argument("-s","--startup",help="fetches 3m data if present", action='store_true')
parser.add_argument("-pv","--posSize", help= "Max position size of a stock, default is  500", nargs = "?", default = 500, type = int)
parser.add_argument("-ps","--posValue", help= "Max value of a position of a stock, default is 5000", nargs = "?", default = 5000, type = int)

args = parser.parse_args()



#Database variables    
serverUser = args.serverUser    
serverPass = args.serverPass
serverAddress = args.serverAddress
database = args.database
alpacaKey = args.alpacaKey
alpacaSKey = args.alpacaSKey
iexKey = args.iexKey

#Position variables
startup = args.startup    
maxPosSize = args.posSize
maxPosValue = args.posValue


#Can edit if needed.
sleepBetweenCalls = 10



class Server:
    def __init__(self, user, password, address, database):
        self.user = user
        self.password = password
        self.address = address
        self.database = database
        self.serverSite = str("mysql+pymysql://"+self.user+":"+self.password+self.address+":3306/"+self.database)
        
class APIs:
    def __init__(self, alpacaKey, alpacaSKey, iexKey):
        self.alpacaKey = alpacaKey
        self.alpacaSkey = alpacaSKey
        self.iexKey = iexKey
        
        self.alpacaApi = tradeapi.REST(
                       key_id = self.alpacaKey,
                       secret_key = self.alpacaSkey,
                       base_url="https://paper-api.alpaca.markets"
                       ) 

#Global variables
server = Server(user = serverUser, password = serverPass, address = serverAddress, database = database)
apis = APIs(alpacaKey, alpacaSKey, iexKey)   


#serverPass = "defaultpass" #insert your mysql serverpassword
#serverUser = "rootUser" # your mysql serverpassword
#database = "stockdata" #database in your mysql you want to use. Need to be setup before running (Create DATABASE DatabaseName)
#serverAddress = "@testinstance.cqqzgxgyyebv.us-east-1.rds.amazonaws.com"

class Trade:
    def __init__(self, ticker, posSize, orderSide, timeStamp, strategy):
        print("An trade has been created")
        self.ticker = ticker
        self.posSize = posSize
        self.orderSide = orderSide
        self.timeStamp = timeStamp
        self.stopPrice = 0
        self.orderID = 0
        self.entryPrice = 0
        self.currentPrice = 0
        self.costBasis = 0
        self.unrealPL = 0
        self.unrealPLprocent = 0
        self.targetPrice = 0
        self.last15MinCandle = None
        self.strategy = strategy
        
    def submitOrder(self):
        order = apis.alpacaApi.submit_order(symbol = self.ticker,
                         qty = self.posSize,
                         side = self.orderSide,
                         type = "market",
                         time_in_force = "day")
        print("An order has been submitted for ", self.ticker, " qty: ", self.posSize)
        self.orderID = order.id
        self.updateTradeDb(action = "Initiated trade", initiated = True)
        
        
    def cancelOrder(orderID):
        apis.alpacaApi.cancel_orderorder(orderID)
        
    def setStopPrice(self,stopPrice):
        self.stopPrice = float(stopPrice)
    
    def setLastCandle(self, candle):
        self.last15MinCandle = candle
        
    def flattenOrder(self, action):
        flattenSide = ""
        if (self.orderSide == "buy"):
            flattenSide = "sell"
        else: flattenSide == "buy"
        
        #Save current trade specs.
        self.setPosition()
        
        apis.alpacaApi.submit_order(symbol = self.ticker,
                         qty = self.posSize,
                         side = flattenSide,
                         type = "market",
                         time_in_force = "day")
        print("An flatten order has been submitted for ", self.ticker, " qty: ", self.posSize)
        self.updateTradeDb(action = action, initiated = False)

    
    def setPosition(self):
        pos = apis.alpacaApi.get_position(self.ticker)
        self.costBasis = pos.cost_basis
        self.unrealPL = pos.unrealized_pl
        self.unrealPLprocent = pos.unrealized_plpc
        self.entryPrice = float(pos.avg_entry_price)
        self.currentPrice = float(pos.current_price)
        
        #Keeping the history of all trades.
    def updateTradeDb(self, action, initiated):
        now = str(apis.alpacaApi.get_clock().timestamp)[0:19]

        #Different update depending on if the order was initiated of flattend.
        if (initiated == False):
            dfData = {"Timestamp": [now],
                      "Ticker": [self.ticker],
                      "Size":[self.posSize],
                      "Side":[self.orderSide],
                      "Strategy":[self.strategy],
                      "Action":[action],
                      "Result":[self.unrealPL]}

        if (initiated):
            dfData = {"Timestamp": [now],
                      "Ticker": [self.ticker],
                      "Size":[self.posSize],
                      "Side":[self.orderSide],
                      "Strategy":[self.strategy],
                      "Action":[action],
                      "Result":["init"]}
        
        tradedb = pd.DataFrame(data = dfData)
        
        write_data_to_sql(tradedb,"tradehistory",if_exists = "append", serverSite = server.serverSite)

def ma_crossing(ma, time_period):
    #This function finds stocks to trade.
    #More accurately it finds stocks that have closed yesterday closed above/below 20EMA
    #If they pass here they will go on a watch list that from where the orders will be place if they make a new high.
    #It is written so that the EMA/SMA and time period can be changed on the fly.
    watchlist = []
        
    tickers = read_snp_tickers(server.serverSite).Symbol.tolist()
    
    for ticker in tickers:
                
        try:
            data = read_from_database("Select date, ticker, uHigh, uLow, uClose from dailydata where ticker ='"+ ticker+"' ORDER BY date DESC limit "+str(time_period+100)+";", server.serverSite)
            
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
                
    return watchlist

def find_hammer_doji():
    #The idea is to look at yesterdays candles, find hammes/dragonfly dojis and then initiate trade if we get a new high.
    watchlist = []
    
    tickers = read_snp_tickers(server.serverSite).Symbol.tolist()
    
    for ticker in tickers:
        
        try:
            #Get the latest data only
            data = read_from_database("Select date, ticker,uOpen, uHigh, uLow, uClose from dailydata where ticker ='"+ ticker+"' ORDER BY date DESC limit 1;",server.serverSite)
            
            data["doji"] = talib.CDLDRAGONFLYDOJI(data.uOpen, data.uHigh, data.uLow, data.uClose)
            data["hammer"] = talib.CDLHAMMER(data.uOpen, data.uHigh, data.uLow, data.uClose)
            
            if (data.doji[0] == 100 | data.hammer[0] == 100):
                watchlist.append([ticker,"buy",data.uHigh[0],"H/D"])
                print("Hd found" , ticker)
                
        except: 
            print("Database fetch has failed for ticker", ticker)
    
    return watchlist
            
            
            
            
            
def get_watchlist_price(watchlist_df):
     
    watchlist_bars = apis.alpacaApi.get_barset(watchlist_df.ticker,'minute',limit = 1).df
    
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
def fire_orders(trades, side, now, time_period, strategy):
    
    current_bp = int(float(apis.alpacaApi.get_account().buying_power))

    succesful_trades = []
    for trade in trades:
        try:
            
            postValue = maxPosValue
            posSize = maxPosSize
    
            #Setting max pos size. Either trade value is 8000 or 500 shares. Which ever is bigger.
            current_price = apis.alpacaApi.get_barset(trade,"1Min",limit = 1).df.iloc[0,3]
            if (current_price * posSize >postValue):
                posSize = int(maxPosValue/current_price)
                
            #Check buying power, if not enough brake loop.    
            if(current_bp < current_price + posSize):
                print("No buying power")
                break
            
            live_trade = Trade(trade, posSize, side, now, strategy)
            
            
            live_trade.submitOrder()
            succesful_trades.append(live_trade)
        except:
            print("Trade failed for ",trade)
    return succesful_trades



def current_active_trade_prices(current_trades):
    
    #Get the latest 15min candle. Future trade decisions is made on the OHLC on it.
    for trade in current_trades:
        current_candle = apis.alpacaApi.get_barset(trade.ticker,"15Min",limit = 1).df
        trade.setLastCandle(current_candle)
        
        

def check_stoploss(current_trades,ema_time_period):
    #Note to self. Search all data at once, not every stock for themself.
    #Find stop prices for the trades.
    for trade in current_trades:
        if (trade.stopPrice == 0) :
            data = read_from_database("Select date, ticker, uHigh, uLow, uClose from dailydata where ticker ='"+ trade.ticker+ "' ORDER BY date DESC limit "+str(ema_time_period+10)+";",server.serverSite)
            
            #Talib need the oldest data to be first     
            data = data.iloc[::-1]
        
            #Setting the stop price to the 20EMA
            data["stop_price"] = talib.EMA(data.uClose, timeperiod = ema_time_period)
            trade.setStopPrice(data.stop_price[0])
            print("Stop price for ", trade.ticker," is set to ", trade.stopPrice)
        else:
            #Get the close price of the last 5 minute candle and comapre it against the stop price
            #If the 5min candle has closed above the stop price, it will flatten the trade.
            #current_trade_price = apis.alpacaApi.get_barset(trade.ticker,"5Min",limit = 1).df.iloc[0,3]
            current_trade_price = trade.last15MinCandle.iloc[0,3]
            if (current_trade_price > trade.stopPrice and trade.orderSide == "sell"):
                trade.flattenOrder(action = "Stoploss")
                current_trades.remove(trade)
            if (current_trade_price < trade.stopPrice and trade.orderSide == "buy"):
                trade.flattenOrder(action = "Stoploss")
                current_trades.remove(trade)
                
                
    return current_trades

def check_target(current_trades):
    #Set target price, and check if target price, current target is 2:1
    for trade in current_trades:
        if (trade.targetPrice  == 0):
            
            #update the current position info, sleep for a while so that the orders  have time get filled.
            time.sleep(5)
            trade.setPosition()
            
            if(trade.orderSide == "buy"):
                trade.targetPrice = trade.entryPrice + ((trade.entryPrice - trade.stopPrice)*2) 
            else:
                trade.targetPrice = trade.entryPrice - ((trade.stopPrice - trade.entryPrice)*2)
            
            print("Target price for ", trade.ticker," is set to ", trade.targetPrice)
        else:
            #Close the trade if the 1min candle high has hit the target
            current_trade_price = trade.last15MinCandle.iloc[0,1]
            if (current_trade_price > trade.stopPrice and trade.orderSide == "sell"):
                trade.flattenOrder(action = "Target")
                current_trades.remove(trade)
            if (current_trade_price < trade.stopPrice and trade.orderSide == "buy"):
                trade.flattenOrder(action = "Target")
                current_trades.remove(trade)
    
    return current_trades
            
            
def get_active_trades(apis):
    current_positions = apis.alpacaApi.list_positions()
    active_trades = []
    
    for pos in current_positions:
        #Currently "buy" is lingo for long, and "sell" is lingo for short.
        #This makes creating and flatteing orders easier.
        #Unfortunately the api does not currently allow shorts but it is ready here when it come available.
        if (pos.side == "long"): 
            side = "buy"
        else: 
            side = "sell"
        
        old_trade = Trade(ticker = pos.symbol,
                          posSize = pos.qty,
                          orderSide = side,
                          timeStamp = "old",
                          strategy = "unknown")
        
        active_trades.append(old_trade)
    
    return active_trades 

def active_trades_to_db(active_trades, serverSite):
    
    active_trade_lists = []
    for trade in active_trades:
        tradeinfo = [trade.ticker, trade.posSize, trade.entryPrice, trade.stopPrice,trade.targetPrice,trade.strategy]
        active_trade_lists.append(tradeinfo)
    
    colnames = ["ticker","PosSize","EntryPrice","StopPrice","TargetPrice","Strategy"]
    active_trade_df = pd.DataFrame(active_trade_lists, columns = colnames)
    write_data_to_sql(active_trade_df,"active_trades",serverSite)

    

def main():
    
    ema_time_period = 20
    
    #Creating the database and putting the data for the last month as a base.
    if (startup):
        print("Startup is true, populating the database wiht stockdata.")
        db_main(server, apis,timeframe = "3m")
    
    #Look for currently active trades, make trade objects and append to active trades.
    active_trades = get_active_trades(apis)

    while True: 
        
        clock = apis.alpacaApi.get_clock()
        now = str(clock.timestamp)[0:19] #Get only current date an time.
        db_main(server, apis, timeframe = "previous")
        
        #Create watchlist and rewrite db before market opens.
        if (clock.is_open == False and "09:15" in now):
            
            latest_data_from_db = read_from_database("SELECT date FROM dailydata ORDER BY date DESC limit 1;",server.serverSite).iloc[0,0]
            latest_data_from_api = get_iex_data(["AAPL"],timeframe = "previous", apikey = apis.iexKey).iloc[0,0] #Testing what the latest data for aapl is, any ticker will do.
            
            #If there is new data, which is true every day except weekends and if the market was closed -> fetch previous days data.
            if (latest_data_from_db != latest_data_from_api):
                #Fetch more data
                print("updating databse with latest data")
                db_main(server, apis, timeframe = "previous")
                print("Database ready")
                
            #Create the watchlist
            print("Building watchlist")
            col_lables = ["ticker","side","price","strategy"]
            print("Ma watchlist ->")
            ma_watchlist = pd.DataFrame(ma_crossing("EMA", ema_time_period),columns = col_lables).sort_values("ticker")
            print("Hd watchlist ->")
            hd_watchlist = pd.DataFrame(find_hammer_doji(),columns = col_lables).sort_values("ticker")
            write_data_to_sql(pd.DataFrame(ma_watchlist),"ma_watchlist", server.serverSite) #Replace is default, meaning yesterdays watchlist gets deleted.
            write_data_to_sql(pd.DataFrame(hd_watchlist),"hd_watchlist", server.serverSite) 
            print("Watchlists ready")
            
            
        if (apis.alpacaApi.get_clock().is_open): #Check if market is open
            print("Market open!")
            time.sleep(300) #Sleep for the first 5 min to avoid the larget market volatility
            
            #Trade!
            while apis.alpacaApi.get_clock().is_open:
                
                #Get the active trade last 15min bars    
                current_active_trade_prices(active_trades)
                
                #Check if the bar has closed below stoploss -> flatten trade
                active_trades = check_stoploss(active_trades, ema_time_period)
                
                #Check if bar high is above target -> flatten trade.
                active_trades = check_target(active_trades)
                
                #The idea behind this is that i can remotely add or remove trades from the database, and they would get updated here too.
                #Read watchlists
                ma_watchlist = read_from_database("SELECT * from ma_watchlist",server.serverSite)
                hd_watchlist = read_from_database("SELECT * from hd_watchlist",server.serverSite)
                
                
                #Sometimes the watchlists are empty, if so creating these lists to avoid having no lists later.
                if (len(hd_watchlist) == 0):
                    found_trades_long_hd = []
                if (len(ma_watchlist) == 0):
                    found_trades_long_ma = []
                
                #Loop trough watchlist and check if the value has been crossed. 
                found_trades_long_ma, found_trades_short_ma = get_watchlist_price(ma_watchlist)
                found_trades_long_hd, found_trades_short_hd = get_watchlist_price(hd_watchlist) #No short strades for the HD strategy should appear
                
                #Fire trades
                succ_trades_long_ma = fire_orders(found_trades_long_ma, "buy", str(now),ema_time_period,"20EMA")
                succ_trades_long_hd = fire_orders(found_trades_long_hd, "buy", str(now),ema_time_period,"H/D")
                succ_trades_short_ma = fire_orders(found_trades_short_ma, "sell", str(now),ema_time_period,"20EMA")
                
                if (len(succ_trades_long_ma + succ_trades_short_ma + succ_trades_long_hd) > 0):
                    for succ_trade in succ_trades_long_ma + succ_trades_short_ma + succ_trades_long_hd:
                        active_trades.append(succ_trade)
                
                traded_stocks = found_trades_long_ma + found_trades_short_ma + found_trades_long_hd
                
                
                #Delete trades from watchlist
                if (len(traded_stocks) > 0):
                    ma_watchlist = ma_watchlist[~ma_watchlist.ticker.str.contains('|'.join(traded_stocks))]
                    #Update the db watchlist
                    write_data_to_sql(pd.DataFrame(ma_watchlist),"ma_watchlist",server.serverSite) 
                    
                if (len(traded_stocks) > 0):
                    hd_watchlist = hd_watchlist[~hd_watchlist.ticker.str.contains('|'.join(traded_stocks))]
                    #Update the db watchlist
                    write_data_to_sql(pd.DataFrame(hd_watchlist),"hd_watchlist",server.serverSite) 
                
                
                #update trades in db
                active_trades_to_db(active_trades, server.serverSite)
                time.sleep(sleepBetweenCalls)
            
        time.sleep(sleepBetweenCalls*3)
        
        #Print out that the system is still running.
        if ("00:00" in now):
            print("System is running", now)
    

if __name__ == "__main__":
    main()
    





    
    
    