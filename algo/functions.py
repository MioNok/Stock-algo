#Functions for the algos
from datetime import date
import populate_database as db
import time
from models import Trade
import pandas as pd
import talib




    
#Currently broken.. reverting to old version. This is not used for now.
def get_watchlist_price_broken(watchlist_df, wl_code, apis, server):
    #Working around the limitations of the API, one call can only contain 100 tickers
    sumtickers = watchlist_df.shape[0]
    index = 100

    #These lists will be populated and returned
    found_trades_long = []
    found_trades_short = []

    while True:
        watchlist_df_sliced = watchlist_df.iloc[index-100:index,:]
        watchlist_bars = apis.alpacaApi.get_barset(watchlist_df.ticker[index-100:index],'minute',limit = 1).df
    
        #The API that returns real time data is not perfect in my opinion. 
        #It returns the last minute candle that the given tickes has traded, not neccecarliy the latest
        #This has led me to do this wierd contraption where i fill the df's all NANs with the mean, and then I can just transpose it and take the first column
        #Since all the rows contain the same values after the fill.na with mean. 
         #You can set the start and end dates on the API call but I'm not sure if it supports minutes, since that is what I would be interested in.
        #This should not have been a problem but it it was

        watchlist_bars = watchlist_bars.fillna(watchlist_bars.mean())
    
        #We only want the "close" values, this is the first way I could come up with. Surely there are better ways.
        close_columns = [col for col in watchlist_bars.columns if "close" in col]
     
        close_values = watchlist_bars[close_columns].transpose().iloc[:,0]
        close_values.sort_index()
    
        watchlist_df_sliced.insert(4,"current_price",list(close_values), True)
        price_difference = watchlist_df_sliced["price"]- watchlist_df_sliced["current_price"]
        watchlist_df_sliced.insert(5,"price_difference",list(price_difference), True)

        #Update the db prices
        #If the tickers are more than 100, we are going to append items to the watchlist db and not replace. 
        fate = "replace"

        if sumtickers > index:
            fate = "append"

        db.write_data_to_sql(watchlist_df_sliced,wl_code+"_watchlist", server.serverSite, fate )
    
        longs = watchlist_df_sliced[watchlist_df_sliced["side"].str.match("buy")]
        for element, stock in longs.iterrows():
            if (stock["price_difference"] < 0 ):
                found_trades_long.append(stock["ticker"])
                print("Trade , Long", stock["ticker"], stock["side"], stock["price"], stock["current_price"] )

        
        shorts = watchlist_df_sliced[watchlist_df_sliced["side"].str.match("sell")]
        for element,stock in shorts.iterrows():
            if (stock["price_difference"] > 0):
                found_trades_short.append(stock["ticker"])


        #If the number of symbols were less than 100, we break here. If not we will loop again and check again.
        if index > sumtickers:
            break

        index = index + 100

    return found_trades_long, found_trades_short

def get_watchlist_price(watchlist_df, wl_code, apis, server):
     
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
    close_values.sort_index()
    
    watchlist_df["current_price"] = list(close_values)
    watchlist_df["price_difference"] = watchlist_df["price"]- watchlist_df["current_price"]
    
    #Update the db prices 
    db.write_data_to_sql(pd.DataFrame(watchlist_df),wl_code+"_watchlist", server.serverSite)
    
    found_trades_long = []
    found_trades_short = []
    
    longs = watchlist_df[watchlist_df["side"].str.match("buy")]
    shorts = watchlist_df[watchlist_df["side"].str.match("sell")]
    
    for index, stock in longs.iterrows():
        if (stock["price_difference"] < 0 ):
            found_trades_long.append(stock["ticker"])
            print("Trade , Long", stock["ticker"], stock["side"], stock["price"], stock["current_price"] )
            

    for index, stock in shorts.iterrows():
        if (stock["price_difference"] > 0):
            found_trades_short.append(stock["ticker"])
            print("Trade , Short", stock["ticker"], stock["side"], stock["price"], stock["current_price"] )

    return found_trades_long, found_trades_short


#Define number of shares here
def fire_orders(trades, side, now, strategy, apis, server, maxPosSize, maxPosValue, algo = "charlie"):
    
    current_bp = int(float(apis.alpacaApi.get_account().buying_power))

    succesful_trades = []
    for trade in trades:
        try:
            
            postValue = maxPosValue
            posSize = maxPosSize
    
            #Setting max pos size. Either trade value is 5000 or 500 shares. Which ever is bigger.
            current_price = apis.alpacaApi.get_barset(trade,"1Min",limit = 1).df.iloc[0,3]
            if (current_price * posSize >postValue):
                posSize = int(maxPosValue/current_price)
                
            #Check buying power, if not enough brake loop.    
            if(current_bp < current_price + posSize):
                print("No buying power")
                break
            
            live_trade = Trade(trade, posSize, side, now, strategy)
            
            
            live_trade.submitOrder(apis,server, algo = algo)
            succesful_trades.append(live_trade)
        except:
            print("Trade failed for ",trade)
    return succesful_trades

#Only used for echo. better version of the one above.
def fire_etf_orders(trades_df, side, now, strategy, apis, server, algo = "echo"):
    
    for trades in trades_df.iterrows():
        try:
            live_trade = Trade(trades[1].symbol, trades[1].posdifference, side, now, strategy)
            
            live_trade.submitOrder(apis,server, algo = algo)
        except:
            print("Trade failed for", trades[1].symbol)
            



def current_active_trade_prices(current_trades, apis):   
    #Get the latest 15min candle. Future trade decisions is made on the OHLC on it.
    for trade in current_trades:
        current_candle = apis.alpacaApi.get_barset(trade.ticker,"15Min",limit = 1).df
        trade.setLastCandle(current_candle)
        trade.setPosition(apis)
        
        

def check_stoploss(current_trades,ema_time_period, server,apis, algo = "charlie"):
    #Note to self. Search all data at once, not every stock for themself.
    #Find stop prices for the trades.
    for trade in current_trades:
        if (trade.stopPrice == 0) :
            data = db.read_from_database("Select distinct date, ticker, uHigh, uLow, uClose from dailydata where ticker ='"+ trade.ticker+ "' ORDER BY date DESC limit "+str(ema_time_period+10)+";",server.serverSite)
            
            #Talib need the oldest data to be first     
            data = data.iloc[::-1]
        
            #Setting the stop price to the 20EMA
            data["stop_price"] = talib.EMA(data.uClose, timeperiod = ema_time_period)
            trade.setStopPrice(data.stop_price[0])
            print("Stop price for ", trade.ticker," is set to ", trade.stopPrice, "entry:", trade.entryPrice )
        else:
            #Get the close price of the last 5 minute candle and comapre it against the stop price
            #If the 5min candle has closed above the stop price, it will flatten the trade.
            #current_trade_price = apis.alpacaApi.get_barset(trade.ticker,"5Min",limit = 1).df.iloc[0,3]
            current_trade_price = trade.last15MinCandle.iloc[0,3]
            if (current_trade_price > trade.stopPrice  and trade.orderSide == "sell"):
                trade.flattenOrder(action = "Stoploss", apis = apis, server = server, algo = algo)
                current_trades.remove(trade)
            if (current_trade_price < trade.stopPrice and trade.orderSide == "buy"):
                trade.flattenOrder(action = "Stoploss", apis = apis, server = server, algo= algo)
                current_trades.remove(trade)
                
                
    return current_trades

def check_target(current_trades,apis, server, algo = "charlie"):
    #Set target price, and check if target price, current target is 2:1
    for trade in current_trades:
        if (trade.targetPrice  == 0):
            
            #update the current position info, sleep for a while so that the orders have time get filled.
            time.sleep(3)
            trade.setPosition(apis)
            
            if(trade.orderSide == "buy"):
                trade.targetPrice = trade.entryPrice + ((trade.entryPrice - trade.stopPrice)*2) 
            else:
                trade.targetPrice = trade.entryPrice - ((trade.stopPrice - trade.entryPrice)*2)
            
            print("Target price for ", trade.ticker," is set to ", trade.targetPrice,  "entry:", trade.entryPrice, "stop:", trade.stopPrice, "order side", trade.orderSide)
        else:
            #Close the trade if the 1min candle high has hit the target
            current_trade_price = trade.last15MinCandle.iloc[0,1]
            current_trade_price_low = trade.last15MinCandle.iloc[0,2]
            if (current_trade_price_low < trade.targetPrice and trade.orderSide == "sell"):
                trade.flattenOrder(action = "Target",apis = apis,server = server, algo = algo)
                current_trades.remove(trade)
                print("Side sell", "Current trade price (15 min candle high?",current_trade_price, "and low", current_trade_price_low)
            if (current_trade_price > trade.targetPrice and trade.orderSide == "buy"):
                trade.flattenOrder(action = "Target", apis = apis, server = server, algo = algo)
                current_trades.remove(trade)
                print("Side Buy","Current trade price (15 min candle high?",current_trade_price, "and low", current_trade_price_low)
    
    return current_trades



def active_trades_to_db(active_trades, serverSite, table_name ="active_trades"):
    
    active_trade_lists = []
    for trade in active_trades:
        tradeinfo = [trade.ticker, trade.posSize, trade.entryPrice, trade.stopPrice,trade.targetPrice,trade.strategy,trade.unrealPL]
        active_trade_lists.append(tradeinfo)
    
    colnames = ["ticker","PosSize","EntryPrice","StopPrice","TargetPrice","Strategy","UnrealPl"]
    active_trade_df = pd.DataFrame(active_trade_lists, columns = colnames)
    db.write_data_to_sql(active_trade_df,table_name, serverSite)



def portfolio_value_to_db(apis,serverSite,code):
    #Fetch the current portfolio values and store in db. Code is used to differentiate the apis
    today = date.today()
    time = today.strftime("%d/%m/%Y")
    portvalue = apis.alpacaApi.get_account().portfolio_value
    df = pd.DataFrame({"code": [code], "portval":[portvalue], "timestamp":[time]})
    db.write_data_to_sql(df,"portvalues",serverSite, if_exists = "append")
    
def echo_active_trades_to_db(apis,serverSite): 
    
    current_portfolio = apis.alpacaApi.list_positions()
    
    current_port_list = []
    for position in current_portfolio:
        current_port_list.append([position.symbol, 
                                  position.current_price, 
                                  position.lastday_price,
                                  position.qty,
                                  position.unrealized_plpc])
        
    colnames = ["ticker","current_price","lastday_price","qty","unreal_pl"]
    current_port_df = pd.DataFrame(current_port_list, columns= colnames)
    
    write_data_to_sql(current_port_df, "active_trades_echo", serverSite, if_exists = "replace")
            
            

    
