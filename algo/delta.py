#Delta
#Delta trades 52 week highs and lows.

import functions as func
import populate_database as db
import pandas as pd

def run_delta(server, apis_delta, active_trades_delta,ema_time_period, maxPosSize,maxPosValue, now):
    
   
    #Get the active trade last 15min bars    
    func.current_active_trade_prices(active_trades_delta, apis_delta)
    
    #Check if the bar has closed below stoploss -> flatten trade
    active_trades_delta = func.check_stoploss(active_trades_delta, ema_time_period, server, apis_delta)
    
    #Check if bar high is above target -> flatten trade.
    active_trades_delta = func.check_target(active_trades_delta, apis_delta)
    
    #Read watchlists
    week_watchlist = db.read_from_database("SELECT * from week_watchlist",server.serverSite)

    
    #Loop trough watchlist and check if the value has been crossed and fire trades.
    if (len(week_watchlist) > 0):
        found_trades_long_week, found_trades_short_week = func.get_watchlist_price(week_watchlist, "ma",apis_delta, server)
        succ_trades_long_week = func.fire_orders(found_trades_long_week, "buy", str(now),"Week52", apis_delta, maxPosSize, maxPosValue)
        succ_trades_short_week = func.fire_orders(found_trades_short_week, "sell", str(now),"Week52", apis_delta, maxPosSize, maxPosValue)
    else:
        #If watchlist is empty, just create empty lists.
        found_trades_long_week = []
        found_trades_short_week = []
        succ_trades_long_week = []
        succ_trades_short_week  = []
        
    #Append succesfull trades to the active trades                
    if (len(succ_trades_long_week + succ_trades_short_week) > 0):
        for succ_trade in succ_trades_long_week + succ_trades_short_week:
            active_trades.append(succ_trade)
        
    traded_stocks = found_trades_long_week + found_trades_short_week 
        
    #Delete trades from watchlist
    if (len(traded_stocks) > 0):
        week_watchlist = week_watchlist[~week_watchlist.ticker.str.contains('|'.join(traded_stocks))]

        #Update the db watchlist
        db.write_data_to_sql(pd.DataFrame(week_watchlist),"week_watchlist",server.serverSite)
        
        
    #update trades in db
    func.active_trades_to_db(active_trades_delta, server.serverSite, table_name= "active_trades_delta")