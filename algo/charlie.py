import functions as func
import populate_database as db
import pandas as pd

def run_charlie(server, apis, active_trades, ema_time_period, maxPosSize, maxPosValue, now):
    
   
    #Get the active trade last 15min bars    
    func.current_active_trade_prices(active_trades,apis)
    
    #Check if the bar has closed below stoploss -> flatten trade
    active_trades = func.check_stoploss(active_trades, ema_time_period, server, apis)
    
    #Check if bar high is above target -> flatten trade.
    active_trades = func.check_target(active_trades, apis, server)
    
    #The idea behind this is that i can remotely add or remove trades from the database, and they would get updated here too.
    #Read watchlists
    ma_watchlist = db.read_from_database("SELECT * from ma_watchlist",server.serverSite)
    hd_watchlist = db.read_from_database("SELECT * from hd_watchlist",server.serverSite)
    bb_watchlist = db.read_from_database("SELECT * from bb_watchlist",server.serverSite)
    
    #Loop trough watchlist and check if the value has been crossed and fire trades.
    if (len(ma_watchlist) > 0):
        found_trades_long_ma, found_trades_short_ma = func.get_watchlist_price(ma_watchlist, "ma",apis, server)
        succ_trades_long_ma = func.fire_orders(found_trades_long_ma, "buy", str(now),"20EMA", apis, server, maxPosSize, maxPosValue)
        succ_trades_short_ma = func.fire_orders(found_trades_short_ma, "sell", str(now),"20EMA", apis, server, maxPosSize, maxPosValue)
    else:
        #If watchlist is empty, just create empty lists.
        found_trades_long_ma = []
        found_trades_short_ma = []
        succ_trades_long_ma = []
        succ_trades_short_ma = []
        
    if (len(hd_watchlist) >0):
        found_trades_long_hd, found_trades_short_hd = func.get_watchlist_price(hd_watchlist,"hd", apis, server) #No short strades for the HD strategy should appear
        succ_trades_long_hd = func.fire_orders(found_trades_long_hd, "buy", str(now),"H/D",apis, server, maxPosSize, maxPosValue)   
    else:
        #If watchlist is empty, just create empty lists.
        found_trades_long_hd = []
        succ_trades_long_hd = []
        
    #Loop trough watchlist and check if the value has been crossed and fire trades.
    if (len(bb_watchlist) > 0):
        found_trades_long_bb, found_trades_short_bb = func.get_watchlist_price(bb_watchlist, "bb", apis, server)
        succ_trades_long_bb = func.fire_orders(found_trades_long_bb, "buy", str(now),"BB",apis, server, maxPosSize, maxPosValue)
        succ_trades_short_bb = func.fire_orders(found_trades_short_bb, "sell", str(now),"BB",apis, server, maxPosSize, maxPosValue)
    else:
        #If watchlist is empty, just create empty lists.
        found_trades_long_bb = []
        found_trades_short_bb = []
        succ_trades_long_bb = []
        succ_trades_short_bb = []
    #Append succesfull trades to the active trades                
    if (len(succ_trades_long_ma + succ_trades_short_ma + succ_trades_long_hd + succ_trades_long_bb + succ_trades_short_bb) > 0):
        for succ_trade in succ_trades_long_ma + succ_trades_short_ma + succ_trades_long_hd+ succ_trades_long_bb + succ_trades_short_bb:
            active_trades.append(succ_trade)
        
    traded_stocks = found_trades_long_ma + found_trades_short_ma + found_trades_long_hd+ found_trades_short_bb + found_trades_long_bb
        
        
    #Delete trades from watchlist
    if (len(traded_stocks) > 0):
        ma_watchlist = ma_watchlist[~ma_watchlist.ticker.str.contains('|'.join(traded_stocks))]
        hd_watchlist = hd_watchlist[~hd_watchlist.ticker.str.contains('|'.join(traded_stocks))]
        bb_watchlist = bb_watchlist[~bb_watchlist.ticker.str.contains('|'.join(traded_stocks))]
        #Update the db watchlist
        db.write_data_to_sql(pd.DataFrame(ma_watchlist),"ma_watchlist",server.serverSite)
        db.write_data_to_sql(pd.DataFrame(hd_watchlist),"hd_watchlist",server.serverSite)
        db.write_data_to_sql(pd.DataFrame(bb_watchlist),"bb_watchlist",server.serverSite) 
        
        
    #update trades in db
    func.active_trades_to_db(active_trades, server.serverSite)