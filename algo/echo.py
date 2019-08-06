#Not currently running..
#Echo
#Echo trades index etfs. About 10 of the most traded etfs.
# Still working on this how it would  determine what etfs to buy/sell..

import functions as func
import populate_database as db
import pandas as pd

def run_echo(server, apis_echo, active_trades_echo, ema_time_period, maxPosSize, maxPosValue, now):
    
   
    #Get the active trade last 15min bars    
    func.current_active_trade_prices(active_trades_echo, apis_echo)
    
    #Check if the bar has closed below stoploss -> flatten trade
    active_trades_echo = func.check_stoploss(active_trades_echo, ema_time_period, server, apis_echo, algo = "echo")
    
    #Check if bar high is above target -> flatten trade.
    active_trades_echo = func.check_target(active_trades_echo, apis_echo, server, algo = "echo")
    
    #Read watchlists
    index_watchlist = db.read_from_database("SELECT * from index_watchlist",server.serverSite)

    
    #Loop trough watchlist and check if the value has been crossed and fire trades.
    if (len(index_watchlist) > 0):
        found_trades_long_index, found_trades_short_index = func.get_watchlist_price(index_watchlist, "index",apis_echo, server)
        succ_trades_long_index = func.fire_orders(found_trades_long_index, "buy", str(now),"index50", apis_echo, server, maxPosSize, maxPosValue, algo = "echo")
        succ_trades_short_index = func.fire_orders(found_trades_short_index, "sell", str(now),"index50", apis_echo, server, maxPosSize, maxPosValue, algo = "echo")
    else:
        #If watchlist is empty, just create empty lists.
        found_trades_long_index = []
        found_trades_short_index = []
        succ_trades_long_index = []
        succ_trades_short_index  = []
        
    #Append succesfull trades to the active trades                
    if (len(succ_trades_long_index + succ_trades_short_index) > 0):
        for succ_trade in succ_trades_long_index + succ_trades_short_index:
            active_trades_echo.append(succ_trade)
        
    traded_stocks = found_trades_long_index + found_trades_short_index 
        
    #Delete trades from watchlist
    if (len(traded_stocks) > 0):
        index_watchlist = index_watchlist[~index_watchlist.ticker.str.contains('|'.join(traded_stocks))]

        #Update the db watchlist
        db.write_data_to_sql(pd.DataFrame(index_watchlist),"index_watchlist",server.serverSite)
        
        
    #update trades in db
    func.active_trades_to_db(active_trades_echo, server.serverSite, table_name= "active_trades_echo")