#Echo
#Echo trades index etfs. About 15 of the most traded etfs.
#Echo runs once a day, thus much simpler. Its trategy is to hold strong etfs at large quantities and weak once with lower quantities.

import functions as func
import populate_database as db
import pandas as pd
import strategies as strategies
import time


def run_echo(server, apis_echo, active_trades_echo, now):

    
    #Analyse the etfs 
    trades_df = strategies.rebalance_index_positions(apis_echo, server)

    #Rebalance according to latest analysis, sell first to free up buying power.
    trades_sell = trades_df[trades_df.posdifference < 0]
    if trades_sell.shape[0] > 0:
        func.fire_etf_orders(trades_sell, "sell", now, "index", apis_echo, server, algo = "echo")
    
    #Sleep for a while so that the orders have time to settle before buying more.
    time.sleep(10)

    trades_buy = trades_df[trades_df.posdifference > 0]
    if trades_buy.shape[0] > 0:
        func.fire_etf_orders(trades_buy, "buy", now, "index", apis_echo, server, algo = "echo")

    #Update active trades for echo.
    func.echo_active_trades_to_db(apis_echo, server.serverSite)
