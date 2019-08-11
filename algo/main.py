#When the database is populated we can analyse the stocks and send trade orders.

import pandas as pd
import time
import argparse

#My scripts.
from populate_database import db_main
import populate_database as db
import strategies as strategies
import functions as func
from models import Server, APIs, Trade

#algos
import charlie
import delta
#import echo

#Can edit if needed.
sleepBetweenCalls = 10


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

    
        
def main(apis, server, startup, startupPrevious, watchlists, maxPosSize, maxPosValue, apis_delta):
    
    ema_time_period = 20
    
    #Creating the database and putting the data for the last month as a base.
    if (startup):
        print("Startup is true, populating the database with stockdata.")
        db_main(server, apis,timeframe = "3m")
        
    #Adding the previous data to the database at startup.
    if (startupPrevious):
        print("StartupPrevious is true, populating the database with previous days stockdata.")
        db_main(server, apis,timeframe = "previous")
    
    #Look for currently active charlie trades, make trade objects and append to active trades.
    active_trades = get_active_trades(apis)
    print("Start complete Charlie")
    
    #Look for currently active delta trades, make trade objects and append to active trades.
    active_trades_delta = get_active_trades(apis_delta)
    print("Start complete Delta")

    while True: 
        
        #I have had instaces when the API has been unreachable.
        try:
            clock = apis.alpacaApi.get_clock()
            now = str(clock.timestamp)[0:19] #Get only current date and time.
        except:
            print("Could not fetch clock")
        
        #Create charlie watchlist and rewrite db before market opens.
        if ((clock.is_open == False and "09:05" in now) or watchlists):
            
            latest_data_from_db = db.read_from_database("SELECT date FROM dailydata ORDER BY date DESC limit 1;", server.serverSite).iloc[0,0]
            latest_data_from_api = db.get_iex_data(["AAPL"],timeframe = "previous", apikey = apis.iexKey).iloc[0,0] #Testing what the latest data for aapl is, any ticker will do.
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
            ma_watchlist = pd.DataFrame(strategies.ma_crossover("EMA", ema_time_period, server),columns = col_lables).sort_values("ticker")
            print("Hd watchlist ->")
            hd_watchlist = pd.DataFrame(strategies.hammer_doji(server),columns = col_lables).sort_values("ticker")
            print("BB watchlist ->")
            bb_watchlist = pd.DataFrame(strategies.bb_cross(server),columns = col_lables).sort_values("ticker")
            
            db.write_data_to_sql(pd.DataFrame(ma_watchlist),"ma_watchlist", server.serverSite) #Replace is default, meaning yesterdays watchlist gets deleted.
            db.write_data_to_sql(pd.DataFrame(hd_watchlist),"hd_watchlist", server.serverSite) 
            db.write_data_to_sql(pd.DataFrame(bb_watchlist),"bb_watchlist", server.serverSite)
            
            print("Week52 watchlist ->")
            week_watchlist = pd.DataFrame(strategies.week_cross(server, apis_delta, active_trades_delta),columns = col_lables).sort_values("ticker")
            db.write_data_to_sql(pd.DataFrame(week_watchlist),"week_watchlist", server.serverSite)
            print("Watchlists ready")
            
            try:
                clock = apis.alpacaApi.get_clock()
                now = str(clock.timestamp)[0:19] #Get only current date an time.
            except:
                print("Could not fetch clock")
            
            
        if (clock.is_open): #Check if market is open
            print("Market open!")
            time.sleep(900) #Sleep for the first 15 min to avoid the larget market volatility
            
            #Fetch portfolio values to db
            func.portfolio_value_to_db(apis, serverSite, "Charlie")
            func.portfolio_value_to_db(apis_delta, serverSite, "Delta")
            #func.portfolio_value_to_db(apis_echo,"Echo")

            while apis.alpacaApi.get_clock().is_open:
                try:
                    clock = apis.alpacaApi.get_clock()
                    now = str(clock.timestamp)[0:19] #Get only current date an time.
                except:
                    print("Could not fetch clock")
            
                #Running charlie
                charlie.run_charlie(server, apis, active_trades, ema_time_period, maxPosSize, maxPosValue, now)
                #Running delta
                delta.run_delta(server, apis_delta, active_trades_delta,ema_time_period, maxPosSize,maxPosValue, now)
                #Add more algos here
                
                time.sleep(sleepBetweenCalls)
            
 
            
        time.sleep(sleepBetweenCalls*3)
        
        #Print out that the system is still running.
        if ("00" in now):
            print("System is running", now)
            
def parseargs():
    parser = argparse.ArgumentParser()


    #Arguments
    #Must haves
    parser = argparse.ArgumentParser()
    parser.add_argument("-su","--serverUser", help= "server username", required = True, type = str)
    parser.add_argument("-sp","--serverPass", help= "server password", required = True, type = str)
    parser.add_argument("-sa","--serverAddress", help= "server addres",required = True, type = str)
    parser.add_argument("-db","--database", help= "the name of your database",required = True, type = str)
    parser.add_argument("-ak","--alpacaKey", help= "alpaca api key", required = True, type = str)
    parser.add_argument("-ask","--alpacaSKey", help= "alpaca secret api key", required = True, type = str)
    parser.add_argument("-akd","--alpacaKeydelta", help= "alpaca api key", required = True, type = str)
    parser.add_argument("-askd","--alpacaSKeydelta", help= "alpaca secret api key", required = True, type = str)
    parser.add_argument("-ik","--iexKey", help= "Iex api key", required = True, type = str)
    
    #Optional 
    parser.add_argument("-s","--startup",help="fetches 3m data if present", action='store_true')
    parser.add_argument("-sprev","--startupPrevious",help="fetches previous data", action='store_true')
    parser.add_argument("-wl","--watchlists",help="run watchlists at boot", action='store_true')
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
    alpacaKeydelta = args.alpacaKeydelta
    alpacaSKeydelta = args.alpacaSKeydelta
    iexKey = args.iexKey
    
    #Position variables
    startup = args.startup
    startupPrevious = args.startupPrevious    
    watchlists = args.watchlists
    maxPosSize = args.posSize
    maxPosValue = args.posValue
    
    #important classes
    server = Server(user = serverUser, password = serverPass, address = serverAddress, database = database)
    apis = APIs(alpacaKey, alpacaSKey, iexKey)
    apis_delta = APIs(alpacaKeydelta, alpacaSKeydelta, iexKey)   

    
    return apis, server, startup, startupPrevious, watchlists, maxPosSize, maxPosValue, apis_delta
    

if __name__ == "__main__":
    apis, server, startup, startupPrevious, watchlists, maxPosSize, maxPosValue, apis_delta = parseargs()
    main(apis, server, startup, startupPrevious, watchlists, maxPosSize, maxPosValue, apis_delta)
    





    
    
    
