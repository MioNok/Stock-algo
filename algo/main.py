#When the database is populated we can analyse the stocks and send trade orders.

import pandas as pd
import time
import argparse
import logging 

#My scripts.
from populate_database import db_main
import populate_database as db
import strategies as strategies
import functions as func
from models import Server, APIs, Trade

#algos
import charlie
import delta
import echo

#Can edit if needed.
sleepBetweenCalls = 10


def get_active_trades(apis):
    current_positions = apis.alpacaApi.list_positions()
    active_trades = []
    
    for pos in current_positions:
        #Currently "buy" is lingo for long, and "sell" is lingo for short.
        #This makes creating and flatteing orders easier.
        #Unfortunately the api does not currently allow shorts but it is ready here when it come available.
        side = "buy" if pos.side == "long" else "sell"
        
        old_trade = Trade(ticker = pos.symbol,
                          posSize = pos.qty,
                          orderSide = side,
                          timeStamp = "old",
                          strategy = "unknown")

                          
        #TODO move these to the model
        old_trade.entryPrice = pos.avg_entry_price
        old_trade.unrealPL = pos.unrealized_pl
        
        active_trades.append(old_trade)
    
    return active_trades 

    
        
def main(apis, server, startup, startupPrevious, watchlists, maxPosSize, maxPosValue, apis_delta, apis_echo, ema_time_period):

    logging.basicConfig(filename = "logs.log", level=logging.DEBUG)
    
    
    #Creating the database and putting the data for the last month as a base.
    if (startup):
        logging.info("Startup is true, populating the database with stockdata.")
        db_main(server, apis,timeframe = "3m")
        
    #Adding the previous data to the database at startup.
    if (startupPrevious):
        logging.info("StartupPrevious is true, populating the database with previous days stockdata.")
        db_main(server, apis,timeframe = "previous")
    
    #Look for currently active charlie trades, make trade objects and append to active trades.
    active_trades = get_active_trades(apis)
    logging.info("Start complete Charlie")
    
    #Look for currently active delta trades, make trade objects and append to active trades.
    active_trades_delta = get_active_trades(apis_delta)
    logging.info("Start complete Delta")

    while True: 
        
        #I have had instaces when the API has been unreachable.
        try:
            clock = apis.alpacaApi.get_clock()
            now = str(clock.timestamp)[0:19] #Get only current date and time. Only returns YYYY-MM-DD-HH:MM. The  and seconds are parsed
        except:
            logging.error("Could not fetch clock")
        
        #Create charlie watchlist and rewrite db before market opens.
        if ((not clock.is_open and "09:05" in now) or watchlists):
            
            latest_data_from_db = db.read_from_database("SELECT date FROM dailydata ORDER BY date DESC limit 1;", server.serverSite).iloc[0,0]
            latest_data_from_api = db.get_iex_data(["AAPL"],timeframe = "previous", apikey = apis.iexKey).iloc[0,0] #Testing what the latest data for aapl is, any ticker will do.
            #If there is new data, which is true every day except weekends and if the market was closed -> fetch previous days data.
            if latest_data_from_db != latest_data_from_api:
                #Fetch more data
                logging.info("updating databse with latest data")
                db_main(server, apis, timeframe = "previous")
                logging.info("Database ready")
                
            #Create the watchlist
            logging.info("Building watchlist")
            col_lables = ["ticker","side","price","strategy"]
            logging.info("Ma watchlist ->")
            ma_watchlist = pd.DataFrame(strategies.ma_crossover("EMA", ema_time_period, server),columns = col_lables).sort_values("ticker")
            logging.info("Hd watchlist ->")
            hd_watchlist = pd.DataFrame(strategies.hammer_doji(server),columns = col_lables).sort_values("ticker")
            logging.info("BB watchlist ->")
            bb_watchlist = pd.DataFrame(strategies.bb_cross(server),columns = col_lables).sort_values("ticker")
            
            db.write_data_to_sql(pd.DataFrame(ma_watchlist),"ma_watchlist", server.serverSite) #Replace is default, meaning yesterdays watchlist gets deleted.
            db.write_data_to_sql(pd.DataFrame(hd_watchlist),"hd_watchlist", server.serverSite) 
            db.write_data_to_sql(pd.DataFrame(bb_watchlist),"bb_watchlist", server.serverSite)
            
            logging.info("Week52 watchlist ->")
            week_watchlist = pd.DataFrame(strategies.week_cross(server, apis_delta, active_trades_delta),columns = col_lables).sort_values("ticker")
            db.write_data_to_sql(pd.DataFrame(week_watchlist),"week_watchlist", server.serverSite)
            logging.info("Watchlists ready")
            
            try:
                clock = apis.alpacaApi.get_clock()
                now = str(clock.timestamp)[0:19] #Get only current date an time.
            except:
                logging.error("Could not fetch clock")
            
            
        if (clock.is_open): #Check if market is open
            logging.info("Market open!")
            time.sleep(900) #Sleep for the first 15 min to avoid the larget market volatility
            
            #Fetch portfolio values to db
            func.portfolio_value_to_db(apis, server.serverSite, "Charlie")
            func.portfolio_value_to_db(apis_delta, server.serverSite, "Delta")
            func.portfolio_value_to_db(apis_echo, server.serverSite,"Echo")

            #Echo should be only run once a day so running if at the start of the day.
            echo.run_echo(server,apis_echo,now)

            while apis.alpacaApi.get_clock().is_open:
                try:
                    clock = apis.alpacaApi.get_clock()
                    now = str(clock.timestamp)[0:19] #Get only current date an time.
                except:
                    logging.error("Could not fetch clock")
            
                #Running charlie
                charlie.run_charlie(server, apis, active_trades, ema_time_period, maxPosSize, maxPosValue, now)
                #Running delta
                delta.run_delta(server, apis_delta, active_trades_delta,ema_time_period, maxPosSize,maxPosValue, now)
                #Add more algos here
                
                time.sleep(sleepBetweenCalls)
            
 
            
        time.sleep(sleepBetweenCalls*3)
        
        #Print out that the system is still running.
        if ("00" in now):
            logging.info("System is running", now)
            
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
    parser.add_argument("-akd","--alpacaKeydelta", help= "alpaca api key delta", required = True, type = str)
    parser.add_argument("-askd","--alpacaSKeydelta", help= "alpaca secret api key delta", required = True, type = str)
    parser.add_argument("-ake","--alpacaKeyecho", help= "alpaca api key echo", required = True, type = str)
    parser.add_argument("-aske","--alpacaSKeyecho", help= "alpaca secret api key echo", required = True, type = str)
    parser.add_argument("-ik","--iexKey", help= "Iex api key", required = True, type = str)
    
    
    #Optional 
    parser.add_argument("-s","--startup",help="fetches 3m data if present", action='store_true')
    parser.add_argument("-sprev","--startupPrevious",help="fetches previous data", action='store_true')
    parser.add_argument("-wl","--watchlists",help="run watchlists at boot", action='store_true')
    parser.add_argument("-pv","--posSize", help= "Max position size of a stock, default is  500", nargs = "?", default = 500, type = int)
    parser.add_argument("-ps","--posValue", help= "Max value of a position of a stock, default is 5000", nargs = "?", default = 5000, type = int)
    parser.add_argument("-ema","--exponentialmovingaverage", help= "ema for charlie/delta, default is 20", nargs = "?", default = 20, type = int)
    
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
    alpacaKeyecho = args.alpacaKeyecho
    alpacaSKeyecho = args.alpacaSKeyecho
    iexKey = args.iexKey
    ema_time_period = args.exponentialmovingaverage
    
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
    apis_echo = APIs(alpacaKeyecho, alpacaSKeyecho, iexKey)   

    
    return apis, server, startup, startupPrevious, watchlists, maxPosSize, maxPosValue, apis_delta, apis_echo, ema_time_period
    

if __name__ == "__main__":
    apis, server, startup, startupPrevious, watchlists, maxPosSize, maxPosValue, apis_delta, apis_echo, ema_time_period = parseargs()
    main(apis, server, startup, startupPrevious, watchlists, maxPosSize, maxPosValue, apis_delta, apis_echo, ema_time_period)
    





    
    
    
