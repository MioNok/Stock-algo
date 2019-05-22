###
#Stock scanner
##
import populate_database as db
import argparse
import alpaca_trade_api as tradeapi
import time
import pandas as pd


def highs_lows(serverSite):
    
    #Find stocks that are at 52 week highs/lows or close.
    high_tickers = pd.DataFrame()
    low_tickers = pd.DataFrame()
    
    query = "SELECT * FROM latestquotes"
    quotes = db.read_from_database(query,serverSite)
    
    quotes["highdiff"] = quotes.week52High - quotes.close
    quotes["lowdiff"] = quotes.close - quotes.week52Low
    
    for ticker in quotes.iterrows():
        #if the close price is 2% or closer to the 52week highs or lows this loop returns it.
        if ((ticker[1].close * 0.02) > ticker[1].highdiff):
            high_tickers = high_tickers.append(ticker[1])
        if ((ticker[1].close * 0.02) > ticker[1].lowdiff):
            low_tickers = low_tickers.append(ticker[1])
            
    db.write_data_to_sql(high_tickers, "hightickers",serverSite)
    db.write_data_to_sql(low_tickers,"lowtickers",serverSite)
            
    

def scannermain(apikey, serverSite, startup, alpacaApi):
    
    if (startup):
            print("Startup selected, running the functions now")
            #Possible to run this at startup or a the specified time before open.
            tickers = db.read_snp_tickers(serverSite)
            #Get latest quotes
            latest_quotes = db.get_iex_quotes(tickers.Symbol,apikey)
            db.write_data_to_sql(latest_quotes,"latestquotes",serverSite)
            print("Wrote quotes data to db")
            
            #Get stocks close to highlows.
            highs_lows(serverSite)
            print("Wrote highlows data to db")
            print("Startup done, exiting")
            time.sleep(3)
            exit()
            
        
    while(True):
         #I have had instaces when the API has been unreachable.
        try:
            clock = alpacaApi.get_clock()
            now = str(clock.timestamp)[0:19] #Get only current date an time.
        except:
            print("Could not fetch clock")
        
        print("Startup complete")    
        
        # Fetching the gappers and stocks close 52week highs and lows at 9:10 and write it to db for the front.
        if (clock.is_open == False and "09:10" in now):
            
            #Get latest quotes
            latest_quotes = db.get_iex_quotes(tickers.Symbol,apikey)
            db.write_data_to_sql(latest_quotes,"latestquotes",serverSite)
            #Get stocks close to highlows.
            highs_lows(serverSite)
            
        time.sleep(30)
            

def parseargs():
    parser = argparse.ArgumentParser()
    
    parser.add_argument("-su","--serverUser", help= "server username", required = True, type = str)
    parser.add_argument("-sp","--serverPass", help= "server password", required = True, type = str)
    parser.add_argument("-sa","--serverAddress", help= "server addres",required = True, type = str)
    parser.add_argument("-db","--database", help= "the name of your database",required = True, type = str)
    parser.add_argument("-ak","--alpacaKey", help= "alpaca api key", required = True, type = str)
    parser.add_argument("-ask","--alpacaSKey", help= "alpaca secret api key", required = True, type = str)
    parser.add_argument("-ik","--iexKey", help= "Iex api key", required = True, type = str)
    
    #Optional
    
    parser.add_argument("-s","--startup",help="fetches quotes at startup", action='store_true')
    args = parser.parse_args()
    iexKey = args.iexKey
    startup = args.startup
    serverSite = str("mysql+pymysql://"+args.serverUser+":"+args.serverPass+args.serverAddress+":3306/"+args.database)
    #serverSite = str("mysql+pymysql://"+serverUser+":"+serverPass+serverAddress+":3306/"+database)
    
    
    alpacaApi = tradeapi.REST(
                       key_id = args.alpacaKey,
                       secret_key = args.alpacaSkey,
                       base_url="https://paper-api.alpaca.markets"
                       )
    
    return iexKey, serverSite, startup, alpacaApi
    

if __name__ == "__main__": 
   scannermain(parseargs())