###
#Stock scanner
##
import populate_database as db
import argparse
import alpaca_trade_api as tradeapi
import time
import pandas as pd
import datetime
import talib

#At boot the last fetch is set to "old", so when run for the first time it will always fetch new data.
lastFetchTime = datetime(2010,1,1)
lastFetchTimeBase = datetime(2010,1,1)



def volatility_data(iexKey,avkey):
    global lastFetchTime
    global lastFetchTimeBase
    currentTime = datetime.utcnow()
        
    if (currentTime - lastFetchTime).seconds > 300:
        # Turning it from dataframe to series to dict.
        vixdata = pd.read_json("https://cloud.iexapis.com/beta/stock/market/batch?symbols=VIXM&types=previous&token="+iexKey).VIXM[0]
        vixLast = pd.DataFrame.from_dict(vixdata,orient = "index")
        vixLast = vixLast.transpose()
        db.write_data_to_sql(vixLast,"vixLast", serverSite)
        lastFetchTime = datetime.utcnow()
    
    else:
        print("else")
        vixLast = db.read_from_database("SELECT * from vixLast;",serverSite)
    
    # If the last baseline was fetched more than 6h ago, fetch new. Baseline is used to count the daily moving averages to detemine the market sentiment.
    if (currentTime - lastFetchTimeBase).seconds > 21600:
        vixBaseLine = pd.read_csv("https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED"+
                                                    "&symbol=VIXM"
                                                    "&outputsize=full"
                                                    "&datatype=csv"
                                                    "&apikey="+avkey)
        db.write_data_to_sql(vixBaseLine,"vixbaseline",serverSite)
        lastFetchTimeBase = datetime.utcnow()
    else:
        vixBaseLine = db.read_from_database("SELECT * from vixbaseline;",serverSite)
    
    vixBaseLine = vixBaseLine.iloc[::-1]
    vixBaseLine["10MA"] = talib.SMA(vixBaseLine.close, timeperiod = 10)
    vixBaseLine["20MA"] = talib.SMA(vixBaseLine.close, timeperiod = 20)
    vixBaseLine["20EMA"] = talib.EMA(vixBaseLine.close, timeperiod = 20)
    vixBaseLine["50MA"] = talib.SMA(vixBaseLine.close, timeperiod = 50)
    vixBaseLine["50EMA"] = talib.EMA(vixBaseLine.close, timeperiod = 50)
    vixBaseLine["100MA"] = talib.SMA(vixBaseLine.close, timeperiod = 100)
    vixBaseLine["200MA"] = talib.SMA(vixBaseLine.close, timeperiod = 200)
    
    #Compree MA values to the current close.
        
    vixLastClose = vixLast.close[0]
        
    #Count how many of the moving averages are above the vixLastClose
    mas = list(vixBaseLine.iloc[-1,9:])
    bear_counter = 0
        
    for ma in mas:
        if ma < vixLastClose:
            bear_counter +=1
            
    bear_counter = pd.DataFrame([bear_counter], columns = ["bearcounter"])
        
    db.write_data_to_sql(bear_counter,"bearcounter", serverSite)


def highs_lows(serverSite):
    
    #Find stocks that are at 52 week highs/lows or close.
    high_tickers = pd.DataFrame()
    low_tickers = pd.DataFrame()
    
    
    
    query = "SELECT * FROM latestquotes"
    quotes = db.read_from_database(query,serverSite)
    quotes.dropna(subset = ["week52High","week52Low"], inplace = True)
    
    quotes.week52High = quotes.week52High.astype('float')
    quotes.week52Low = quotes.week52Low.astype('float')
    quotes.close = quotes.close.astype('float')
    
    quotes["highdiff"] = quotes.week52High - quotes.close
    quotes["lowdiff"] = quotes.close - quotes.week52Low
    
    for ticker in quotes.iterrows():
        #if the close price is 2% or closer to the 52week highs or lows this loop returns it.
        if ((ticker[1].close * 0.02) > ticker[1].highdiff):
            high_tickers = high_tickers.append(ticker[1])
        if ((ticker[1].close * 0.02) > ticker[1].lowdiff):
            low_tickers = low_tickers.append(ticker[1])
            
    #Dont write data if it is empty!
    #If it it empty write empty df to clear yesterdays data
    empty_df = pd.DataFrame( columns = list(quotes))
    
    if len(high_tickers) > 0 :        
        db.write_data_to_sql(high_tickers, "hightickers",serverSite)
    else:
        db.write_data_to_sql(empty_df,"hightickers",serverSite)
        
    if len(low_tickers) > 0 :
        db.write_data_to_sql(low_tickers,"lowtickers",serverSite)
    else:
        db.write_data_to_sql(empty_df,"lowtickers",serverSite)
            
    

def scannermain(apikey, serverSite, startup, alpacaApi):
    
    if (startup):
            print("Startup selected, running the functions now")
            #Possible to run this at startup or a the specified time before open.
            tickers = db.read_snp_tickers(serverSite)
            #Get latest quotes
            #latest quotes are also used by the front to calculate the gappers
            latest_quotes = db.get_iex_quotes(tickers.Symbol[0:100],apikey)
            db.write_data_to_sql(latest_quotes,"latestquotes",serverSite)
            print("Wrote quotes data to db")
            
            volatility_data(apikey,avkey)
            print("Wrote volatility data data to db")

            
            #Get stocks close to highlows.
            highs_lows(serverSite)
            print("Wrote highlows data to db")
            print("Startup done, exiting")
            time.sleep(2)
            exit()
            
        
    while(True):
         #I have had instaces when the API has been unreachable.
        try:
            clock = alpacaApi.get_clock()
            now = str(clock.timestamp)[0:19] #Get only current date an time.
        except:
            print("Could not fetch clock")    
        
        # Fetching the gappers and stocks close 52week highs and lows at 9:10 and write it to db for the front.
        if (clock.is_open == False and "09:10" in now):
            
            #Get latest quotes
            latest_quotes = db.get_iex_quotes(tickers.Symbol,apikey)
            db.write_data_to_sql(latest_quotes,"latestquotes",serverSite)
            
            #Get stocks close to highlows.
            highs_lows(serverSite)
            print("Wrote highlows data to db")
            
            volatility_data(apikey,avkey)
            print("Wrote volatility data data to db")
            
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
    parser.add_argument("-av","--alphavantagekey", help= "alphavantage key", required = True, type = str)
    
    #Optional
    
    parser.add_argument("-s","--startup",help="fetches quotes at startup", action='store_true')
    args = parser.parse_args()
    iexKey = args.iexKey
    avkey = args.alphavantagekey
    startup = args.startup
    serverSite = str("mysql+pymysql://"+args.serverUser+":"+args.serverPass+args.serverAddress+":3306/"+args.database)
    #serverSite = str("mysql+pymysql://"+serverUser+":"+serverPass+serverAddress+":3306/"+database)
    
    
    alpacaApi = tradeapi.REST(
                       key_id = args.alpacaKey,
                       secret_key = args.alpacaSKey,
                       base_url="https://paper-api.alpaca.markets"
                       )
    
    return iexKey, serverSite, startup, alpacaApi, avkey
    

if __name__ == "__main__": 
   apikey, serverSite, startup, alpacaApi, avkey = parseargs()
   scannermain(apikey, serverSite, startup, alpacaApi, avkey)
