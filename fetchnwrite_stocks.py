
import pandas as pd
import sqlalchemy
import time


#This works when assuming you have a root user and the host is localhost.

#Edit these
apikey = ["InsetPassword"] # Insert apikey.
serverpass = "default" #isert your mysql server 
database = "stockdata" #database in your mysql you want to use.
sleeptime = 10

key_counter = 0

def getSnP500data():
    snp500data = pd.read_csv("https://datahub.io/core/s-and-p-500-companies-financials/r/constituents-financials.csv")
    return snp500data.drop(["SEC Filings"], axis =1)
    

#Legacy, but if needed the dow tickers are still here.
dowTickers = ["AXP","AAPL","BA","CAT","CSCO","CVX","DIS","DOW","GS",
              "HD","IBM","INTC","JNJ","JPM","KO","MCD","MMM", "MRK",
              "MSFT","NKE","PFE","PG","TRV","UNH","UTX","V","VZ","WBA",
              "WMT","XOM"]

#Uses the dow tickers if nothing else is specified.
def read_data_daily_and_ti(tickers = dowTickers):
    rawStockDataDaily = pd.DataFrame()

    for ticker in tickers:
        try:
            tempRawStockDataDaily = pd.read_csv("https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED"+
                                                "&symbol="+ ticker +
                                                "&outputsize=full"+
                                                "&datatype=csv"+
                                                "&apikey="+ apikey[key_counter])
        
            #Write the ticker name to the df to keep track of what data belong where.
            tempRawStockDataDaily["ticker"] = ticker
        
            print("Fetched " + ticker + " daily data" )
            #SMA200 needs 200 values to count the SMA, so the first 200 would ne null for us int the dataframe.
            #Avoiding this for now by just deleting the first 200 values. Will do this for every timeframe.
            #tempRawStockDataDaily = tempRawStockDataDaily.iloc[0:tempRawStockDataDaily.shape[0]-199]
        
            
            
            #Adding the SMA data to the dataframe.
            tempRawStockDataDaily["sma200"] = getMA(ticker, 200, "SMA")
            tempRawStockDataDaily["sma100"] = getMA(ticker, 100, "SMA")
            tempRawStockDataDaily["sma50"] = getMA(ticker, 50, "SMA")
            
            tempRawStockDataDaily["ema200"] = getMA(ticker, 200, "EMA")
            tempRawStockDataDaily["ema100"] = getMA(ticker, 100, "EMA")
            tempRawStockDataDaily["ema50"] = getMA(ticker, 50, "EMA")
            tempRawStockDataDaily["ema20"] = getMA(ticker, 20, "EMA")
            
            tempRawStockDataDaily["rsi14"] = getRSI(ticker, 14)
            
            
        except: print("The fetch failed. Most probably to many calls /minute. Consider starting over with a longer sleep time.")
        keyCounter()
        #Saving the reults for this ticker and moving on to the following ticker.
        rawStockDataDaily = rawStockDataDaily.append(tempRawStockDataDaily)
        time.sleep(sleeptime) #Free API key only gets you so far, as of writing this alphavantage is limiting the amount of API calls you can make in a minute..
      
    return rawStockDataDaily

def getMA(ticker, time_period, ma):
    
    rawStockDataSMA = pd.read_csv("https://www.alphavantage.co/query?function="+ ma +
                                  "&symbol="+ ticker +
                                  "&interval=daily"+
                                  "&series_type=close"+
                                  "&time_period="+str(time_period)+
                                  "&datatype=csv"+
                                  "&apikey="+ apikey[key_counter])
    print("Fetched " + ticker + " "+ma+ str(time_period) +" data")
    time.sleep(sleeptime)
    keyCounter()
    return rawStockDataSMA[ma]

def getRSI(ticker, time_period):
    
    rawStockDataRSI = pd.read_csv("https://www.alphavantage.co/query?function=RSI"+
                                  "&symbol="+ ticker +
                                  "&interval=daily"+
                                  "&series_type=close"+
                                  "&time_period="+str(time_period)+
                                  "&datatype=csv"+
                                  "&apikey="+ apikey[key_counter])
    print("Fetched " + ticker + " RSI"+ str(time_period) +" data")
    time.sleep(sleeptime)
    keyCounter()
    return rawStockDataRSI["RSI"]
    

#Depending on if you are fetcing the data all at once or not appending or replaing the data might be the right option.
def write_data_to_sql(dailyStockData, table_name, if_exists = "append"):
    #you need mysql alchemy and pymysql to run this. syntax is:  Username:password@host:port/database
    engine = sqlalchemy.create_engine("mysql+pymysql://root:"+serverpass+"@localhost:3306/stockdata")
    #Writing the data, name is table name. 
    dailyStockData.to_sql(name = table_name, con = engine,index = False,  if_exists = if_exists)

def read_snp_tickers():
    query = """SELECT Symbol  
            FROM fiscdata;"""

    #Creating the sqlalchemy engine and read sample data.
    engine = sqlalchemy.create_engine("mysql+pymysql://root:"+serverpass+"@localhost:3306/stockdata")
    tickers = pd.read_sql(query, engine)
    return tickers


#If you have multiple keys this function cycles trough them. Dont be like me.
def keyCounter():
    global key_counter 
    key_counter +=1
    if (key_counter >= len(apikey)): key_counter = 0
            
    
#Main
def main():
    
    fiscStockData = getSnP500data() 
    write_data_to_sql(fiscStockData, "fiscdata", if_exists = "replace")    
    snpTickers = read_snp_tickers()
    
    #Do not currently want to wait it to fetch all 500 stocks so subsetting the amout for now.
    stockdata = read_data_daily_and_ti(snpTickers["Symbol"][0:20].tolist())   
    write_data_to_sql(stockdata, "dailydata")


main()

