#Api key ##

import pandas as pd
import sqlalchemy
import time

#This works when assuming you have a root user and the host is localhost.

#Edit these
apikey = ["InsertYourKeysHere"] # Insert apikey.
serverpass = "YourDBPassword" #isert your mysql server 
database = "stockdata" #database in your mysql you want to use.



#To start with I will read the DOW stocks daily dataand their 200SMA. (more to come..)
dowTickers = ["AXP","AAPL","BA","CAT","CSCO","CVX","DIS","DOW","GS",
              "HD","IBM","INTC","JNJ","JPM","KO","MCD","MMM", "MRK",
              "MSFT","NKE","PFE","PG","TRV","UNH","UTX","V","VZ","WBA",
              "WMT","XOM"]


def read_data_daily(tickers):
    rawStockDataDaily = pd.DataFrame()

    for ticker in tickers:
        try:
            tempRawStockDataDaily = pd.read_csv("https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED"+
                                                "&symbol="+ ticker +
                                                "&outputsize=full"+
                                                "&datatype=csv"+
                                                "&apikey="+ apikey)
        
            #Write the ticker name to the df to keep track of what data belong where.
            tempRawStockDataDaily["ticker"] = ticker
        
            print("Fetched " + ticker + " daily data" )
            #SMA200 needs 200 values to count the SMA, so the first 200 would ne null for us int the dataframe.
            #Avoiding this for now by just deleting the first 200 values. Will do this for every timeframe.
            tempRawStockDataDaily = tempRawStockDataDaily.iloc[0:tempRawStockDataDaily.shape[0]-199]
        
            tempRawStockDataSMA200 = pd.read_csv("https://www.alphavantage.co/query?function=SMA"+
                                                 "&symbol="+ ticker +
                                                 "&interval=daily"+
                                                 "&series_type=close"+
                                                 "&time_period=200"+
                                                 "&datatype=csv"+
                                                 "&apikey="+ apikey)
            print("Fetched " + ticker + " sma200 data" )
            
            #Adding the SMA data to the dataframe.
            tempRawStockDataDaily["sma200"] = tempRawStockDataSMA200["SMA"]
        except: print("The fetch failed. Most probably to many calls /minute. Consider starting over with a longer sleep time.")
        
        #Saving the reults for this ticker and moving on to the following ticker.
        rawStockDataDaily = rawStockDataDaily.append(tempRawStockDataDaily)
        time.sleep(20) #Free API key only gets you so far, as of writing this alphavantage is limiting the amount of API calls you can make in a minute..
      
    return (rawStockDataDaily)


def write_daily_data(dailyStockData):
    #you need mysql alchemy and pymysql to run this. syntax is:  Username:password@host:port/database
    engine = sqlalchemy.create_engine("mysql+pymysql://root:"+serverpass+"@localhost:3306/stockdata")
    #Writing the data, name is table name. 
    dailyStockData.to_sql(name = "stockdata", con = engine,index = False,  if_exists = "append")
    
    
#Main
stockdata = read_data_daily(dowTickers)    
write_daily_data(stockdata) 


