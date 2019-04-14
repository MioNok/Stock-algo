###
#This script will initially populate the database with the stock data from alphavantage and the tickers and fisc data from datahub.
#Some of the fuctions are used in the other scripts
###

import pandas as pd
import sqlalchemy
import time


#This works when assuming host is localhost.
#Later version will use a cloud provider like AWS.

#Edit these
apikey = ["insertApikey"] # Insert Alphavantage apikey.
serverpass = "defaultpass" #insert your mysql serverpassword
serveruser = "root" #insert your mysql serverpassword
database = "stockdata" #database in your mysql you want to use. Need to be setup before running (Create DATABASE DatabaseName)
serverSite = "mysql+pymysql://"+serveruser+":"+serverpass+"@localhost:3306/"+database

#How long to sleep between search cycles
sleeptime = 5

key_counter = 0

def getSnP500data():
    snp500data = pd.read_csv("https://datahub.io/core/s-and-p-500-companies-financials/r/constituents-financials.csv")
    print("S&P500 data fetched")
    return snp500data.drop(["SEC Filings"], axis =1)
    

#Legacy, but if needed the dow tickers are still here.
dowTickers = ["AXP","AAPL","BA","CAT","CSCO","CVX","DIS","DOW","GS",
              "HD","IBM","INTC","JNJ","JPM","KO","MCD","MMM", "MRK",
              "MSFT","NKE","PFE","PG","TRV","UNH","UTX","V","VZ","WBA",
              "WMT","XOM"]

#Uses the dow tickers if nothing else is specified.
def read_data_daily(tickers = dowTickers, outputsize = "full", saveLatestOnly = False):
    rawStockDataDaily = pd.DataFrame()

    for ticker in tickers:
        retry = True
        retry_counter = 0
        
        while retry:
            tempRawStockDataDaily = pd.read_csv("https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED"+
                                                "&symbol="+ ticker +
                                                "&outputsize="+outputsize+
                                                "&datatype=csv"+
                                                "&apikey="+ apikey[key_counter])
            
            #Check that the data is correct
            if tempRawStockDataDaily.shape[1] == 9:
                 #Write the ticker name to the df to keep track of what data belong where.
                tempRawStockDataDaily["ticker"] = ticker
                print("Fetched " + ticker + " daily data" )
                keyCounter()
                
                #Saving the reults for this ticker and moving on to the following ticker.
                if saveLatestOnly:
                    rawStockDataDaily = rawStockDataDaily.append(tempRawStockDataDaily.iloc[0:])
                else:
                    rawStockDataDaily = rawStockDataDaily.append(tempRawStockDataDaily)
                
                #Free API key only gets you so far, as of writing this alphavantage is limiting the amount of API calls you can make in a minute..
                time.sleep(sleeptime)
                retry = False
                
            else: 
                #Fetch has failed. Think about what you have done and try again.
                retry_counter += 1 # counting the retrys, if above 10, stop pining the server, its not happening.
                print("Retrying to fetch ", ticker)
                
                if retry_counter == 5:
                    print("The fetch has failed 5 times in a row, something is wrong with the server, your api call or your key. Jumping over this one.")
                    retry = False
                #Lets try again.    
                else: 
                    retry = True
                time.sleep(sleeptime)
            
    return rawStockDataDaily


    
#Depending on if you are fetcing the data all at once or not appending or replaing the data might be the right option.
def write_data_to_sql(dailyStockData, table_name, if_exists = "replace"):
    #you need mysql alchemy and pymysql to run this. syntax is:  Username:password@host:port/database
    engine = sqlalchemy.create_engine(serverSite)
    #Writing the data, name is table name. 
    dailyStockData.to_sql(name = table_name, con = engine,index = False,  if_exists = if_exists)

def read_snp_tickers():
    query = """SELECT Symbol  
            FROM fiscdata;"""

    #Creating the sqlalchemy engine and read sample data.
    engine = sqlalchemy.create_engine(serverSite)
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
    stockdata = read_data_daily(snpTickers["Symbol"][0:2].tolist())   
    write_data_to_sql(stockdata, "dailydata")


if __name__ == "__main__":
    main()

