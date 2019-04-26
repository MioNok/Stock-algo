###
#This script will initially populate the database with the stock data from alphavantage and the tickers and fisc data from datahub.
#Some of the fuctions are used in the other scripts
###

import pandas as pd
import sqlalchemy
import time
import alpaca_trade_api as tradeapi


#This works when assuming host is localhost.
#Later version will use a cloud provider like AWS.

#Edit these
serverpass = "defaultpass" #insert your mysql serverpassword
serveruser = "root" #insert your mysql serverpassword
database = "stockdata" #database in your mysql you want to use. Need to be setup before running (Create DATABASE DatabaseName)
serverAddres = "@localhost"
serverSite = "mysql+pymysql://"+serveruser+":"+serverpass+serverAddres+":3306/"+database

#Alpaca tradeApi
api = tradeapi.REST(
    key_id="",
    secret_key="",
    base_url="https://paper-api.alpaca.markets"
)

def getSnP500data():
    snp500data = pd.read_csv("https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv")
    print("S&P500 data fetched")
    return snp500data
    

#Legacy, but if needed the dow tickers are still here.
#dowTickers = ["AXP","AAPL","BA","CAT","CSCO","CVX","DIS","DOW","GS",
#              "HD","IBM","INTC","JNJ","JPM","KO","MCD","MMM", "MRK",
#              "MSFT","NKE","PFE","PG","TRV","UNH","UTX","V","VZ","WBA",
#              "WMT","XOM"]


def read_data_daily_alpaca(tickers, time_period = "day"):
    current_trade_prices = None
    index = 0
    while index <=len(tickers):
        if current_trade_prices is None:
            current_trade_prices = api.get_barset(tickers[index:index+100],time_period,limit = 500)
        else:
            current_trade_prices.update(api.get_barset(tickers[index:index+100],time_period,limit = 500))
        index += 100
        
    price_df = current_trade_prices.df    

    
    print("Found data for", int(price_df.shape[1]/5),"stocks.")
    
    #Writing the whole dataframe to the sql server returs a "too many columns error". 
    #Hence we will split up the dataframe so that all close/open values are under one column etc.
    index = 0
    stockdata = pd.DataFrame()
    while index <= price_df.shape[1]:
        temp_df = price_df.iloc[:,index:index+5]
        
        #Save ticker before changing colnames. have to change colnames in order to append them all together.
        ticker = list(temp_df)[0][0]
        temp_df.columns = ["open","high","low","close","volume"]
        
        #Get the ticker and the timestamp of the data
        temp_df["ticker"] = ticker #ticker is hidden in the colname which is a tuple
        temp_df["timestamp"] = temp_df.index #timestamp is hidden in the index.
        
        stockdata = stockdata.append(temp_df, ignore_index = True)
        index += 5
        
        if ticker == tickers.iloc[-1]: 
            break
        
    return stockdata
    

#Depending on if you are fetcing the data all at once or not appending or replaing the data might be the right option.
def write_data_to_sql(df, table_name, if_exists = "replace"):
    #you need mysql alchemy and pymysql to run this. syntax is:  Username:password@host:port/database
    engine = sqlalchemy.create_engine(serverSite)
    #Writing the data, name is table name. 
    df.to_sql(name = table_name, con = engine,index = False,  if_exists = if_exists)

def read_snp_tickers():
    query = """SELECT Symbol  
            FROM fiscdata;"""

    #Creating the sqlalchemy engine and read sample data.
    engine = sqlalchemy.create_engine(serverSite)
    tickers = pd.read_sql(query, engine)
    return tickers


def read_from_database(query):    
    #Creating the sqlalchemy engine and read sample data.
    engine = sqlalchemy.create_engine(serverSite)
    fetchedData = pd.read_sql(query, engine)    
    return fetchedData
    
    
    
#Main
def db_main():

    fiscStockData = getSnP500data() 
    write_data_to_sql(fiscStockData, "fiscdata", if_exists = "replace")    
    snpTickers = read_from_database("""SELECT Symbol  
                                  FROM fiscdata;""")

    stockdata = read_data_daily_alpaca(snpTickers.Symbol)
    write_data_to_sql(stockdata, "dailydata")


if __name__ == "__main__":
    db_main()
