###
#This script will initially populate the database with the stock data from alphavantage and the tickers and fisc data from datahub.
#Some of the fuctions are used in the other scripts
###

import pandas as pd
import sqlalchemy
import time
import alpaca_trade_api as tradeapi


##################################

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

#IEX apikey
apikey = ""

##################################


def getSnP500data():
    snp500data = pd.read_csv("https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv")
    print("S&P500 data fetched")
    return snp500data

#This function is purely for backup purposes here
#The problem with the Alpaca data currently is that it only return adjusted daily data,
#However I need unadjusted data for the tehnical analysis. If for some reason the IEX API does not work, this can be used as a last resort.
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

def get_iex_data(tickers, timeframe):
    
    #Turn tickers to a str string for the api URL.
    symbols = ""
    for ticker in list(tickers):
        symbols = symbols+","+ticker
    
    str_tickers = symbols[1:]
        
    #To stay below the threshold of 500k "messages" per month for the free apikey we will search once 1m data, 
    #and then every day just the previous days data.
    if (timeframe == "1m"):
        stock_data = pd.read_json("https://cloud.iexapis.com/beta/stock/market/batch?symbols="+str_tickers+"&types=chart&range=1m&token="+apikey)
    
    elif(timeframe == "previous"): 
        stock_data = pd.read_json("https://cloud.iexapis.com/beta/stock/market/batch?symbols="+str_tickers+"&types=previous&token="+apikey)
    
    else:
        print("Unrecognized timeframe")
    
    #Transpose df for the loop
    stock_data = stock_data.transpose()
    
    stock_data_final = pd.DataFrame()
    
    for key, element in stock_data.iterrows():
        
        if (timeframe == "1m"):
            stock_data_temp = pd.DataFrame(element[0])
            stock_data_temp["ticker"] = element.name
        
        #pd dataframe requiers that the index is given if the result only has one row..
        
        elif (timeframe == "previous"):
            stock_data_temp = pd.DataFrame(element[0], index = [0])
            stock_data_temp.rename(columns={"symbol":"ticker"}, inplace=True)
        
        stock_data_final = stock_data_final.append(stock_data_temp)
    
    return stock_data_final
        

def read_data_daily_IEX(tickers,timeframe):
    
    price_df = None
    index = 0
    while index <= len(tickers):
        if price_df is None:
            price_df = get_iex_data(tickers[index:index+50],timeframe = timeframe)
        else:
            price_df= price_df.append(get_iex_data(tickers[index:index+50]),timeframe = timeframe)            
        index = index + 50
            
    #print("Found data for", int(price_df.shape[1]/5),"stocks.")
    
    return price_df
    

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
    
#When lauching the db for the first time the timeframe should be 1m, it drops the old tables and creates new one whit new data.
#Afterwards timeframe should be set to "previous". This meand that only the latets data is fetched and appended to the database.
def db_main(timeframe):

    fiscStockData = getSnP500data() 
    write_data_to_sql(fiscStockData, "fiscdata", if_exists = "replace")    
    snpTickers = read_from_database("""SELECT Symbol  
                                  FROM fiscdata;""")

    #stockdata = read_data_daily_alpaca(snpTickers.Symbol)
    stockdata = read_data_daily_IEX(snpTickers.Symbol, timeframe = timeframe)
    
    if (timeframe == "1m"):
        write_data_to_sql(stockdata, "dailydata", if_exists = "replace")
    elif(timeframe == "Previous"):
        write_data_to_sql(stockdata, "dailydata", if_exists = "append")


if __name__ == "__main__":
    db_main()
