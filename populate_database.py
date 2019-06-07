
#This script contains the modules that the algo need to have to be able to inteact with the db. 

import pandas as pd
import sqlalchemy


def getSnP500data():
    snp500data = pd.read_csv("https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv")
    print("S&P500 data fetched")
    return snp500data

#This function is purely for backup purposes here
#The problem with the Alpaca data currently is that it only return adjusted daily data,
#However I need unadjusted data for the tehnical analysis. If for some reason the IEX API does not work this can be used.
def read_data_alpaca(tickers,api, time_period = "day"):
    current_trade_prices = None
    index = 0
    while index < len(tickers):
        if current_trade_prices is None:
            current_trade_prices = api.get_barset(tickers[index:index+100],time_period,limit = 1000)
        else:
            current_trade_prices.update(api.get_barset(tickers[index:index+100],time_period,limit = 1000))
        index += 100
        
    price_df = current_trade_prices.df    

    
    print("Found data for", int(price_df.shape[1]/5),"stocks.")
    
    #Writing the whole dataframe to the sql server returs a "too many columns error". 
    #Hence we will split up the dataframe so that all close/open values are under one column etc.
    index = 0
    stockdata = pd.DataFrame()
    while index <= price_df.shape[1]:
        try:
            temp_df = price_df.iloc[:,index:index+5]
            
            #Save ticker before changing colnames. have to change colnames in order to append them all together.
            ticker = list(temp_df)[0][0]
            print("Parsed",ticker)
            temp_df.columns = ["open","high","low","close","volume"]
            
            #Get the ticker and the timestamp of the data
            temp_df["ticker"] = ticker #ticker is hidden in the colname which is a tuple
            temp_df["timestamp"] = temp_df.index #timestamp is hidden in the index.
            
            stockdata = stockdata.append(temp_df, ignore_index = True)
            index += 5
            
            if ticker == tickers.iloc[-1]: 
                break
        except: print("Parsing failed")
        
    return stockdata


#Main data fetching function.
def get_iex_data(tickers, timeframe, apikey):
    
    #Turn tickers to a str string for the api URL.

    #To stay below the threshold of 500k "messages" per month for the free apikey we will search once 3m data, 
    #and then every day just the previous days data.
    index = 0
    stock_data_final = pd.DataFrame()
    while index <len(tickers):
        
        symbols = ""
        for ticker in list(tickers)[index:index+100]:
            symbols = symbols+","+ticker
    
        str_tickers = symbols[1:]
    
        if (timeframe == "3m"):
            stock_data = pd.read_json("https://cloud.iexapis.com/beta/stock/market/batch?symbols="+str_tickers+"&types=chart&range=3m&token="+apikey)
        
        elif(timeframe == "previous"): 
            stock_data = pd.read_json("https://cloud.iexapis.com/beta/stock/market/batch?symbols="+str_tickers+"&types=previous&token="+apikey)
        
        else:
            print("Unrecognized timeframe")
        
        #Transpose df for the loop
        stock_data = stock_data.transpose()
        
        for key, element in stock_data.iterrows():
            
            if (timeframe == "3m"):
                stock_data_temp = pd.DataFrame(element[0])
                stock_data_temp["ticker"] = element.name
            
            #pd dataframe requiers that the index is given if the result only has one row..
            
            elif (timeframe == "previous"):
                stock_data_temp = pd.Series(element[0]).to_frame()
                stock_data_temp = stock_data_temp.transpose()
                stock_data_temp.rename(columns={"symbol":"ticker"}, inplace=True)
            
            stock_data_final = stock_data_final.append(stock_data_temp)
            
        index += 100
        
    #Dropping column "0" that is generated from the to_frame and transpose.    
    #stock_data_final = stock_data_final.drop(stock_data_final.columns[0], axis=1) 
    return stock_data_final 


def get_iex_quotes(tickers, apikey):
    
    #Get the latest quotes for the tickers in the list. Used for the gapscanner.
    
    index = 0
    stock_quotes_final = pd.DataFrame()
    while index <len(tickers):
        
        symbols = ""
        for ticker in list(tickers)[index:index+100]:
            symbols = symbols+","+ticker
    
        str_tickers = symbols[1:]
    

        stock_quotes = pd.read_json("https://cloud.iexapis.com/beta/stock/market/batch?symbols="+str_tickers+"&types=quote&token="+apikey)
           
        #Transpose df for the loop
        stock_quotes = stock_quotes.transpose()
        
        for key, element in stock_quotes.iterrows():
            

            stock_data_temp = pd.Series(element[0]).to_frame()
            stock_data_temp = stock_data_temp.transpose()
            
            stock_data_temp.rename(columns={"symbol":"ticker"}, inplace=True)
            
            stock_quotes_final = stock_quotes_final.append(stock_data_temp)
            
        index += 100
    
    return stock_quotes_final 

#Depending on if you are fetcing the data all at once or not appending or replaing the data might be the right option.
def write_data_to_sql(df, table_name, serverSite, if_exists = "replace"  ):
    #you need mysql alchemy and pymysql to run this. syntax is:  Username:password@host:port/database
    engine = sqlalchemy.create_engine(serverSite)
    #Writing the data, name is table name. 
    df.to_sql(name = table_name, con = engine, index = False,  if_exists = if_exists)
    #Disposing the engine.
    engine.dispose()

#You can specify a limit on how many stocks you want to fetch if desired. 
def read_snp_tickers(serverSite, limit = 510 ):
    query = "SELECT Symbol FROM fiscdata LIMIT "+str(limit)+";"

    #Creating the sqlalchemy engine and read sample data.
    engine = sqlalchemy.create_engine(serverSite)
    tickers = pd.read_sql(query, engine)
    engine.dispose()
    return tickers


def read_from_database(query, serverSite):    
    #Creating the sqlalchemy engine and read sample data.
    engine = sqlalchemy.create_engine(serverSite)
    fetchedData = pd.read_sql(query, engine)
    engine.dispose()
    return fetchedData

    
#Main
    
#When lauching the db for the first time the timeframe should be 1m, it drops the old tables and creates new one whit new data.
#Afterwards timeframe should be set to "previous". This meand that only the latets data is fetched and appended to the database.
def db_main(server, apis, timeframe):
    
    
    #Db functions
    fiscStockData = getSnP500data() 
    write_data_to_sql(fiscStockData, "fiscdata", serverSite = server.serverSite, if_exists = "replace",  )
    
    #Get the tickers.
    snpTickers = read_snp_tickers(server.serverSite)

    #stockdata = read_data_daily_alpaca(snpTickers.Symbol)
    stockdata = get_iex_data(snpTickers.Symbol, timeframe = timeframe, apikey = apis.iexKey)
    
    #If the timeframe is 3m that means that completely new data is brought and repleace it with the old, in other cases just get the latest data and append it.
    if (timeframe == "3m"):
        write_data_to_sql(stockdata, "dailydata",serverSite = server.serverSite, if_exists = "replace" )
    elif(timeframe == "previous"):
        stockdata.drop( "change", axis = 1, inplace = True)
        stockdata.drop( 0 , axis = 1, inplace = True)
        write_data_to_sql(stockdata, "dailydata",serverSite = server.serverSite, if_exists = "append" )

