#Just fortesting purposes, not an important script for the program
#Read you data from you SQL database.

import pandas as pd
import sqlalchemy
import matplotlib.pyplot as plt

#Edit these
serverpass = "defaultpass" #isert your mysql server 
database = "stockdata" #database in your mysql you want to use.


#Sample query I put together to showcase
query = """SELECT *  
        FROM stockdata
        WHERE timestamp between '2018-01-01' AND '2018-12-31'  
        AND ticker = 'INTC';"""

#Creating the sqlalchemy engine and read sample data.
engine = sqlalchemy.create_engine("mysql+pymysql://root:"+serverpass+"@localhost:3306/stockdata")

sampleData = pd.read_sql(query, engine)


sampleData["timestamp"] = pd.to_datetime(sampleData["timestamp"])

#Plot IBM data
plt.plot(sampleData["timestamp"],sampleData["close"])
plt.plot(sampleData["timestamp"],sampleData["sma200"])
plt.xlabel("Timestamps")
plt.ylabel("Stockprice")
plt.legend(["close","SMA200"])
plt.show()
