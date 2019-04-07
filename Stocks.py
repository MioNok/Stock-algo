#Api key ##

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

apikey = "apikey" # Insert apikey.

msft = pd.read_csv("https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED"+
                   "&symbol=MSFT"+
                   "&interval=15min"+
                   "&outputsize=full"+
                   "&time_period=5"+
                   "&datatype=csv"+
                   "&apikey="+apikey)

msft_scalped = msft.iloc[0:msft.shape[0]-199]
msft_scalped["timestamp"] = pd.to_datetime(msft_scalped["timestamp"])

msft_sma200 = pd.read_csv("https://www.alphavantage.co/query?function=SMA"+
                   "&symbol=MSFT"+
                   "&interval=daily"+
                   "&series_type=close"+
                   "&time_period=200"+
                   "&datatype=csv"+
                   "&apikey="apikey)

msft_sma200["time"] = pd.to_datetime(msft_sma200["time"])


#WORKS!
plt.plot(msft_scalped["timestamp"],msft_scalped["close"])
plt.plot(msft_sma200["time"],msft_sma200["SMA"])
plt.show()

