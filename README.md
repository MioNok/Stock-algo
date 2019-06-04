# Stock-analysis
**Ongoing project** where I use different APIs to collect, store and analyze US stock market data. With this data I have built an alogrithm that will buy/sell stocks according to a single or a set of strategies. 

Stock data, past/active trades and watchlistst will be stored in a cloud SQL database. To view the results I have built an front-end using flask, bootstrap and some ready made themes to visualise the algorithms performance. The front-end is based on the scrips from this repo, they both interact with the same database. Some extra functionality has also been added to the front end such as user registration, personal watchlists and stock news.

To view the current progress of the front-end you can visit this link: **https://stockfront.appspot.com**.

I also plan to backtest these trading strategies and then build MLPs to further evaluate each trades chance of success using several technical indicators. This will also be used to manage the risk of the porfolio. I have formatting the data for this but no no NN's have been made for this.
