#Models

import pandas as pd
import populate_database as db
import alpaca_trade_api as tradeapi

class Server:
    def __init__(self, user, password, address, database):
        self.user = user
        self.password = password
        self.address = address
        self.database = database
        self.serverSite = str("mysql+pymysql://"+self.user+":"+self.password+self.address+":3306/"+self.database)
        
class APIs:
    def __init__(self, alpacaKey, alpacaSKey, iexKey):
        self.alpacaKey = alpacaKey
        self.alpacaSkey = alpacaSKey
        self.iexKey = iexKey
        
        self.alpacaApi = tradeapi.REST(
                       key_id = self.alpacaKey,
                       secret_key = self.alpacaSkey,
                       base_url="https://paper-api.alpaca.markets"
                       ) 
        
class Trade:
    def __init__(self, ticker, posSize, orderSide, timeStamp, strategy):
        self.ticker = ticker
        self.posSize = posSize
        self.orderSide = orderSide
        self.timeStamp = timeStamp
        self.stopPrice = 0
        self.orderID = 0
        self.entryPrice = 0
        self.currentPrice = 0
        self.costBasis = 0
        self.unrealPL = 0
        self.unrealPLprocent = 0
        self.targetPrice = 0
        self.last15MinCandle = None
        self.strategy = strategy
        print("An trade has been created for ticker", self.ticker)
        
    def submitOrder(self, apis):
        order = apis.alpacaApi.submit_order(symbol = self.ticker,
                         qty = self.posSize,
                         side = self.orderSide,
                         type = "market",
                         time_in_force = "day")
        print("An order has been submitted for ", self.ticker, " qty: ", self.posSize)
        self.orderID = order.id
        self.updateTradeDb(action = "Initiated trade", initiated = True, apis = apis)
        
        
    def cancelOrder(orderID, apis):
        apis.alpacaApi.cancel_orderorder(orderID)
        
    def setStopPrice(self,stopPrice):
        self.stopPrice = float(stopPrice)
    
    def setLastCandle(self, candle):
        self.last15MinCandle = candle
        
    def flattenOrder(self, action, apis):
        flattenSide = ""
        if (self.orderSide == "buy"):
            flattenSide = "sell"
        else: flattenSide == "buy"
        
        #Save current trade specs.
        self.setPosition()
        
        apis.alpacaApi.submit_order(symbol = self.ticker,
                         qty = self.posSize,
                         side = flattenSide,
                         type = "market",
                         time_in_force = "day")
        print("An flatten order has been submitted for ", self.ticker, " qty: ", self.posSize)
        self.updateTradeDb(action = action, initiated = False, apis = apis)

    
    def setPosition(self, apis):
        pos = apis.alpacaApi.get_position(self.ticker)
        self.costBasis = pos.cost_basis
        self.unrealPL = pos.unrealized_pl
        self.unrealPLprocent = pos.unrealized_plpc
        self.entryPrice = float(pos.avg_entry_price)
        self.currentPrice = float(pos.current_price)
        
        #Keeping the history of all trades.
    def updateTradeDb(self, action, initiated, apis):
        now = str(apis.alpacaApi.get_clock().timestamp)[0:19]

        #Different update depending on if the order was initiated of flattend.
        if (initiated == False):
            dfData = {"Timestamp": [now],
                      "Ticker": [self.ticker],
                      "Size":[self.posSize],
                      "Side":[self.orderSide],
                      "Strategy":[self.strategy],
                      "Action":[action],
                      "Result":[self.unrealPL]}

        if (initiated):
            dfData = {"Timestamp": [now],
                      "Ticker": [self.ticker],
                      "Size":[self.posSize],
                      "Side":[self.orderSide],
                      "Strategy":[self.strategy],
                      "Action":[action],
                      "Result":["init"]}
        
        tradedb = pd.DataFrame(data = dfData)
        
        db.write_data_to_sql(tradedb,"tradehistory",if_exists = "append", serverSite = server.serverSite)
    