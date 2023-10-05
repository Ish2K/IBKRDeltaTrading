from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import *

import threading
import time
import redis
import json
from dotenv import load_dotenv
load_dotenv()
import os
import datetime
import pytz

redis_client = redis.Redis(host=os.getenv('REDIS_HOST'), port=os.getenv('REDIS_PORT'), db=0, password=os.getenv('REDIS_PASSWORD'))
sub = redis_client.pubsub()
orderIdMap = {}
sub.subscribe( os.getenv('REDIS_ORDER_CALCULATOR_CHANNEL') )

class IBapi(EWrapper, EClient):
	def __init__(self):
		EClient.__init__(self, self)

	def nextValidId(self, orderId: int):
		super().nextValidId(orderId)
		self.nextorderId = orderId
		print('The next valid order id is: ', self.nextorderId)

	def orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
		print('orderStatus - orderid:', orderId, "Symbol", orderIdMap[orderId]["Symbol"], 'status:', status, 'filled', filled, 'remaining', remaining, 'lastFillPrice', lastFillPrice)
		msg = {
			"status": status,
			"RemainingQuantity": remaining,
			"FilledQuantity": filled,
			"Symbol": orderIdMap[orderId]["Symbol"],
			"Action": orderIdMap[orderId]["Action"],
			"AvgFillPrice": avgFillPrice
		}
	
	def openOrder(self, orderId, contract, order, orderState):
		print('openOrder id:', orderId, contract.symbol, contract.secType, '@', contract.exchange, ':', order.action, order.orderType, order.totalQuantity, orderState.status)
		orderIdMap[orderId] = {
			"Symbol": contract.symbol,
			"Action": order.action
		}

	def execDetails(self, reqId, contract, execution):
		print('Order Executed: ', reqId, contract.symbol, contract.secType, contract.currency, execution.execId, execution.orderId, execution.shares, execution.lastLiquidity)


def run_loop():
	app.run()

def Stock_order(symbol):
    contract = Contract()
    contract.symbol = symbol
    contract.secType = 'STK'
    contract.exchange = 'SMART'
    contract.currency = 'USD'
    contract.primaryExchange = "NASDAQ"
    return contract

app = IBapi()
app.connect('127.0.0.1', 7497, 13)

app.nextorderId = None

#Start the socket in a thread
api_thread = threading.Thread(target=run_loop, daemon=True)
api_thread.start()

with open('input.json', 'r') as f:
    config = json.load(f)

#Check if the API is connected via orderid
while True:
	if isinstance(app.nextorderId, int):
		print('connected')
		break
	else:
		print('waiting for connection')
		time.sleep(1)

def execute_orders():
	
    trading_config = config['trading']
    last_execution_time = {}
    for raw_message in sub.listen():
        if raw_message["type"] != "message":
            continue
        # print(raw_message)
        message = json.loads(raw_message["data"])
        print(message)
        current_time = datetime.datetime.now(pytz.timezone('US/Eastern'))
        current_minute = current_time.minute
        if(message):
            if(message['symbol'] not in trading_config):
                trading_config[message['symbol']] = {
                    "target_delta":config["default_target_delta"],
                    "hedge_frequency":config["default_hedge_frequency"],
                    "stop_hedging": config["default_stop_hedging"]
                }

            if(message["closed_position"]):
                order = Order()
                if(message['order_size'] > 0):
                    order.action = "BUY"
                else:
                    order.action = "SELL"
                order.totalQuantity = abs(int(message["order_size"]))
                
                order.orderType = "MARKET"
                order.eTradeOnly = False
                order.firmQuoteOnly = False

                app.placeOrder(app.nextorderId, Stock_order(message["symbol"]), order)
                app.nextorderId += 1
            if(trading_config[message['symbol']]['stop_hedging']):
                continue

            if(message['symbol'] in last_execution_time):
                if((current_time - last_execution_time[message['symbol']]).seconds < 60):
                    continue
                  
            if((current_minute % trading_config[message['symbol']]['hedge_frequency']) == 0):
                order = Order()
                if(message['order_size'] > 0):
                    order.action = "BUY"
                else:
                    order.action = "SELL"
                order.totalQuantity = abs(int(message["order_size"]))
                
                order.orderType = "MARKET"
                order.eTradeOnly = False
                order.firmQuoteOnly = False

                app.placeOrder(app.nextorderId, Stock_order(message["symbol"]), order)
                app.nextorderId += 1

                last_execution_time[message['symbol']] = current_time

def run_executor():

    start_time = datetime.datetime.now(pytz.timezone('US/Eastern')).replace(hour=10, minute=0, second=0, microsecond=0)
    end_time = datetime.datetime.now(pytz.timezone('US/Eastern')).replace(hour=15, minute=56, second=0, microsecond=0)

    current_time = datetime.datetime.now(pytz.timezone('US/Eastern'))

    while(current_time < start_time):
        time.sleep(1)
        current_time = datetime.datetime.now(pytz.timezone('US/Eastern'))

    execution_thread = threading.Thread(target=execute_orders, daemon=True)
    execution_thread.start()

    while(current_time < end_time):
        time.sleep(1)
        current_time = datetime.datetime.now(pytz.timezone('US/Eastern'))

    time.sleep(3)

    #Cancel order 
    print('cancelling order')
    app.cancelOrder(app.nextorderId)

    time.sleep(3)
    app.disconnect()