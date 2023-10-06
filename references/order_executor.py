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
import logging

redis_client = redis.Redis(host=os.getenv('REDIS_HOST'), port=os.getenv('REDIS_PORT'), db=0, password=os.getenv('REDIS_PASSWORD'))
sub = redis_client.pubsub()
orderIdMap = {}
sub.subscribe( os.getenv('REDIS_OPTION_CHANNEL') )
logging.basicConfig(filename='./logs/order_executor.log', level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s',\
                        datefmt='%m/%d/%Y %I:%M:%S %p', filemode='w')

class IBapi(EWrapper, EClient):
	def __init__(self):
		EClient.__init__(self, self)

	def nextValidId(self, orderId: int):
		super().nextValidId(orderId)
		self.nextorderId = orderId
		logging.info('The next valid order id is: {}'.format(self.nextorderId))

	def orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
		# print('orderStatus - orderid:', orderId, "Symbol", orderIdMap[orderId]["Symbol"], 'status:', status, 'filled', filled, 'remaining', remaining, 'lastFillPrice', lastFillPrice)
		logging.info('orderStatus - orderid: {} Symbol: {} status: {} filled: {} remaining: {} lastFillPrice: {}'.format(orderId, orderIdMap[orderId]["Symbol"], status, filled, remaining, lastFillPrice))
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
		logging.info('openOrder id: {} symbol: {} secType: {} @ {} : {} {} {} {} {}'.format(orderId, contract.symbol, contract.secType, contract.exchange, order.action, order.orderType, order.totalQuantity, orderState.status, orderState.initMarginChange))
		orderIdMap[orderId] = {
			"Symbol": contract.symbol,
			"Action": order.action
		}

	def execDetails(self, reqId, contract, execution):
		print('Order Executed: ', reqId, contract.symbol, contract.secType, contract.currency, execution.execId, execution.orderId, execution.shares, execution.lastLiquidity)
		logging.info('Order Executed: {} {} {} {} {} {} {} {}'.format(reqId, contract.symbol, contract.secType, contract.currency, execution.execId, execution.orderId, execution.shares, execution.lastLiquidity))

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

def process_order(order_data):
    order_size = order_data['delta_position'] - order_data['stock_position']
    if(abs(order_size) < 1):
        return
    
    order = Order()
    order.action = 'BUY' if order_size > 0 else 'SELL'
    order.orderType = 'MARKET'
    order.totalQuantity = int(abs(order_size))
    order.eTradeOnly = False
    order.firmQuoteOnly = False

    contract = Stock_order(order_data['symbol'])

    logging.info('Placing order for {} {} shares'.format(order.action, order.totalQuantity))
    logging.info('Contract: {} {} {} {} {}'.format(contract.symbol, contract.secType, contract.currency, contract.exchange, contract.primaryExchange))

    app.placeOrder(app.nextorderId, contract, order)
    app.nextorderId += 1

def process_message(message: dict):
    logging.info('Received message: {}'.format(message))
    data = list(message.values())
    order_data = dict()

    current_time = datetime.datetime.now(pytz.timezone('US/Eastern'))
    current_minute = current_time.minute

    for rec in data:
        # print(rec)
        if (rec['symbol'] in order_data):
            order_data[rec['symbol']]['delta_position'] += rec['delta_position']
        else:
            order_data[rec['symbol']] = rec.copy()

    for symbol in order_data:
        if(redis_client.exists(symbol)):
             continue
        if(order_data[symbol]['delta_position'] == 0):
            process_order(order_data[symbol])
            redis_client.set(symbol, 1)
            redis_client.expire(symbol, 60)
            continue
        if(order_data[symbol]['stop_hedging']):
            continue
        # add datetime check here for frequency
        if((current_minute % order_data[symbol]['hedge_frequency']) == 0):
            process_order(order_data[symbol])
            redis_client.set(symbol, 1)
            redis_client.expire(symbol, 60)
            continue

def execute_orders():
	
    trading_config = config['trading']
    last_execution_time = {}
    current_thread = None
	
    for raw_message in sub.listen():
        if raw_message["type"] != "message":
            continue
        # print(raw_message)
        message = json.loads(raw_message["data"])
        # print(message)
        if(message):
            if((not current_thread) or (not current_thread.is_alive())):
                current_thread = threading.Thread(target=process_message, args=(message,), daemon=True)
                current_thread.start()

start_time = datetime.datetime.now(pytz.timezone('US/Eastern')).replace(hour=9, minute=35, second=0, microsecond=0)
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