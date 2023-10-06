from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import *

import threading
import time
import redis
import json

import logging 
import os
from dotenv import load_dotenv
load_dotenv()
import pytz
import datetime

with open('input.json', 'r') as f:
    config = json.load(f)

logging.basicConfig(filename='./logs/position_monitoring.log', level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s',\
                    datefmt='%m/%d/%Y %I:%M:%S %p', filemode='w')

account_id = config['account_id']
host = config['host']
port = config['port']
client_id = 1

redis_client = redis.Redis(
  host=os.getenv('REDIS_HOST'),
  port=os.getenv('REDIS_PORT'),
  password=os.getenv('REDIS_PASSWORD')
)

class IBapi(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.nextorderId = 100
        self.reqMapper = dict()
        self.contractMapper = dict()
        self.stock_position = dict()
        self.option_position = dict()
        self.option_data = dict()
        self.order_thread = None

    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        self.nextorderId = orderId
        # print('The next valid order id is: ', self.nextorderId)
        logging.info('The next valid order id is: %s', self.nextorderId)
        self.reqPositions()
    
    def position(self, account: str, contract: Contract, position: float, avgCost: float):
        super().position(account, contract, position, avgCost)

        # logging.info("Position. Account: %s, Symbol: %s, SecType: %s, Currency: %s, ContractRight: %s, Position: %s, Avg cost: %s, Delta Neutral Combo: %s", account, contract.symbol, contract.secType, contract.currency, contract.right, str(position), str(avgCost), contract.deltaNeutralContract)
        if((contract.secType == 'STK')):
            self.stock_position[contract.symbol] = float(position)
            # redis_client.publish(os.getenv('REDIS_STOCK_POSITIONS'), json.dumps(self.stock_position))

        elif((contract.secType == 'OPT')):
            contract_details_str = contract.symbol + "_" + contract.lastTradeDateOrContractMonth + "_" + str(contract.strike) + "_" + contract.right
            if(contract_details_str in self.contractMapper):
                reqId = self.contractMapper[contract_details_str]
                self.reqMapper[reqId] = {
                    'symbol': contract.symbol,
                    'strike': contract.strike,
                    'lastTradeDateOrContractMonth': contract.lastTradeDateOrContractMonth,
                    'contractRight': contract.right,
                    'position': float(position)
                }
            else:
                reqId = self.nextorderId
                self.nextorderId += 1
                self.contractMapper[contract_details_str] = reqId
            
                self.reqMapper[reqId] = {
                    'symbol': contract.symbol,
                    'strike': contract.strike,
                    'lastTradeDateOrContractMonth': contract.lastTradeDateOrContractMonth,
                    'contractRight': contract.right,
                    'position': float(position)
                }
                contract.exchange = 'SMART'
                self.reqMktData(reqId, contract, "", False, False, [])


    def positionEnd(self):
        super().positionEnd()
        logging.info("PositionEnd")
        # self.cancelPositions()
        # self.reqPositions()
        # print("PositionEnd")
    
    def tickOptionComputation(self, reqId: int, tickType: int, tickAttrib: int,
                          impliedVol: float, delta: float, optPrice: float, pvDividend: float,
                          gamma: float, vega: float, theta: float, undPrice: float):
        super().tickOptionComputation(reqId, tickType, tickAttrib, impliedVol, delta,
                                    optPrice, pvDividend, gamma, vega, theta, undPrice)
        # logging.info("TickOptionComputation. TickerId: %s, TickType: %s, TickAttrib: %s, ImpliedVolatility: %s, Delta: %s, OptionPrice: %s, pvDividend: %s, Gamma: %s, Vega: %s, Theta: %s, UnderlyingPrice: %s", reqId, tickType, str(tickAttrib), str(impliedVol), str(delta), str(optPrice), str(pvDividend), str(gamma), str(vega), str(theta), str(undPrice))

        if(not delta):
            return
        contract_details_str = self.reqMapper[reqId]['symbol'] + "_" + self.reqMapper[reqId]['lastTradeDateOrContractMonth'] + "_" + str(self.reqMapper[reqId]['strike']) + "_" + self.reqMapper[reqId]['contractRight']
        symbol = self.reqMapper[reqId]['symbol']

        target_delta = config['default_target_delta']
        hedge_frequeny = config['default_hedge_frequency']
        stop_hedging = config['default_stop_hedging']

        if(symbol in config['trading']):
            target_delta = config['trading'][symbol]['target_delta']
            hedge_frequeny = config['trading'][symbol]['hedge_frequency']
            stop_hedging = config['trading'][symbol]['stop_hedging']
        
        msg = {
            'symbol': symbol,
            'strike': self.reqMapper[reqId]['strike'],
            'lastTradeDateOrContractMonth': self.reqMapper[reqId]['lastTradeDateOrContractMonth'],
            'contractRight': self.reqMapper[reqId]['contractRight'],
            'delta_position': (target_delta - delta) * self.reqMapper[reqId]['position'] * 100,
            'stock_position': (self.stock_position[symbol] if symbol in self.stock_position else 0),
            'hedge_frequency': hedge_frequeny,
            'stop_hedging': (1 if stop_hedging else 0)
            
        }

        # print(msg)

        self.option_data[contract_details_str] = msg
        # redis_client.publish(os.getenv('REDIS_OPTION_CHANNEL'), json.dumps(self.option_data))

        if(self.order_thread is None):
            self.order_thread = threading.Thread(target=process_message, args=(self.option_data,), daemon=True)
            self.order_thread.start()
        elif(not self.order_thread.is_alive()):
            self.order_thread = threading.Thread(target=process_message, args=(self.option_data,), daemon=True)
            self.order_thread.start()


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
    current_time = datetime.datetime.now(pytz.timezone('US/Eastern'))
    if(not ((current_time > trading_start_time) and (current_time < end_time))):
        return
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

start_time = datetime.datetime.now(pytz.timezone('US/Eastern')).replace(hour=9, minute=30, second=0, microsecond=0)
trading_start_time = datetime.datetime.now(pytz.timezone('US/Eastern')).replace(hour=10, minute=0, second=0, microsecond=0)
end_time = datetime.datetime.now(pytz.timezone('US/Eastern')).replace(hour=15, minute=56, second=0, microsecond=0)
current_time = datetime.datetime.now(pytz.timezone('US/Eastern'))

while(current_time < start_time):
    time.sleep(1)
    current_time = datetime.datetime.now(pytz.timezone('US/Eastern'))

app = IBapi()
app.connect(host=host, port=port, clientId=client_id)

app.nextorderId = None

#Start the socket in a thread
api_thread = threading.Thread(target=run_loop, daemon=True)
api_thread.start()

#Check if the API is connected via orderid
while True:
	if isinstance(app.nextorderId, int):
		print('connected')
		break
	else:
		print('waiting for connection')
		time.sleep(1)

seconds_left = str((end_time - current_time).seconds)

print("Seconds left to market close: " + str((end_time - current_time).seconds))

time.sleep(int(seconds_left))
app.disconnect()
