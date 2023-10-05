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
    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        self.nextorderId = orderId
        # print('The next valid order id is: ', self.nextorderId)
        logging.info('The next valid order id is: %s', self.nextorderId)
        self.reqPositions()
    
    def position(self, account: str, contract: Contract, position: float, avgCost: float):
        super().position(account, contract, position, avgCost)

        # logging.info("Position. Account: %s, Symbol: %s, SecType: %s, Currency: %s, ContractRight: %s, Position: %s, Avg cost: %s, Delta Neutral Combo: %s", account, contract.symbol, contract.secType, contract.currency, contract.right, str(position), str(avgCost), contract.deltaNeutralContract)
        if((contract.secType == 'STK') or (contract.secType == 'OPT')):
        
            msg = {
                'time' : datetime.datetime.now(pytz.timezone('US/Eastern')).strftime("%Y-%m-%d %H:%M:%S"),
                'reqId': self.nextorderId,
                'type': 'position',
                'account': account,
                'symbol': contract.symbol,
                'secType': contract.secType,
                'currency': contract.currency,
                'contractRight': contract.right,
                'lastTradeDateOrContractMonth': contract.lastTradeDateOrContractMonth,
                'strike': contract.strike,
                'position': position,
                'avgCost': avgCost,
                'deltaNeutralContract': contract.deltaNeutralContract
            }

            redis_client.publish(os.getenv('REDIS_POSITION_CHANNEL'), json.dumps(msg))
            
            contract.exchange = 'SMART'

            self.reqMktData(self.nextorderId, contract, "", False, False, [])
            self.nextorderId += 1

    def positionEnd(self):
        super().positionEnd()
        logging.info("PositionEnd")
        # print("PositionEnd")
    
    def tickOptionComputation(self, reqId: int, tickType: int, tickAttrib: int,
                          impliedVol: float, delta: float, optPrice: float, pvDividend: float,
                          gamma: float, vega: float, theta: float, undPrice: float):
        super().tickOptionComputation(reqId, tickType, tickAttrib, impliedVol, delta,
                                    optPrice, pvDividend, gamma, vega, theta, undPrice)
        # logging.info("TickOptionComputation. TickerId: %s, TickType: %s, TickAttrib: %s, ImpliedVolatility: %s, Delta: %s, OptionPrice: %s, pvDividend: %s, Gamma: %s, Vega: %s, Theta: %s, UnderlyingPrice: %s", reqId, tickType, str(tickAttrib), str(impliedVol), str(delta), str(optPrice), str(pvDividend), str(gamma), str(vega), str(theta), str(undPrice))

        msg = {
            'time' : datetime.datetime.now(pytz.timezone('US/Eastern')).strftime("%Y-%m-%d %H:%M:%S"),
            'reqId': reqId,
            'type': 'tickOptionComputation',
            # 'tickType': tickType,
            # 'tickAttrib': tickAttrib,
            # 'impliedVol': impliedVol,
            'delta': delta,
            # 'optPrice': optPrice,
            # 'pvDividend': pvDividend,
            # 'gamma': gamma,
            # 'vega': vega,
            # 'theta': theta,
            # 'undPrice': undPrice
        }

        redis_client.publish(os.getenv('REDIS_OPTION_CHANNEL'), json.dumps(msg))


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

def run_position_monitoring():

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

    time.sleep(40)
    app.disconnect()
