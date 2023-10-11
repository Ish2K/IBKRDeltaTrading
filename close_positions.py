from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import *
from ibapi.ticktype import *
from ibapi.tag_value import TagValue
import json

import logging
import threading
import random
import time

with open('input.json', 'r') as f:
    config = json.load(f)

logging.basicConfig(filename='./logs/closing_positions.log', level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s',\
                    datefmt='%m/%d/%Y %I:%M:%S %p', filemode='w')

account_id = config['account_id']
host = config['host']
port = config['port']
client_id = random.randint(100, 1000)

class IBapi(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.nextorderId = 100
        self.all_positions = []

    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        self.nextorderId = orderId
        # print('The next valid order id is: ', self.nextorderId)
        logging.info('The next valid order id is: %s', self.nextorderId)
        self.reqPositions()
    
    def close_positions(self):

        for position in self.all_positions:
            contract = position[0]
            position = position[1]

            contract.exchange = "SMART"
            order = Order()
            order.action = 'BUY' if position < 0 else 'SELL'
            order.orderType = 'MARKET'
            order.totalQuantity = int(abs(position))
            order.algoStrategy = "Adaptive"
            order.algoParams = []
            order.algoParams.append(TagValue("adaptivePriority", "Urgent"))
            order.eTradeOnly = False
            order.firmQuoteOnly = False

            self.placeOrder(self.nextorderId, contract, order)
            self.nextorderId += 1

    def position(self, account: str, contract: Contract, position: float, avgCost: float):
        super().position(account, contract, position, avgCost)

        # logging.info("Position. Account: %s, Symbol: %s, SecType: %s, Currency: %s, ContractRight: %s, Position: %s, Avg cost: %s, Delta Neutral Combo: %s", account, contract.symbol, contract.secType, contract.currency, contract.right, str(position), str(avgCost), contract.deltaNeutralContract)
        if((contract.secType == 'STK') or (contract.secType == 'OPT')):
            if(abs(position) >= 1):
                # self.close_position(contract, position)
                self.all_positions.append([contract, position])

    def positionEnd(self):
        super().positionEnd()
        logging.info("PositionEnd")
        self.cancelPositions()
        self.close_positions()
        # self.reqPositions()
        # print("PositionEnd")

app = IBapi()

def run_loop():
	app.run()

def close_all_positions():
    global app

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
    
    time.sleep(60)
    app.disconnect()

# close_all_positions()