import pandas as pd
import os
import json
import redis
from dotenv import load_dotenv
load_dotenv()
import threading
import time
import logging
import pytz
import datetime
from utils import *


redis_client = redis.Redis(
    host=os.getenv('REDIS_HOST'),
    port=os.getenv('REDIS_PORT'),
    password=os.getenv('REDIS_PASSWORD')
)

r = redis_client.pubsub()
r.subscribe(os.getenv('REDIS_DELTA_CHANNEL'))

with open('input.json', 'r') as f:
    config = json.load(f)

def run_order_calculator():

    # set start time to 10am Eastern

    start_time = datetime.datetime.now(pytz.timezone('US/Eastern')).replace(hour=10, minute=0, second=0, microsecond=0)
    end_time = datetime.datetime.now(pytz.timezone('US/Eastern')).replace(hour=15, minute=56, second=0, microsecond=0)

    logging.basicConfig(filename='./logs/order_calculator.log', level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s',\
                        datefmt='%m/%d/%Y %I:%M:%S %p', filemode='w')

    current_time = datetime.datetime.now(pytz.timezone('US/Eastern'))

    while(current_time < start_time):
        time.sleep(1)
        current_time = datetime.datetime.now(pytz.timezone('US/Eastern'))

    logging.info('Order calculator started')

    for message in r.listen():
        if message["type"] != "message":
            continue

        msg = json.loads(message['data'])
        stock_position = pd.DataFrame(msg['stock']).drop_duplicates()
        position_option = pd.DataFrame(msg['option']).drop_duplicates()

        try:
            stock_position = calculate_stock_position(stock_position)
            # logging.info(stock_position)
            position_option = calculate_option_delta(position_option, config)
            # logging.info(position_option)
        except Exception as e:
            logging.error(e)
            continue
        # position_option["targe_delta"] = position_option.apply(lambda x: config['trading'][x['symbol']]['target_delta'] if (x['symbol'] in config['trading']) else 0, axis=1)

        # merge on symbol
        order_calculator = position_option.merge(stock_position, on=['symbol'], how='outer')

        # drop rows where delta is null
        order_calculator = order_calculator[order_calculator['delta'].notnull()]

        order_calculator = (calculate_adjustment(order_calculator).to_dict('records'))
        # print(order_calculator)
        logging.info(order_calculator)

        for rec in order_calculator:

            if(rec['adjustment']==0):
                continue
            msg = {
                "symbol": rec['symbol'],
                "order_size": rec['adjustment'],
                "closed_position": rec['idealPosition']==0,
            }

            redis_client.publish(os.getenv('REDIS_ORDER_CALCULATOR_CHANNEL'), json.dumps(msg))

# run_order_calculator()