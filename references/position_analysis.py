import redis
import json
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()
import threading
import time

import datetime
import pytz
import logging

redis_client = redis.Redis(
    host=os.getenv('REDIS_HOST'),
    port=os.getenv('REDIS_PORT'),
    password=os.getenv('REDIS_PASSWORD')
    )

with open('input.json', 'r') as f:
    config = json.load(f)

pubsub = redis_client.pubsub()

pubsub.subscribe(os.getenv('REDIS_OPTION_CHANNEL'))
pubsub.subscribe(os.getenv('REDIS_POSITION_CHANNEL'))

logging.basicConfig(filename='./logs/position_analysis_2.log', level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s',\
                        datefmt='%m/%d/%Y %I:%M:%S %p', filemode='w')

def process(option, position):

    for key in option:
        position[key] = option[key]
    
    return position

def stock_position_analysis(stock_position, msg):
    if(stock_position.shape[0]==0):
        stock_position = pd.concat([stock_position, pd.DataFrame([msg])])
        # stock_position.to_csv(path, index=False, mode='w')
    elif(stock_position[stock_position['symbol']==msg['symbol']].shape[0]==0):
        temp_df = pd.DataFrame([msg])
        stock_position = pd.concat([stock_position, temp_df])
        # stock_position.to_csv(path, index=False, mode='w')
    else:
        # update all columns of the row where msg['symbol'] == stock_position['symbol']
        # print("Position Updated!")
        logging.info('Updating stock position for symbol: {} to {}'.format(msg['symbol'], msg['position']))
        stock_position.loc[stock_position['symbol']==msg['symbol'], 'position'] = msg['position']
        # stock_position.to_csv(path, index=False, mode='w')
    
    return stock_position

def option_position_analysis(position_option, msg):

    if(position_option.shape[0]==0):
        position_option = pd.concat([position_option, pd.DataFrame([msg])])
        # position_option.to_csv(path, index=False, mode='w')
    elif(position_option[(position_option['symbol']==msg['symbol']) & (position_option['strike']==msg['strike']) & (position_option['lastTradeDateOrContractMonth']==msg['lastTradeDateOrContractMonth']) & (position_option['contractRight']==msg['contractRight'])].shape[0]==0):
        temp_df = pd.DataFrame([msg])
        position_option = pd.concat([position_option, temp_df])
        # position_option.to_csv(path, index=False, mode='w')
    else:
        if(not msg['delta']):
            return position_option
        else:
            conditions = (position_option['symbol']==msg['symbol']) & (position_option['strike']==msg['strike']) & (position_option['lastTradeDateOrContractMonth']==msg['lastTradeDateOrContractMonth']) & (position_option['contractRight']==msg['contractRight'])
            position_option.loc[conditions, 'delta'] = msg['delta']
            position_option.loc[conditions, 'position'] = msg['position']
            # position_option.to_csv(path, index=False, mode='w')
    return position_option

def monitor_position():

    position_option_mapper = {}
    stock_position = pd.DataFrame()
    position_option = pd.DataFrame()

    for message in pubsub.listen():
        if message["type"] != "message":
            continue
        
        msg = json.loads(message['data'])

        if(msg['type'] == 'position'):
            if(msg['secType'] =='OPT'):
                position_option_mapper[msg['reqId']] = msg
            else:
                stock_position = stock_position_analysis(stock_position, msg)
                
        
        elif(msg['type'] == 'tickOptionComputation'):
            if(msg['reqId'] in position_option_mapper):
                process_msg = process(msg, position_option_mapper[msg['reqId']])
                position_option = option_position_analysis(position_option, process_msg)
        msg = {
            "option": position_option.to_dict(orient='records'),
            "stock": stock_position.to_dict(orient='records')
        }
        
        redis_client.publish(os.getenv('REDIS_DELTA_CHANNEL'), json.dumps(msg))

def run_position_analysis():

    start_time = datetime.datetime.now(pytz.timezone('US/Eastern')).replace(hour=10, minute=0, second=0, microsecond=0)
    end_time = datetime.datetime.now(pytz.timezone('US/Eastern')).replace(hour=15, minute=56, second=0, microsecond=0)

    current_time = datetime.datetime.now(pytz.timezone('US/Eastern'))

    while(current_time < start_time):
        time.sleep(1)
        current_time = datetime.datetime.now(pytz.timezone('US/Eastern'))
    
    redis_thread = threading.Thread(target=monitor_position, daemon=True)
    redis_thread.start()

    seconds_left = ((end_time - current_time).seconds)

    time.sleep(int(seconds_left))
    pubsub.close()

# run_position_analysis()