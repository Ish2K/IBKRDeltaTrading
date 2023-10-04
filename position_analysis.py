import redis
import json
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()
import threading
import time

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

def process(option, position):

    for key in option:
        position[key] = option[key]
    
    return position

def monitor_position():

    position_option_mapper = {}
    stock_position = pd.DataFrame()

    for message in pubsub.listen():
        if message["type"] != "message":
            continue
        
        msg = json.loads(message['data'])

        if(msg['type'] == 'position'):
            if(msg['secType'] =='OPT'):
                position_option_mapper[msg['reqId']] = msg
            else:
                path = config['data_path'] + 'stock_position.csv'
                if(stock_position.shape[0]==0):
                    stock_position = pd.concat([stock_position, pd.DataFrame([msg])])
                    stock_position.to_csv(path, index=False, mode='w')
                elif(stock_position[stock_position['symbol']==msg['symbol']].shape[0]==0):
                    temp_df = pd.DataFrame([msg])
                    stock_position = pd.concat([stock_position, temp_df])
                    stock_position.to_csv(path, index=False, mode='w')
                else:
                    # update all columns of the row where msg['symbol'] == stock_position['symbol']
                    stock_position.loc[stock_position['symbol']==msg['symbol'], 'position'] = msg['position']
                    stock_position.to_csv(path, index=False, mode='w')
        
        elif(msg['type'] == 'tickOptionComputation'):
            if(msg['reqId'] in position_option_mapper):
                process_msg = process(msg, position_option_mapper[msg['reqId']])
                redis_client.publish(os.getenv('REDIS_POSITION_OPTION_CHANNEL'), json.dumps(process_msg))


redis_thread = threading.Thread(target=monitor_position, daemon=True)
redis_thread.start()

time.sleep(40)
pubsub.close()
