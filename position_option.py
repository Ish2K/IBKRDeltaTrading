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

pubsub.subscribe(os.getenv('REDIS_POSITION_OPTION_CHANNEL'))

def monitor_position():

    position_option = pd.DataFrame()
    for message in pubsub.listen():
        if message["type"] != "message":
            continue
        
        msg = json.loads(message['data'])
        # print(msg)
        path = config['data_path'] + 'position_option.csv'
        if(position_option.shape[0]==0):
            position_option = pd.concat([position_option, pd.DataFrame([msg])])
            position_option.to_csv(path, index=False, mode='w')
        elif(position_option[(position_option['symbol']==msg['symbol']) & (position_option['strike']==msg['strike']) & (position_option['lastTradeDateOrContractMonth']==msg['lastTradeDateOrContractMonth']) & (position_option['contractRight']==msg['contractRight'])].shape[0]==0):
            temp_df = pd.DataFrame([msg])
            position_option = pd.concat([position_option, temp_df])
            position_option.to_csv(path, index=False, mode='w')
        else:
            if(not msg['delta']):
                continue
            else:
                conditions = (position_option['symbol']==msg['symbol']) & (position_option['strike']==msg['strike']) & (position_option['lastTradeDateOrContractMonth']==msg['lastTradeDateOrContractMonth']) & (position_option['contractRight']==msg['contractRight'])
                position_option.loc[conditions, 'delta'] = msg['delta']
                position_option.to_csv(path, index=False, mode='w')
        

redis_thread = threading.Thread(target=monitor_position, daemon=True)
redis_thread.start()

time.sleep(40)
pubsub.close()
