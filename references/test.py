import time
import pytz
import datetime

st = time.time()
current_time = datetime.datetime.now(pytz.timezone('US/Eastern'))
en = time.time()

print(en-st)