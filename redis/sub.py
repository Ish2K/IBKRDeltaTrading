import redis
import time

r = redis.StrictRedis(host='localhost', port=6379, db=0)
p = r.pubsub()

while True:
    p.subscribe('test')
    message = p.get_message()
    if message:
        print ("Subscriber: %s" % message['data'])
    time.sleep(1)
    p.unsubscribe('test')