import json
import os
import threading, logging, time
import signal
from kafka import KafkaConsumer, KafkaProducer

consumer_stop = threading.Event()

#bytes to array function
def bytes_to_array(bytes):
    return json.loads(bytes.decode('utf-8'))

class Consumer(threading.Thread):

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='kafka:9092',
                                auto_offset_reset='earliest')
        consumer.subscribe(['coordenadas'])
        self.valid = 0
        self.invalid = 0
        self.msg = None

        for message in consumer:
            if message.value != None and message.value != self.msg:
                self.valid += 1
                self.msg = bytes_to_array(message.value)

            else:
                self.invalid += 1
                consumer_stop.set()
                break

            if consumer_stop.is_set():
                self.msg = None
                break
    
            consumer.close()


def add_value_to_file(value):
    if not os.path.exists('./output/data.txt'):
        f = open('./output/data.txt', 'w')
        f.write(value+'\n')
        f.close()
    else:
        f = open('./output/data.txt', 'a')
        f.write(value+'\n')
        f.close()


'''async def consume():
    consumer = AIOKafkaConsumer(
        'coordenadas',
        bootstrap_servers='kafka:9092')
    await consumer.start()
    try:
        async for msg in consumer:
            print(msg)
            return json.loads(msg.value)
    finally:
        await consumer.stop()

async def coordenadas():
    while True:
        try:
            data = await asyncio.wait_for(consume(), 60)
            add_value_to_file(str(data))
            print(data)
        except asyncio.TimeoutError:
            print('timeout')'''

def main():
    threads = Consumer()

    threads.start()
    
    time.sleep(10)
    print('Message received: %s' % threads.msg)
    print('Invalid messages: %d' % threads.invalid)
    print('Valid messages: %d' % threads.valid)
    consumer_stop.set()
    threads.join()


    

if __name__ == '__main__':
    #logging.basicConfig(
     #   format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
      #  level=logging.INFO
       # )
    main()