import threading
import logging
import json
import os
import time
from kafka import KafkaConsumer
from db.conn import query

consumer_stop = threading.Event()

def bytes_to_array(bytes):
    return json.loads(bytes.decode('utf-8'))

class Consumer(threading.Thread):

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='kafka:9092',
                                auto_offset_reset='earliest')
        consumer.subscribe(['ventas'])
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

def list_to_txt(arr):
    if not os.path.exists('./output/data.txt'):
        f = open('./output/data.txt', 'w')
        f.write(arr+'\n')
        f.close()
    else:
        f = open('./output/data.txt', 'a')
        f.write(arr+'\n')
        f.close()

arr = []

def main():
    thread = Consumer()
    thread.start()

    patentes = query('SELECT patente FROM miembros WHERE stock <= 20')
    for patente in patentes:
        if len(arr) == 5:
            list_to_txt(arr)
            arr = []
            consumer_stop.set()
        arr.append(patente)
    
    
    thread.join()


async def stock():
    while len(arr) != 5:
        arr.append(await consume())
    print(arr)
    arr.clear()

if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )
    main()